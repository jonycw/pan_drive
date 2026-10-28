//! Browser page: paste a share link, save it into the signed-in drive, then download.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dav_server::body::Body;
use http_body_util::{BodyExt, Limited};
use hyper::{Method, Request, Response};
use serde::Serialize;
#[cfg(test)]
use serde::Deserialize;

use crate::drive::QuarkDrive;
use crate::fetch::{self, FetchArgs, FetchReport};
use crate::portal::{self, Portal, Role};

const REQUESTED_WITH: &str = "quark-share";
const MAX_BODY: usize = 64 * 1024;
const MAX_LOG_LINES: usize = 120;
const READY_PREFIX: &str = "\u{0001}READY:";

pub struct ShareHub {
    drive: QuarkDrive,
    portal: Arc<Portal>,
    tasks: Arc<Mutex<Vec<DownloadTask>>>,
    queue: Arc<Mutex<VecDeque<u64>>>,
    next_id: AtomicU64,
    worker_started: std::sync::atomic::AtomicBool,
}

#[derive(Clone)]
struct ReadyFile {
    rel: String,
    fid: String,
    size: u64,
}

struct DownloadTask {
    id: u64,
    owner: String,
    raw: String,
    share_id: String,
    only_fids: Vec<String>,
    phase: String,
    progress: String,
    file_name: String,
    percent: u64,
    speed: String,
    written: u64,
    total: u64,
    error: Option<String>,
    lines: VecDeque<String>,
    ready_files: Vec<ReadyFile>,
    cancel: Arc<AtomicBool>,
    counted: bool,
    last_saved: Instant,
}


#[cfg(test)]
#[derive(Debug, Deserialize)]
struct StartBody {
    url: String,
    #[serde(default)]
    pwd: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    download: String,
    #[serde(default)]
    cloud_only: bool,
    #[serde(default)]
    force_save: bool,
    #[serde(default)]
    jobs: usize,
}

enum RouteKind {
    UserPage,
    AdminPage,
    Api,
    Dirs,
}

impl ShareHub {
    pub fn new(drive: QuarkDrive) -> Arc<Self> {
        Self::from_portal(drive, Arc::new(Portal::open(PathBuf::from("portal.json"))))
    }

    pub fn from_portal(drive: QuarkDrive, portal: Arc<Portal>) -> Arc<Self> {
        let loaded = portal.load_downloads();
        let next_id = loaded.iter().map(|record| record.id).max().unwrap_or(0).saturating_add(1);
        let tasks = loaded
            .into_iter()
            .map(|record| task_from_record(&portal, record))
            .collect();
        Arc::new(Self {
            drive,
            portal,
            tasks: Arc::new(Mutex::new(tasks)),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            next_id: AtomicU64::new(next_id),
            worker_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub async fn handle(
        &self,
        req: Request<hyper::body::Incoming>,
        peer: SocketAddr,
        strip_prefix: Option<&str>,
    ) -> Response<Body> {
        self.ensure_worker();
        let path = strip_mount(req.uri().path(), strip_prefix);
        if path == "/" {
            return match *req.method() {
                Method::GET | Method::HEAD => html_response(200, &page_html()),
                _ => text_response(405, "请用浏览器打开"),
            };
        }
        let kind = match route_kind(path, None) {
            Some(kind) => kind,
            None => return text_response(404, "没有这个页面"),
        };
        match kind {
            RouteKind::UserPage => match *req.method() {
                Method::GET | Method::HEAD => html_response(200, &page_html()),
                _ => text_response(405, "请用浏览器打开"),
            },
            RouteKind::AdminPage => match *req.method() {
                Method::GET | Method::HEAD => html_response(200, &admin_page_html()),
                _ => text_response(405, "请用浏览器打开"),
            },
            RouteKind::Dirs => {
                if self.user_name(&req).is_none() && !self.is_admin(&req) {
                    return json_error(401, "请先登录");
                }
                match *req.method() {
                    Method::GET => dirs_response(req.uri().query()),
                    _ => json_error(405, "不支持的方法"),
                }
            }
            RouteKind::Api => self.api(req, peer, strip_prefix).await,
        }
    }

    pub fn is_admin(&self, req: &Request<hyper::body::Incoming>) -> bool {
        cookie_from(req, "qd_admin")
            .and_then(|token| self.portal.role(&token))
            .is_some_and(|role| matches!(role, Role::Admin))
    }

    fn user_name(&self, req: &Request<hyper::body::Incoming>) -> Option<String> {
        let token = cookie_from(req, "qd_user")?;
        match self.portal.role(&token)? {
            Role::User(name) => Some(name),
            Role::Admin => None,
        }
    }

    fn ensure_worker(&self) {
        if self
            .worker_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            self.worker_started
                .store(false, std::sync::atomic::Ordering::Release);
            return;
        }
        spawn_worker(
            self.drive.clone(),
            self.portal.clone(),
            self.tasks.clone(),
            self.queue.clone(),
        );
    }

    async fn api(
        &self,
        req: Request<hyper::body::Incoming>,
        peer: SocketAddr,
        strip_prefix: Option<&str>,
    ) -> Response<Body> {
        let path = strip_mount(req.uri().path(), strip_prefix).to_string();
        let method = req.method().clone();
        if method == Method::GET && path == "/api/session" {
            return self.session_response(&req);
        }
        if method == Method::GET && (path == "/api/tasks" || path == "/api/share") {
            return self.tasks_response(&req);
        }
        if method == Method::GET && path == "/api/files" {
            return self.user_files(&req, req.uri().query()).await;
        }
        if method == Method::GET && path == "/api/browser-files" {
            return self.browser_files_list(self.user_name(&req).as_deref());
        }
        if method == Method::GET && path == "/api/browser-files/raw" {
            return self
                .browser_file_raw(self.user_name(&req).as_deref(), req.uri().query())
                .await;
        }
        if method == Method::GET && path == "/api/admin/state" {
            return self.admin_state(&req);
        }
        if method == Method::GET && path == "/api/admin/logs" {
            return self.admin_logs(&req);
        }
        if method == Method::GET && path == "/api/admin/drive" {
            return self.admin_drive(&req).await;
        }
        if method != Method::POST {
            return json_error(405, "不支持的方法");
        }
        if !requested_with(&req) {
            return json_error(400, "请求来源不对");
        }
        let user_token = cookie_from(&req, "qd_user");
        let admin_token = cookie_from(&req, "qd_admin");
        let user_name = user_token
            .as_deref()
            .and_then(|token| self.portal.role(token))
            .and_then(|role| match role {
                Role::User(name) => Some(name),
                Role::Admin => None,
            });
        let is_admin = admin_token
            .as_deref()
            .and_then(|token| self.portal.role(token))
            .is_some_and(|role| matches!(role, Role::Admin));
        let login_base = request_origin(&req);
        let body = match read_json(req).await {
            Ok(body) => body,
            Err(response) => return response,
        };
        let response = match path.as_str() {
            "/api/login" => self.user_login(&body),
            "/api/logout" => self.logout_token(user_token.as_deref(), "qd_user"),
            "/api/download-dir" => self.user_set_download_dir(user_name.as_deref(), &body),
            "/api/preview" => self.preview_shares(user_name.as_deref(), &body).await,
            "/api/tasks/cancel" => self.cancel_for(user_name.as_deref(), &body),
            "/api/tasks" | "/api/share" => {
                let Some(owner) = user_name.as_deref() else {
                    return json_error(401, "请先登录");
                };
                if let Err(message) = self.prepare_user_folder(owner).await {
                    return json_error(400, &message);
                }
                self.enqueue_for(Some(owner), &body)
            }
            "/api/admin/setup" => {
                if !peer.ip().is_loopback() {
                    return json_error(403, "首次设置管理密码请在本机操作");
                }
                self.admin_setup(&body)
            }
            "/api/admin/login" => self.admin_login(&body),
            "/api/admin/logout" => self.logout_token(admin_token.as_deref(), "qd_admin"),
            "/api/admin/password" => self.admin_change_password(is_admin, admin_token.as_deref(), &body),
            "/api/admin/settings" => self.admin_settings(is_admin, &body).await,
            "/api/admin/users" => self.admin_add_user(is_admin, &body),
            "/api/admin/users/delete" => self.admin_delete_user(is_admin, &body),
            "/api/admin/users/expiry" => self.admin_update_expiry(is_admin, &body),
            "/api/admin/users/bind" => self.admin_bind_user(is_admin, &body),
            "/api/admin/users/delivery" => self.admin_delivery_link(is_admin, &body, &login_base),
            "/api/admin/cookies" => self.admin_add_cookie(is_admin, &body),
            "/api/admin/cookies/update" => self.admin_update_cookie(is_admin, &body),
            "/api/admin/cookies/delete" => self.admin_delete_cookie(is_admin, &body),
            "/api/admin/cookies/use" => self.admin_use_cookie(is_admin, &body),
            "/api/admin/drive/delete" => self.admin_drive_delete(is_admin, &body).await,
            _ => json_error(404, "没有这个接口"),
        };
        // Don't refresh admin cookie on user login/logout — insert() would overwrite Set-Cookie: qd_user.
        if is_admin && path != "/api/admin/logout" && path != "/api/login" && path != "/api/logout" {
            self.stamp_admin(admin_token.as_deref(), response)
        } else {
            response
        }
    }

    fn session_response(&self, req: &Request<hyper::body::Incoming>) -> Response<Body> {
        let Some(name) = self.user_name(req) else {
            return json_error(401, "请先登录");
        };
        let expires_on = self.portal.user_expiry_label(&name).unwrap_or_default();
        let download_dir = self.portal.user_download_dir(&name);
        let browser = download_dir == portal::Portal::BROWSER_DIR_MARK;
        let mut response = json_response(
            200,
            serde_json::json!({
                "ok": true,
                "role": "user",
                "name": name,
                "expires_on": expires_on,
                "download_dir": download_dir,
                "browser": browser,
            }),
        );
        if let Some(expires_at) = self.portal.user_expires_at(&name) {
            refresh_user_cookie(&mut response, req, expires_at);
        }
        response
    }

    fn tasks_response(&self, req: &Request<hyper::body::Incoming>) -> Response<Body> {
        let Some(owner) = self.user_name(req) else {
            return json_error(401, "请先登录");
        };
        let mut response = json_response(
            200,
            serde_json::json!({"ok": true, "tasks": self.task_views(&owner)}),
        );
        if let Some(expires_at) = self.portal.user_expires_at(&owner) {
            refresh_user_cookie(&mut response, req, expires_at);
        }
        response
    }

    fn user_set_download_dir(&self, owner: Option<&str>, body: &serde_json::Value) -> Response<Body> {
        let Some(owner) = owner else {
            return json_error(401, "请先登录");
        };
        let path = body.get("path").and_then(|value| value.as_str()).unwrap_or("");
        let label = body
            .get("label")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        match self.portal.set_user_download_dir(owner, path) {
            Ok(download_dir) => {
                let browser = download_dir == portal::Portal::BROWSER_DIR_MARK;
                json_response(
                    200,
                    serde_json::json!({
                        "ok": true,
                        "download_dir": download_dir,
                        "browser": browser,
                        "label": if browser {
                            if label.is_empty() { "本机文件夹" } else { label }
                        } else {
                            download_dir.as_str()
                        },
                    }),
                )
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn browser_files_list(&self, owner: Option<&str>) -> Response<Body> {
        let Some(owner) = owner else {
            return json_error(401, "请先登录");
        };
        if !self.portal.is_browser_download(owner) {
            return json_response(200, serde_json::json!({"ok": true, "files": []}));
        }
        let mut files = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let tasks = lock_tasks(&self.tasks);
        for task in tasks.iter().filter(|task| task.owner == owner && task.phase == "完成") {
            for file in &task.ready_files {
                if !seen.insert(file.fid.clone()) {
                    continue;
                }
                files.push(serde_json::json!({
                    "rel": file.rel,
                    "fid": file.fid,
                    "size": file.size,
                }));
            }
        }
        files.sort_by(|left, right| {
            let left_rel = left.get("rel").and_then(|v| v.as_str()).unwrap_or("");
            let right_rel = right.get("rel").and_then(|v| v.as_str()).unwrap_or("");
            left_rel.cmp(right_rel)
        });
        json_response(200, serde_json::json!({"ok": true, "files": files}))
    }

    async fn browser_file_raw(&self, owner: Option<&str>, query: Option<&str>) -> Response<Body> {
        let Some(owner) = owner else {
            return json_error(401, "请先登录");
        };
        if !self.portal.is_browser_download(owner) {
            return json_error(400, "当前不是浏览器本机保存模式");
        }
        let fid = query_value(query, "fid").unwrap_or_default();
        if fid.is_empty() {
            return json_error(400, "缺少文件");
        }
        let ready = {
            let tasks = lock_tasks(&self.tasks);
            tasks
                .iter()
                .filter(|task| task.owner == owner)
                .flat_map(|task| task.ready_files.iter())
                .find(|file| file.fid == fid)
                .cloned()
        };
        let Some(ready) = ready else {
            return json_error(404, "文件不存在或已过期");
        };
        let choice = match self.portal.cookie_for_user(owner) {
            Ok(choice) => choice,
            Err(message) => return json_error(400, &message),
        };
        if let Err(message) = apply_cookie_choice(&self.drive, &choice) {
            return json_error(400, &message);
        }
        let response = match self.drive.stream_download(&ready.fid).await {
            Ok(response) => response,
            Err(err) => return json_error(502, &brief_error(&format!("{err:#}"))),
        };
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(err) => return json_error(502, &format!("读取网盘文件失败: {err}")),
        };
        let name = ready
            .rel
            .rsplit('/')
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("download");
        let disposition = format!("attachment; filename*=UTF-8''{}", encode_path_seg(name));
        Response::builder()
            .status(200)
            .header(hyper::header::CONTENT_TYPE, "application/octet-stream")
            .header(hyper::header::CONTENT_DISPOSITION, disposition)
            .header(hyper::header::CONTENT_LENGTH, bytes.len())
            .header(hyper::header::CACHE_CONTROL, "no-store")
            .body(Body::from(bytes))
            .unwrap_or_else(|_| text_response(500, "响应失败"))
    }

    fn enqueue_for(&self, owner: Option<&str>, body: &serde_json::Value) -> Response<Body> {
        let Some(owner) = owner else {
            return json_error(401, "请先登录");
        };
        let text = body
            .get("text")
            .and_then(|value| value.as_str())
            .or_else(|| body.get("url").and_then(|value| value.as_str()))
            .unwrap_or("")
            .to_string();
        if let Err(message) = self.portal.ensure_can_download(owner) {
            return json_error(400, &message);
        }
        let mode = if body.get("replace").and_then(|value| value.as_bool()) == Some(true) {
            "replace"
        } else if body.get("wait").and_then(|value| value.as_bool()) == Some(true) {
            "wait"
        } else {
            "ask"
        };
        match self.enqueue(owner, &text, body.get("picks"), mode) {
            Ok(tasks) => json_response(200, serde_json::json!({"ok": true, "tasks": tasks})),
            Err(message) if message == "这个文件已经在下载" || message == "这个账号还有别的下载没完成" => {
                json_response(
                    409,
                    serde_json::json!({"ok": false, "busy": true, "error": message}),
                )
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn logout_token(&self, token: Option<&str>, cookie: &str) -> Response<Body> {
        if let Some(token) = token {
            let actor = if cookie == "qd_admin" {
                "管理端".to_string()
            } else {
                self.portal.user_from_token(token).unwrap_or_else(|| "用户".to_string())
            };
            self.portal.logout(token);
            self.portal.log_op(&actor, "退出", "", true);
        }
        let mut response = json_response(200, serde_json::json!({"ok": true}));
        clear_cookie(&mut response, cookie);
        response
    }

    fn user_login(&self, body: &serde_json::Value) -> Response<Body> {
        let name = body.get("name").and_then(|value| value.as_str()).unwrap_or("");
        let password = body.get("password").and_then(|value| value.as_str()).unwrap_or("");
        match self.portal.login_user(name, password) {
            Ok((token, expires_on)) => {
                self.portal.log_op(name.trim(), "登录", "登录成功", true);
                let download_dir = self.portal.user_download_dir(name.trim());
                let browser = download_dir == portal::Portal::BROWSER_DIR_MARK;
                let mut response = json_response(
                    200,
                    serde_json::json!({
                        "ok": true,
                        "name": name.trim(),
                        "expires_on": expires_on,
                        "download_dir": download_dir,
                        "browser": browser,
                    }),
                );
                if let Some(expires_at) = self.portal.user_expires_at(name.trim()) {
                    set_cookie(&mut response, "qd_user", &token, max_age_until(expires_at));
                }
                response
            }
            Err(message) => {
                self.portal.log_op(name.trim(), "登录", &message, false);
                json_error(401, &message)
            }
        }
    }

    fn admin_setup(&self, body: &serde_json::Value) -> Response<Body> {
        let password = body
            .get("password")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        match self.portal.setup_admin(password) {
            Ok(()) => {
                self.portal.log_op("管理端", "设置密码", "已设置管理密码", true);
                json_response(200, serde_json::json!({"ok": true}))
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_login(&self, body: &serde_json::Value) -> Response<Body> {
        let password = body.get("password").and_then(|value| value.as_str()).unwrap_or("");
        match self.portal.login_admin(password) {
            Ok(token) => {
                let mut response = json_response(200, serde_json::json!({"ok": true}));
                let max_age = self
                    .portal
                    .touch_admin(&token)
                    .map(max_age_until)
                    .unwrap_or(60 * 60);
                set_cookie(&mut response, "qd_admin", &token, max_age);
                self.portal.log_op("管理端", "登录", "登录成功", true);
                response
            }
            Err(message) => {
                self.portal.log_op("管理端", "登录", &message, false);
                json_error(401, &message)
            }
        }
    }

    fn admin_change_password(
        &self,
        is_admin: bool,
        token: Option<&str>,
        body: &serde_json::Value,
    ) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let old_password = body
            .get("old_password")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let new_password = body
            .get("new_password")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        match self
            .portal
            .change_admin_password(old_password, new_password, token)
        {
            Ok(()) => {
                self.portal.log_op("管理端", "修改密码", "管理密码已更新", true);
                json_response(200, serde_json::json!({"ok": true}))
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_state(&self, req: &Request<hyper::body::Incoming>) -> Response<Body> {
        if !self.portal.admin_ready() {
            return json_response(200, serde_json::json!({"ok": true, "ready": false}));
        }
        if !self.is_admin(req) {
            return json_error(401, "请先登录管理端");
        }
        let settings = self.portal.settings();
        let response = json_response(
            200,
            serde_json::json!({
                "ok": true,
                "ready": true,
                "settings": settings,
                "users": self.portal.users(),
                "cookies": self.portal.cookies(),
            }),
        );
        self.stamp_admin(cookie_from(req, "qd_admin").as_deref(), response)
    }

    fn admin_logs(&self, req: &Request<hyper::body::Incoming>) -> Response<Body> {
        if !self.is_admin(req) {
            return json_error(401, "请先登录管理端");
        }
        let logs: Vec<_> = self
            .portal
            .recent_ops(100)
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "at": portal::format_expiry(row.at_ms),
                    "actor": row.actor,
                    "action": row.action,
                    "detail": row.detail,
                    "ok": row.ok,
                })
            })
            .collect();
        let response = json_response(200, serde_json::json!({"ok": true, "logs": logs}));
        self.stamp_admin(cookie_from(req, "qd_admin").as_deref(), response)
    }

    async fn admin_drive(&self, req: &Request<hyper::body::Incoming>) -> Response<Body> {
        if !self.is_admin(req) {
            return json_error(401, "请先登录管理端");
        }
        let path = query_value(req.uri().query(), "path").unwrap_or_else(|| "/".to_string());
        let response = match self.drive_listing(&path).await {
            Ok(value) => json_response(200, value),
            Err(message) => json_error(400, &message),
        };
        self.stamp_admin(cookie_from(req, "qd_admin").as_deref(), response)
    }

    async fn admin_drive_delete(
        &self,
        is_admin: bool,
        body: &serde_json::Value,
    ) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let path = body.get("path").and_then(|value| value.as_str()).unwrap_or("/");
        let fid = body.get("fid").and_then(|value| value.as_str()).unwrap_or("").trim();
        if fid.is_empty() || fid == "0" || fid.len() > 128 || fid.contains('/') || fid.contains('\\')
        {
            return json_error(400, "不能删除这个位置");
        }
        let choice = match self.use_admin_cookie() {
            Ok(choice) => choice,
            Err(message) => return json_error(400, &message),
        };
        let (parent_fid, built) = match self.open_cloud_dir(&path).await {
            Ok(found) => found,
            Err(err) => return json_error(400, &self.note_drive_failure(&choice, err)),
        };
        let files = match self.drive.list_dir_all(&parent_fid).await {
            Ok(files) => files,
            Err(err) => {
                return json_error(400, &self.note_drive_failure(&choice, format!("{err:#}")))
            }
        };
        let Some(file) = files.into_iter().find(|file| file.fid == fid) else {
            return json_error(400, "这个文件已经不在当前目录");
        };
        let target = if built == "/" {
            format!("/{}", file.file_name)
        } else {
            format!("{built}/{}", file.file_name)
        };
        let kind = if file.dir { "目录" } else { "文件" };
        if let Err(err) = self.drive.remove_file(&file.fid, false).await {
            let message = self.note_drive_failure(&choice, format!("{err:#}"));
            self.portal
                .log_op("管理端", "删除网盘内容", &format!("{kind} {target}"), false);
            return json_error(400, &message);
        }
        self.portal
            .log_op("管理端", "删除网盘内容", &format!("{kind} {target}"), true);
        match self.drive_listing(&built).await {
            Ok(value) => json_response(200, value),
            Err(message) => json_error(400, &message),
        }
    }

    fn use_admin_cookie(&self) -> Result<portal::CookieChoice, String> {
        let choice = self.portal.active_cookie()?;
        apply_cookie_choice(&self.drive, &choice)?;
        Ok(choice)
    }

    async fn open_cloud_dir(&self, path: &str) -> Result<(String, String), String> {
        let path = path.trim();
        if path.chars().count() > 1000 {
            return Err("目录路径太长".to_string());
        }
        match self.drive.lookup_dir_path(path).await {
            Ok(Some(found)) => Ok(found),
            Ok(None) => Err("这个目录不存在".to_string()),
            Err(err) => Err(format!("{err:#}")),
        }
    }

    async fn drive_listing(&self, path: &str) -> Result<serde_json::Value, String> {
        let choice = self.use_admin_cookie()?;
        let (fid, built) = match self.open_cloud_dir(path).await {
            Ok(found) => found,
            Err(err) => return Err(self.note_drive_failure(&choice, err)),
        };
        let mut files = match self.drive.list_dir_all(&fid).await {
            Ok(files) => files,
            Err(err) => return Err(self.note_drive_failure(&choice, format!("{err:#}"))),
        };
        files.sort_by(|left, right| {
            right.dir.cmp(&left.dir).then_with(|| {
                left.file_name
                    .to_lowercase()
                    .cmp(&right.file_name.to_lowercase())
            })
        });
        let entries: Vec<_> = files
            .into_iter()
            .map(|file| {
                serde_json::json!({
                    "fid": file.fid,
                    "name": file.file_name,
                    "dir": file.dir,
                    "size": file.size,
                    "updated_on": if file.updated_at == 0 {
                        String::new()
                    } else {
                        portal::format_expiry(file.updated_at as i64)
                    },
                })
            })
            .collect();
        Ok(serde_json::json!({
            "ok": true,
            "path": built,
            "entries": entries,
        }))
    }

    fn note_drive_failure(&self, choice: &portal::CookieChoice, err: String) -> String {
        let brief = brief_error(&err);
        if let portal::CookieChoice::Use { id, .. } = choice {
            if cookie_auth_failed(&err) {
                self.portal.note_cookie(id, "invalid", &brief);
                return "当前 Cookie 已失效".to_string();
            }
        }
        brief
    }

    fn stamp_admin(&self, token: Option<&str>, mut response: Response<Body>) -> Response<Body> {
        let Some(token) = token else {
            return response;
        };
        if let Some(expires_at) = self.portal.touch_admin(token) {
            set_cookie(&mut response, "qd_admin", token, max_age_until(expires_at));
        }
        response
    }

    async fn admin_settings(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let cloud = body.get("cloud_dir").and_then(|value| value.as_str()).unwrap_or("");
        let cloud = if cloud.trim().is_empty() {
            "/来自分享".to_string()
        } else {
            match normalize_cloud_path(cloud) {
                Ok(path) => path,
                Err(message) => return json_error(400, &message),
            }
        };
        let download = body
            .get("download_dir")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let jobs = body.get("jobs").and_then(|value| value.as_u64()).unwrap_or(3) as usize;
        let download = if body.get("download_dir").is_some() {
            download.to_string()
        } else {
            self.portal.settings().download_dir
        };
        let _ = self.activate_selected_cookie();
        let created = match self.drive.ensure_dir_path(&cloud).await {
            Ok((_, path)) => path,
            Err(err) => return json_error(400, &format!("网盘目录没有创建成功：{}", brief_error(&format!("{err:#}")))),
        };
        match self.portal.update_settings(&created, &download, jobs) {
            Ok(settings) => {
                self.portal.log_op("管理端", "保存下载设置", &format!("网盘目录 {created}"), true);
                json_response(
                    200,
                    serde_json::json!({"ok": true, "settings": settings, "created": created}),
                )
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_update_expiry(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let name = body.get("name").and_then(|value| value.as_str()).unwrap_or("");
        let expires_on = body.get("expires_on").and_then(|value| value.as_str()).unwrap_or("");
        match self.portal.update_user_expiry(name, expires_on) {
            Ok(()) => {
                self.portal.log_op("管理端", "修改有效期", &format!("{name} {expires_on}"), true);
                json_response(200, serde_json::json!({"ok": true, "users": self.portal.users()}))
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_add_user(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let given = body.get("name").and_then(|value| value.as_str()).unwrap_or("").trim();
        let name = if given.is_empty() {
            self.portal.fresh_user_name()
        } else {
            given.to_string()
        };
        let password = body.get("password").and_then(|value| value.as_str()).unwrap_or("");
        let password = if password.is_empty() { "123456" } else { password };
        let expires_on = body.get("expires_on").and_then(|value| value.as_str()).unwrap_or("");
        let cookie_id = body.get("cookie_id").and_then(|value| value.as_str()).unwrap_or("");
        let limit = body.get("download_limit").and_then(|value| value.as_u64()).unwrap_or(2) as u32;
        match self.portal.add_user(&name, password, expires_on, cookie_id, limit) {
            Ok(()) => {
                self.portal.log_op("管理端", "添加账号", &name, true);
                json_response(
                    200,
                    serde_json::json!({
                        "ok": true,
                        "name": name,
                        "users": self.portal.users(),
                        "cookies": self.portal.cookies()
                    }),
                )
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_bind_user(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let name = body.get("name").and_then(|value| value.as_str()).unwrap_or("");
        let cookie_id = body.get("cookie_id").and_then(|value| value.as_str()).unwrap_or("");
        let limit = body.get("download_limit").and_then(|value| value.as_u64()).unwrap_or(2) as u32;
        match self.portal.update_user_binding(name, cookie_id, limit) {
            Ok(()) => {
                self.portal.log_op("管理端", "修改绑定", &format!("{name} 同时下载 {limit}"), true);
                json_response(
                    200,
                    serde_json::json!({"ok": true, "users": self.portal.users(), "cookies": self.portal.cookies()}),
                )
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_delivery_link(
        &self,
        is_admin: bool,
        body: &serde_json::Value,
        login_base: &str,
    ) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let name = body
            .get("name")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        if name.is_empty() {
            return json_error(400, "请选择账号");
        }
        let password = body
            .get("password")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim();
        let password = if password.is_empty() {
            "123456"
        } else {
            password
        };
        let expires_on = match self.portal.user_expires_on(name) {
            Ok(value) => value,
            Err(message) => return json_error(400, &message),
        };
        if let Err(message) = self.portal.reset_user_password(name, password) {
            return json_error(400, &message);
        }
        let login_url = body
            .get("login_url")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                if login_base.is_empty() {
                    "/share".to_string()
                } else {
                    format!("{}/share", login_base.trim_end_matches('/'))
                }
            });
        let delivery_text = format!(
            "登录地址：{login_url}\n账号：{name}\n密码：{password}\n有效期：{expires_on}"
        );
        self.portal
            .log_op("管理端", "生成交付图片", name, true);
        json_response(
            200,
            serde_json::json!({
                "ok": true,
                "delivery_text": delivery_text,
                "login_url": login_url,
                "name": name,
                "password": password,
                "expires_on": expires_on,
            }),
        )
    }

    fn admin_delete_user(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let name = body.get("name").and_then(|value| value.as_str()).unwrap_or("");
        match self.portal.delete_user(name) {
            Ok(()) => {
                self.portal.log_op("管理端", "删除账号", name.trim(), true);
                json_response(200, serde_json::json!({"ok": true, "users": self.portal.users()}))
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_add_cookie(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let label = body.get("label").and_then(|value| value.as_str()).unwrap_or("");
        let raw = body.get("value").and_then(|value| value.as_str()).unwrap_or("");
        match self.portal.add_cookie(label, raw) {
            Ok(()) => {
                self.portal.log_op("管理端", "添加 Cookie", label.trim(), true);
                let _ = self.activate_selected_cookie();
                self.cookie_response()
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_update_cookie(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let id = body.get("id").and_then(|value| value.as_str()).unwrap_or("");
        let label = body.get("label").and_then(|value| value.as_str()).unwrap_or("");
        let raw = body.get("value").and_then(|value| value.as_str());
        match self.portal.update_cookie(id, label, raw) {
            Ok(()) => {
                self.portal.log_op("管理端", "修改 Cookie", label.trim(), true);
                let _ = self.activate_selected_cookie();
                self.cookie_response()
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_delete_cookie(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let id = body.get("id").and_then(|value| value.as_str()).unwrap_or("");
        match self.portal.delete_cookie(id) {
            Ok(()) => {
                self.portal.log_op("管理端", "删除 Cookie", id, true);
                let _ = self.activate_selected_cookie();
                self.cookie_response()
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn admin_use_cookie(&self, is_admin: bool, body: &serde_json::Value) -> Response<Body> {
        if !is_admin {
            return json_error(401, "请先登录管理端");
        }
        let id = body.get("id").and_then(|value| value.as_str()).unwrap_or("");
        match self.portal.use_cookie(id) {
            Ok(raw) => {
                self.portal.log_op("管理端", "切换当前 Cookie", id, true);
                crate::cookie::set_file_watch_paused(true);
                if let Err(message) = crate::cookie::install_cookie_text(&self.drive.cookie_store(), &raw)
                {
                    return json_error(400, &message);
                }
                self.cookie_response()
            }
            Err(message) => json_error(400, &message),
        }
    }

    fn cookie_response(&self) -> Response<Body> {
        json_response(
            200,
            serde_json::json!({"ok": true, "cookies": self.portal.cookies()}),
        )
    }

    pub fn import_file_cookie_if_empty(&self) {
        if self.portal.has_cookies() {
            let _ = self.activate_selected_cookie();
            return;
        }
        let Some(raw) = crate::cookie::saved_file_text() else {
            return;
        };
        if self.portal.add_cookie("quark.cookie", &raw).is_ok() {
            let _ = self.activate_selected_cookie();
        }
    }

    async fn prepare_user_folder(&self, owner: &str) -> Result<(), String> {
        if self.portal.user_download_dir(owner).trim().is_empty() {
            return Err("请先选择本机保存目录".to_string());
        }
        self.use_owner_cookie(owner)?;
        let path = user_cloud_path(&self.portal.settings(), owner)?;
        match self.drive.ensure_dir_path(&path).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let text = format!("{err:#}");
                if cookie_auth_failed(&text) {
                    if let Ok(portal::CookieChoice::Use { id, .. }) = self.portal.cookie_for_user(owner) {
                        self.portal.note_cookie(&id, "invalid", &brief_error(&text));
                    }
                    Err("绑定的 Cookie 已失效".to_string())
                } else {
                    Err(brief_error(&text))
                }
            }
        }
    }

    async fn user_files(&self, req: &Request<hyper::body::Incoming>, query: Option<&str>) -> Response<Body> {
        let Some(owner) = self.user_name(req) else {
            return json_error(401, "请先登录");
        };
        if let Err(message) = self.use_owner_cookie(&owner) {
            return json_error(400, &message);
        }
        let root = match user_cloud_path(&self.portal.settings(), &owner) {
            Ok(path) => path,
            Err(message) => return json_error(400, &message),
        };
        let requested = query_value(query, "path").unwrap_or_default();
        let path = match path_under(&root, &requested) {
            Ok(path) => path,
            Err(message) => return json_error(400, &message),
        };
        let listed = match self.drive.lookup_dir_path(&path).await {
            Ok(Some((fid, built))) => match self.drive.list_dir_all(&fid).await {
                Ok(mut files) => {
                    files.sort_by(|left, right| {
                        right
                            .dir
                            .cmp(&left.dir)
                            .then_with(|| left.file_name.cmp(&right.file_name))
                    });
                    let entries: Vec<serde_json::Value> = files
                        .into_iter()
                        .map(|file| {
                            serde_json::json!({
                                "name": file.file_name,
                                "dir": file.dir,
                                "size": file.size,
                            })
                        })
                        .collect();
                    (built, entries)
                }
                Err(err) => return json_error(400, &brief_error(&format!("{err:#}"))),
            },
            Ok(None) => (path, Vec::new()),
            Err(err) => return json_error(400, &brief_error(&format!("{err:#}"))),
        };
        json_response(
            200,
            serde_json::json!({
                "ok": true,
                "root": root,
                "path": listed.0,
                "entries": listed.1,
            }),
        )
    }

    fn use_owner_cookie(&self, owner: &str) -> Result<(), String> {
        let choice = self.portal.cookie_for_user(owner)?;
        apply_cookie_choice(&self.drive, &choice)
    }

    fn activate_selected_cookie(&self) -> Result<(), String> {
        match self.portal.select_cookie(&[]) {
            Ok(choice) => apply_cookie_choice(&self.drive, &choice),
            Err(message) => Err(message),
        }
    }

    fn cancel_for(&self, owner: Option<&str>, body: &serde_json::Value) -> Response<Body> {
        let Some(owner) = owner else {
            return json_error(401, "请先登录");
        };
        let id = body.get("id").and_then(|value| value.as_u64()).unwrap_or(0);
        match self.cancel_task(owner, id) {
            Ok(tasks) => json_response(200, serde_json::json!({"ok": true, "tasks": tasks})),
            Err(message) => json_error(400, &message),
        }
    }

    async fn preview_shares(&self, owner: Option<&str>, body: &serde_json::Value) -> Response<Body> {
        let Some(owner) = owner else {
            return json_error(401, "请先登录");
        };
        let text = body
            .get("text")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if text.is_empty() {
            return json_error(400, "请粘贴分享链接");
        }
        if let Err(message) = self.use_owner_cookie(owner) {
            return json_error(400, &message);
        }
        let (pwd_id, passcode) = match fetch::parse_share_input(&text, "") {
            Ok(parsed) => parsed,
            Err(err) => return json_error(400, &err.to_string()),
        };
        let cloud = user_cloud_path(&self.portal.settings(), owner).unwrap_or_else(|_| "/来自分享".to_string());
        let group = match self.drive.open_share(&pwd_id, &passcode).await {
            Ok(snapshot) => {
                let entries: Vec<serde_json::Value> = snapshot
                    .entries
                    .iter()
                    .map(|entry| {
                        serde_json::json!({
                            "fid": entry.fid,
                            "name": entry.file_name,
                            "dir": entry.is_dir,
                            "size": entry.size,
                        })
                    })
                    .collect();
                serde_json::json!({
                    "raw": text,
                    "title": snapshot.title,
                    "entries": entries,
                })
            }
            Err(err) => serde_json::json!({
                "raw": text,
                "title": "",
                "error": brief_error(&format!("{err:#}")),
                "entries": [],
            }),
        };
        json_response(
            200,
            serde_json::json!({"ok": true, "cloud": cloud, "groups": [group]}),
        )
    }

    fn enqueue(
        &self,
        owner: &str,
        text: &str,
        picks: Option<&serde_json::Value>,
        mode: &str,
    ) -> Result<serde_json::Value, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("请粘贴分享链接".to_string());
        }
        if let Err(err) = fetch::parse_share_input(text, "") {
            return Err(err.to_string());
        }
        let pending = lock_tasks(&self.tasks)
            .iter()
            .filter(|task| {
                task.owner == owner && (task.phase == "排队" || task.phase == "下载中")
            })
            .count();
        if pending >= 100 {
            return Err("排队的链接太多了，请等前面的完成一些再提交".to_string());
        }
        let selected = picks.map(|value| selected_fids(value, text));
        let fids = selected.clone().unwrap_or_default();
        let share_id = fetch::parse_share_input(text, "").map(|(id, _)| id).unwrap_or_default();
        let same_file = self.share_busy(owner, &share_id, &fids);
        let other_busy = self.account_busy(owner);
        if mode == "replace" {
            self.cancel_active(owner);
        } else if same_file && mode != "queue" {
            return Err("这个文件已经在下载".to_string());
        } else if other_busy && mode == "ask" {
            return Err("这个账号还有别的下载没完成".to_string());
        }
        self.ensure_worker();
        let waiting = if mode == "replace" && other_busy {
            "正在停掉前面的下载"
        } else if other_busy {
            "等这个账号当前的下载结束"
        } else {
            ""
        };
        Ok(serde_json::Value::Array(vec![self.push_task(
            owner,
            text.to_string(),
            selected,
            waiting,
        )?]))
    }

    fn push_task(
        &self,
        owner: &str,
        raw: String,
        selected: Option<Vec<String>>,
        waiting: &str,
    ) -> Result<serde_json::Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let parsed = fetch::parse_share_input(&raw, "");
        let (phase, error, queue_it, share_id, only_fids) = match parsed {
            Err(err) => (
                "失败".to_string(),
                Some(brief_error(&format!("{err:#}"))),
                false,
                String::new(),
                Vec::new(),
            ),
            Ok((share_id, _)) => {
                let only_fids = selected.clone().unwrap_or_default();
                if selected.as_ref().is_some_and(|fids| fids.is_empty()) {
                    (
                        "失败".to_string(),
                        Some("请选择要下载的文件".to_string()),
                        false,
                        share_id,
                        only_fids,
                    )
                } else if self.share_busy(owner, &share_id, selected.as_deref().unwrap_or(&[])) {
                    (
                        "失败".to_string(),
                        Some("这个文件正在下载".to_string()),
                        false,
                        share_id,
                        only_fids,
                    )
                } else {
                    ("排队".to_string(), None, true, share_id, only_fids)
                }
            }
        };
        let counted = false;
        let queued = queue_it;
        let logged_share = share_id.clone();
        let logged_error = error.clone();
        let task = DownloadTask {
            id,
            owner: owner.to_string(),
            raw: raw.clone(),
            share_id,
            only_fids,
            phase: phase.clone(),
            progress: if phase == "排队" && !waiting.is_empty() {
                waiting.to_string()
            } else {
                String::new()
            },
            file_name: String::new(),
            percent: 0,
            speed: String::new(),
            written: 0,
            total: 0,
            error: error.clone(),
            lines: VecDeque::new(),
            ready_files: Vec::new(),
            cancel: Arc::new(AtomicBool::new(false)),
            counted,
            last_saved: Instant::now(),
        };
        let view = task_snapshot(&task);
        {
            let mut tasks = lock_tasks(&self.tasks);
            tasks.push(task);
            let overflow = tasks.iter().filter(|task| task.owner == owner).count().saturating_sub(40);
            if overflow > 0 {
                let mut removed = 0;
                tasks.retain(|task| {
                    if removed < overflow && task.owner == owner && task.phase != "下载中" && task.phase != "排队" {
                        removed += 1;
                        false
                    } else {
                        true
                    }
                });
            }
        }
        if queue_it {
            lock_queue(&self.queue).push_back(id);
        }
        if let Some(task) = lock_tasks(&self.tasks).iter_mut().find(|task| task.id == id) {
            save_task(&self.portal, task, true);
        }
        if queued {
            self.portal.log_op(owner, "排队下载", &format!("分享 {logged_share}"), true);
        } else if let Some(message) = logged_error {
            self.portal.log_op(owner, "下载未排队", &message, false);
        }
        Ok(view)
    }

    fn task_views(&self, owner: &str) -> Vec<serde_json::Value> {
        lock_tasks(&self.tasks)
            .iter()
            .rev()
            .filter(|task| task.owner == owner)
            .map(task_snapshot)
            .collect()
    }

    fn share_busy(&self, owner: &str, share_id: &str, fids: &[String]) -> bool {
        lock_tasks(&self.tasks).iter().any(|task| {
            task.owner == owner
                && task.share_id == share_id
                && (task.phase == "排队" || task.phase == "下载中")
                && !task.cancel.load(Ordering::Relaxed)
                && (task.only_fids.is_empty()
                    || fids.is_empty()
                    || task.only_fids.iter().any(|fid| fids.iter().any(|item| item == fid)))
        })
    }

    fn account_busy(&self, owner: &str) -> bool {
        lock_tasks(&self.tasks).iter().any(|task| {
            task.owner == owner
                && (task.phase == "排队" || task.phase == "下载中")
                && !task.cancel.load(Ordering::Relaxed)
        })
    }

    fn cancel_active(&self, owner: &str) {
        let mut refunds = 0u32;
        {
            let mut tasks = lock_tasks(&self.tasks);
            for task in tasks.iter_mut() {
                if task.owner == owner && (task.phase == "排队" || task.phase == "下载中") {
                    task.cancel.store(true, Ordering::Relaxed);
                    if task.phase == "排队" {
                        if task.counted {
                            task.counted = false;
                            refunds += 1;
                        }
                        task.phase = "已取消".to_string();
                        task.progress = "已取消".to_string();
                        save_task(&self.portal, task, true);
                    }
                }
            }
        }
        for _ in 0..refunds {
            self.portal.refund_download(owner);
        }
    }

    fn cancel_task(&self, owner: &str, id: u64) -> Result<serde_json::Value, String> {
        {
            let mut tasks = lock_tasks(&self.tasks);
            let Some(task) = tasks.iter_mut().find(|task| task.id == id && task.owner == owner) else {
                return Err("没有这个下载".to_string());
            };
            if task.phase != "排队" && task.phase != "下载中" {
                return Err("这个任务已经结束".to_string());
            }
            task.cancel.store(true, Ordering::Relaxed);
            let refund = task.phase == "排队" && task.counted;
            if task.phase == "排队" {
                task.counted = false;
                task.phase = "已取消".to_string();
                task.progress = "已取消".to_string();
                save_task(&self.portal, task, true);
            }
            if refund {
                self.portal.refund_download(owner);
            }
            self.portal.log_op(owner, "取消下载", &task.share_id, true);
        }
        Ok(serde_json::Value::Array(self.task_views(owner)))
    }
}

fn push_line(lines: &mut VecDeque<String>, line: &str) {
    if line.is_empty() {
        return;
    }
    lines.push_back(line.to_string());
    while lines.len() > MAX_LOG_LINES {
        lines.pop_front();
    }
}


pub fn mounted_path<'a>(path: &'a str, strip_prefix: Option<&str>) -> &'a str {
    strip_mount(path, strip_prefix)
}

pub fn is_share_path(path: &str, strip_prefix: Option<&str>) -> bool {
    route_kind(path, strip_prefix).is_some()
}

fn route_kind(path: &str, strip_prefix: Option<&str>) -> Option<RouteKind> {
    let path = strip_mount(path, strip_prefix);
    match path {
        "/share" | "/share/" => Some(RouteKind::UserPage),
        "/admin" | "/admin/" => Some(RouteKind::AdminPage),
        "/api/dirs" => Some(RouteKind::Dirs),
        path if is_api_path(path) => Some(RouteKind::Api),
        _ => None,
    }
}

fn is_api_path(path: &str) -> bool {
    matches!(
        path,
        "/api/login"
            | "/api/logout"
            | "/api/session"
            | "/api/tasks"
            | "/api/tasks/cancel"
            | "/api/share"
            | "/api/files"
            | "/api/preview"
            | "/api/download-dir"
            | "/api/browser-files"
            | "/api/browser-files/raw"
            | "/api/admin/users/expiry"
            | "/api/admin/login"
            | "/api/admin/logout"
            | "/api/admin/password"
            | "/api/admin/setup"
            | "/api/admin/state"
            | "/api/admin/logs"
            | "/api/admin/settings"
            | "/api/admin/users"
            | "/api/admin/users/delete"
            | "/api/admin/users/bind"
            | "/api/admin/users/delivery"
            | "/api/admin/cookies"
            | "/api/admin/cookies/update"
            | "/api/admin/cookies/delete"
            | "/api/admin/cookies/use"
            | "/api/admin/drive"
            | "/api/admin/drive/delete"
    )
}

fn strip_mount<'a>(path: &'a str, strip_prefix: Option<&str>) -> &'a str {
    let Some(prefix) = strip_prefix else {
        return path;
    };
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        return path;
    }
    if path == prefix {
        return "/";
    }
    match path.strip_prefix(prefix) {
        Some(rest) if rest.starts_with('/') => rest,
        _ => path,
    }
}

#[cfg(test)]
fn build_args(body: &StartBody) -> Result<FetchArgs, String> {
    if body.url.trim().is_empty() {
        return Err("请粘贴分享链接".to_string());
    }
    let to = {
        let to = body.to.trim();
        if to.is_empty() {
            "/来自分享".to_string()
        } else {
            to.to_string()
        }
    };
    let jobs = if body.jobs == 0 {
        3
    } else {
        body.jobs.clamp(1, 8)
    };
    let download = if body.cloud_only {
        None
    } else {
        let raw = body.download.trim();
        if raw.is_empty() {
            return Err("请选择本机保存目录".to_string());
        }
        let path = PathBuf::from(raw);
        if !path.is_absolute() {
            return Err("本机目录请填绝对路径，例如 D:\\Downloads\\夸克分享".to_string());
        }
        Some(path)
    };
    Ok(FetchArgs {
        url: body.url.trim().to_string(),
        pwd: body.pwd.trim().to_string(),
        to,
        download,
        jobs,
        cloud_only: body.cloud_only,
        force_save: body.force_save,
        only_fids: Vec::new(),
    })
}

fn encode_path_seg(name: &str) -> String {
    percent_encoding::utf8_percent_encode(name, percent_encoding::NON_ALPHANUMERIC).to_string()
}

fn suggest_download_dir() -> String {
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        return PathBuf::from(profile)
            .join("Downloads")
            .join("夸克分享")
            .display()
            .to_string();
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join("Downloads")
            .join("夸克分享")
            .display()
            .to_string();
    }
    String::new()
}

fn json_response(status: u16, value: serde_json::Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .header("cache-control", "no-store")
        .body(Body::from(value.to_string()))
        .unwrap()
}

fn json_error(status: u16, message: &str) -> Response<Body> {
    json_response(
        status,
        serde_json::json!({
            "ok": false,
            "error": message,
        }),
    )
}

fn html_response(status: u16, html: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/html; charset=utf-8")
        .header("cache-control", "no-store")
        .body(Body::from(html.to_string()))
        .unwrap()
}

fn text_response(status: u16, message: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("cache-control", "no-store")
        .body(Body::from(message.to_string()))
        .unwrap()
}

fn spawn_worker(
    drive: QuarkDrive,
    portal: Arc<Portal>,
    tasks: Arc<Mutex<Vec<DownloadTask>>>,
    queue: Arc<Mutex<VecDeque<u64>>>,
) {
    tokio::spawn(async move {
        loop {
            let next = lock_queue(&queue).pop_front();
            let Some(id) = next else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };
            let (raw, owner, only_fids, cancel) = {
                let mut tasks = lock_tasks(&tasks);
                let Some(task) = tasks.iter_mut().find(|task| task.id == id) else {
                    continue;
                };
                if task.phase != "排队" {
                    continue;
                }
                if task.cancel.load(Ordering::Relaxed) {
                    let refund = task.counted;
                    let name = task.owner.clone();
                    task.counted = false;
                    task.phase = "已取消".to_string();
                    task.progress = "已取消".to_string();
                    save_task(&portal, task, true);
                    drop(tasks);
                    if refund {
                        portal.refund_download(&name);
                    }
                    continue;
                }
                task.phase = "下载中".to_string();
                save_task(&portal, task, true);
                (
                    task.raw.clone(),
                    task.owner.clone(),
                    task.only_fids.clone(),
                    task.cancel.clone(),
                )
            };
            let settings = portal.settings();
            let mut args = match task_fetch_args(&portal, &raw, &settings, &owner) {
                Ok(args) => args,
                Err(message) => {
                    finish_task(&portal, &tasks, id, Err(message));
                    continue;
                }
            };
            args.only_fids = only_fids;
            args.jobs = portal.user_parallel(&owner);
            let report = Arc::new(TaskReport {
                tasks: tasks.clone(),
                id,
                last_progress: Mutex::new(HashMap::new()),
                cancel,
                portal: portal.clone(),
            });
            if report.cancelled() {
                finish_task(&portal, &tasks, id, Err("已取消".to_string()));
                continue;
            }
            let choice = match portal.cookie_for_user(&owner) {
                Ok(choice) => choice,
                Err(message) => {
                    finish_task(&portal, &tasks, id, Err(message));
                    continue;
                }
            };
            if let Err(message) = apply_cookie_choice(&drive, &choice) {
                if let portal::CookieChoice::Use { id: cookie_id, .. } = &choice {
                    portal.note_cookie(cookie_id, "invalid", &message);
                }
                finish_task(&portal, &tasks, id, Err("绑定的 Cookie 已失效".to_string()));
                continue;
            }
            let result = fetch::run_with_report(&drive, &args, report.clone()).await;
            match result {
                Ok(()) => {
                    if let portal::CookieChoice::Use { id: cookie_id, .. } = &choice {
                        portal.note_cookie(cookie_id, "ok", "");
                    }
                    finish_task(&portal, &tasks, id, Ok(()));
                }
                Err(err) => {
                    let text = format!("{err:#}");
                    if let portal::CookieChoice::Use { id: cookie_id, .. } = &choice {
                        if cookie_auth_failed(&text) {
                            portal.note_cookie(cookie_id, "invalid", &brief_error(&text));
                            finish_task(&portal, &tasks, id, Err("绑定的 Cookie 已失效".to_string()));
                            continue;
                        }
                    }
                    finish_task(&portal, &tasks, id, Err(text));
                }
            }
        }
    });
}

fn apply_cookie_choice(drive: &QuarkDrive, choice: &portal::CookieChoice) -> Result<(), String> {
    match choice {
        portal::CookieChoice::Unmanaged => {
            crate::cookie::set_file_watch_paused(false);
            if let Some(raw) = crate::cookie::saved_file_text() {
                crate::cookie::install_cookie_text(&drive.cookie_store(), &raw)?;
            }
            Ok(())
        }
        portal::CookieChoice::Use { raw, .. } => {
            crate::cookie::set_file_watch_paused(true);
            crate::cookie::install_cookie_text(&drive.cookie_store(), raw)
        }
    }
}

pub(crate) fn cookie_auth_failed(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    const HINTS: &[&str] = &[
        "未登录",
        "登录失效",
        "登录已过期",
        "登录已失效",
        "重新登录",
        "登录态",
        "cookie失效",
        "invalid cookie",
        "require login",
        "not login",
        "status=401",
        "code=401",
        "code=31001",
        "401 unauthorized",
    ];
    HINTS.iter().any(|hint| text.contains(hint) || lower.contains(hint))
}

fn user_cloud_path(settings: &portal::Settings, owner: &str) -> Result<String, String> {
    let name = owner.trim();
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err("账号名不能作为网盘目录".to_string());
    }
    let parent = settings.cloud_dir.trim();
    let parent = if parent.is_empty() {
        "/来自分享"
    } else {
        parent
    };
    let parent = normalize_cloud_path(parent)?;
    if parent == "/" {
        Ok(format!("/{name}"))
    } else {
        Ok(format!("{parent}/{name}"))
    }
}

fn normalize_cloud_path(path: &str) -> Result<String, String> {
    let mut out = String::new();
    for seg in path.split(['/', '\\']) {
        let seg = seg.trim();
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            return Err("路径不能包含 ..".to_string());
        }
        out.push('/');
        out.push_str(seg);
    }
    if out.is_empty() {
        out = "/".to_string();
    }
    Ok(out)
}

fn path_under(root: &str, requested: &str) -> Result<String, String> {
    let root = normalize_cloud_path(root)?;
    if requested.trim().is_empty() {
        return Ok(root);
    }
    let path = normalize_cloud_path(requested)?;
    if path == root || path.starts_with(&format!("{root}/")) {
        Ok(path)
    } else {
        Err("只能查看自己的目录".to_string())
    }
}

fn task_fetch_args(
    portal: &Portal,
    raw: &str,
    settings: &portal::Settings,
    owner: &str,
) -> Result<FetchArgs, String> {
    fetch::parse_share_input(raw, "").map_err(|err| brief_error(&format!("{err:#}")))?;
    let browser = portal.is_browser_download(owner);
    let download = if browser {
        None
    } else {
        Some(portal.resolve_download_dir(owner)?)
    };
    if !browser && download.is_none() {
        return Err("请先选择本机保存目录".to_string());
    }
    if portal.user_download_dir(owner).trim().is_empty() {
        return Err("请先选择本机保存目录".to_string());
    }
    let to = user_cloud_path(settings, owner)?;
    let jobs = if settings.jobs == 0 {
        3
    } else {
        settings.jobs.clamp(1, 8)
    };
    Ok(FetchArgs {
        url: raw.to_string(),
        pwd: String::new(),
        to,
        download,
        jobs,
        cloud_only: browser,
        force_save: false,
        only_fids: Vec::new(),
    })
}

fn record_of(task: &DownloadTask) -> crate::meta::DownloadRecord {
    let mut lines: Vec<String> = task
        .lines
        .iter()
        .filter(|line| !line.starts_with(READY_PREFIX))
        .cloned()
        .collect();
    if !task.ready_files.is_empty() {
        let payload: Vec<serde_json::Value> = task
            .ready_files
            .iter()
            .map(|file| {
                serde_json::json!({
                    "rel": file.rel,
                    "fid": file.fid,
                    "size": file.size,
                })
            })
            .collect();
        if let Ok(json) = serde_json::to_string(&payload) {
            lines.push(format!("{READY_PREFIX}{json}"));
        }
    }
    crate::meta::DownloadRecord {
        id: task.id,
        owner: task.owner.clone(),
        raw: task.raw.clone(),
        share_id: task.share_id.clone(),
        only_fids: task.only_fids.clone(),
        phase: task.phase.clone(),
        progress: task.progress.clone(),
        file_name: task.file_name.clone(),
        percent: task.percent,
        speed: task.speed.clone(),
        written: task.written,
        total: task.total,
        error: task.error.clone(),
        lines,
        counted: task.counted,
    }
}

fn parse_ready_files(lines: &[String]) -> Vec<ReadyFile> {
    for line in lines {
        let Some(json) = line.strip_prefix(READY_PREFIX) else {
            continue;
        };
        let Ok(values) = serde_json::from_str::<Vec<serde_json::Value>>(json) else {
            continue;
        };
        return values
            .into_iter()
            .filter_map(|value| {
                Some(ReadyFile {
                    rel: value.get("rel")?.as_str()?.to_string(),
                    fid: value.get("fid")?.as_str()?.to_string(),
                    size: value.get("size")?.as_u64().unwrap_or(0),
                })
            })
            .collect();
    }
    Vec::new()
}

fn store_ready_files(task: &mut DownloadTask, files: &[(String, String, u64)]) {
    task.ready_files = files
        .iter()
        .map(|(rel, fid, size)| ReadyFile {
            rel: rel.clone(),
            fid: fid.clone(),
            size: *size,
        })
        .collect();
    let total: u64 = task.ready_files.iter().map(|file| file.size).sum();
    task.total = total;
    task.written = total;
    task.percent = if task.ready_files.is_empty() { 0 } else { 100 };
    if let Some(first) = task.ready_files.first() {
        task.file_name = first
            .rel
            .rsplit('/')
            .next()
            .unwrap_or(first.rel.as_str())
            .to_string();
    }
    if !task.ready_files.is_empty() {
        task.progress = format!("待保存到本机（{} 个文件）", task.ready_files.len());
    }
}

fn save_task(portal: &Portal, task: &mut DownloadTask, force: bool) {
    let now = Instant::now();
    if !force && now.duration_since(task.last_saved) < Duration::from_secs(2) {
        return;
    }
    task.last_saved = now;
    portal.save_download(&record_of(task));
}

fn task_from_record(portal: &Portal, mut record: crate::meta::DownloadRecord) -> DownloadTask {
    if record.phase == "排队" || record.phase == "下载中" {
        let refund = record.phase == "排队" && record.counted;
        record.counted = false;
        record.phase = "失败".to_string();
        record.progress = "服务重启，这次没有继续".to_string();
        record.error = Some(record.progress.clone());
        if refund {
            portal.refund_download(&record.owner);
        }
        portal.save_download(&record);
    }
    let ready_files = parse_ready_files(&record.lines);
    let lines: VecDeque<String> = record
        .lines
        .into_iter()
        .filter(|line| !line.starts_with(READY_PREFIX))
        .collect();
    DownloadTask {
        id: record.id,
        owner: record.owner,
        raw: record.raw,
        share_id: record.share_id,
        only_fids: record.only_fids,
        phase: record.phase,
        progress: record.progress,
        file_name: record.file_name,
        percent: record.percent,
        speed: record.speed,
        written: record.written,
        total: record.total,
        error: record.error,
        lines,
        ready_files,
        cancel: Arc::new(AtomicBool::new(false)),
        counted: record.counted,
        last_saved: Instant::now(),
    }
}

fn finish_task(portal: &Portal, tasks: &Mutex<Vec<DownloadTask>>, id: u64, result: Result<(), String>) {
    let summary = {
        let mut tasks = lock_tasks(tasks);
        let Some(task) = tasks.iter_mut().find(|task| task.id == id) else {
            return;
        };
        if task.cancel.load(Ordering::Relaxed) {
            task.phase = "已取消".to_string();
            task.progress = "已取消".to_string();
            task.error = None;
        } else {
            match result {
                Ok(()) => {
                    task.phase = "完成".to_string();
                    task.error = None;
                    if !task.ready_files.is_empty()
                        && (task.progress.is_empty() || task.progress == "完成")
                    {
                        task.progress =
                            format!("待保存到本机（{} 个文件）", task.ready_files.len());
                        task.percent = 100;
                    } else if task.progress.is_empty() {
                        task.progress = "完成".to_string();
                    }
                }
                Err(message) => {
                    let detail = tidy_detail(&message);
                    let shown = if detail.is_empty() { message } else { detail };
                    let brief = brief_error(&shown);
                    push_line(&mut task.lines, &format!("失败: {brief}"));
                    task.error = Some(shown);
                    task.phase = "失败".to_string();
                    task.progress = brief;
                }
            }
        }
        save_task(portal, task, true);
        (task.owner.clone(), task.phase.clone(), task.progress.clone())
    };
    portal.log_op(&summary.0, "下载结束", &format!("{} {}", summary.1, summary.2), summary.1 == "完成");
}

struct TaskReport {
    tasks: Arc<Mutex<Vec<DownloadTask>>>,
    id: u64,
    last_progress: Mutex<HashMap<String, (Instant, u64)>>,
    cancel: Arc<AtomicBool>,
    portal: Arc<Portal>,
}

impl FetchReport for TaskReport {
    fn log(&self, line: &str) {
        if !user_log_line(line) {
            return;
        }
        let mut tasks = lock_tasks(&self.tasks);
        if let Some(task) = tasks.iter_mut().find(|task| task.id == self.id) {
            push_line(&mut task.lines, line);
            save_task(&self.portal, task, false);
        }
    }

    fn phase(&self, phase: &str) {
        let shown = match phase {
            "读取分享" | "准备目录" | "转存" => "准备下载",
            "下载" => "正在下载",
            other => other,
        };
        let mut tasks = lock_tasks(&self.tasks);
        if let Some(task) = tasks.iter_mut().find(|task| task.id == self.id) {
            task.progress = shown.to_string();
            save_task(&self.portal, task, true);
        }
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }

    fn ready_files(&self, files: &[(String, String, u64)]) {
        let mut tasks = lock_tasks(&self.tasks);
        if let Some(task) = tasks.iter_mut().find(|task| task.id == self.id) {
            store_ready_files(task, files);
            save_task(&self.portal, task, true);
        }
    }

    fn file(&self, name: &str, written: u64, total: u64, kind: &str) {
        let now = Instant::now();
        let mut speed = String::new();
        if kind == "progress" {
            let mut last = self
                .last_progress
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            if let Some((prev_at, prev_written)) = last.get(name).copied() {
                let elapsed = now.duration_since(prev_at);
                if elapsed < Duration::from_millis(400) {
                    return;
                }
                let secs = elapsed.as_secs_f64().max(0.001);
                if written >= prev_written {
                    speed = fmt_speed(((written - prev_written) as f64 / secs) as u64);
                }
            }
            last.insert(name.to_string(), (now, written));
        }
        let percent = if kind == "done" || kind == "skipped" {
            100
        } else if total > 0 {
            written.saturating_mul(100) / total.max(1)
        } else {
            0
        };
        let label = match kind {
            "skipped" => format!("{name} 已存在"),
            "done" => format!("{name} 完成"),
            "error" => format!("{name} 失败"),
            "start" => format!("开始 {name}"),
            _ => {
                if total > 0 {
                    format!("{percent}%")
                } else {
                    format!("下载 {name}")
                }
            }
        };
        let mut tasks = lock_tasks(&self.tasks);
        if let Some(task) = tasks.iter_mut().find(|task| task.id == self.id) {
            task.file_name = name.to_string();
            task.percent = percent;
            task.written = written;
            task.total = total;
            if !speed.is_empty() {
                task.speed = speed;
            }
            if kind == "done" || kind == "skipped" || kind == "error" {
                task.speed.clear();
            }
            task.progress = label;
            save_task(&self.portal, task, kind != "progress");
        }
    }
}

fn user_log_line(line: &str) -> bool {
    let text = line.trim();
    text.starts_with("完成:")
        || text.starts_with('✓')
        || text.starts_with('✗')
        || text.starts_with("失败:")
        || text.starts_with("待下载")
        || text.starts_with("没有文件需要下载")
}

fn task_snapshot(task: &DownloadTask) -> serde_json::Value {
    let lines: Vec<&str> = task
        .lines
        .iter()
        .filter(|line| !line.starts_with(READY_PREFIX))
        .map(String::as_str)
        .collect();
    serde_json::json!({
        "id": task.id,
        "raw": task.raw,
        "phase": task.phase,
        "progress": task.progress,
        "error": task.error,
        "error_brief": task.error.as_deref().map(brief_error),
        "lines": lines,
        "file_name": task.file_name,
        "percent": task.percent,
        "speed": task.speed,
        "written": task.written,
        "total": task.total,
        "ready_count": task.ready_files.len(),
    })
}

fn fmt_speed(bytes_per_sec: u64) -> String {
    const UNITS: [&str; 4] = ["B/s", "KB/s", "MB/s", "GB/s"];
    let mut value = bytes_per_sec as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes_per_sec} B/s")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn lock_tasks(tasks: &Mutex<Vec<DownloadTask>>) -> std::sync::MutexGuard<'_, Vec<DownloadTask>> {
    tasks.lock().unwrap_or_else(|err| err.into_inner())
}

fn lock_queue(queue: &Mutex<VecDeque<u64>>) -> std::sync::MutexGuard<'_, VecDeque<u64>> {
    queue.lock().unwrap_or_else(|err| err.into_inner())
}

fn requested_with(req: &Request<hyper::body::Incoming>) -> bool {
    req.headers()
        .get("x-requested-with")
        .and_then(|value| value.to_str().ok())
        == Some(REQUESTED_WITH)
}

fn request_origin(req: &Request<hyper::body::Incoming>) -> String {
    let proto = req
        .headers()
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("http")
        .split(',')
        .next()
        .unwrap_or("http")
        .trim();
    let host = req
        .headers()
        .get("x-forwarded-host")
        .or_else(|| req.headers().get(hyper::header::HOST))
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(',')
        .next()
        .unwrap_or("")
        .trim();
    if host.is_empty() {
        String::new()
    } else {
        format!("{proto}://{host}")
    }
}

fn cookie_from(req: &Request<hyper::body::Incoming>, name: &str) -> Option<String> {
    let header = req
        .headers()
        .get(hyper::header::COOKIE)
        .and_then(|value| value.to_str().ok());
    portal::cookie_value(header, name)
}

fn set_cookie(response: &mut Response<Body>, name: &str, token: &str, max_age: u64) {
    let value = format!("{name}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}");
    if let Ok(header) = hyper::header::HeaderValue::from_str(&value) {
        // append: never replace another Set-Cookie (e.g. qd_user + qd_admin together)
        response.headers_mut().append(hyper::header::SET_COOKIE, header);
    }
}

fn max_age_until(expires_at: i64) -> u64 {
    let left = expires_at.saturating_sub(portal::now_ms()) / 1000;
    left.clamp(60, 10 * 365 * 24 * 60 * 60) as u64
}

fn refresh_user_cookie(response: &mut Response<Body>, req: &Request<hyper::body::Incoming>, expires_at: i64) {
    if let Some(token) = cookie_from(req, "qd_user") {
        set_cookie(response, "qd_user", &token, max_age_until(expires_at));
    }
}

fn selected_fids(picks: &serde_json::Value, line: &str) -> Vec<String> {
    picks.get(line)
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn clear_cookie(response: &mut Response<Body>, name: &str) {
    let value = format!("{name}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0");
    if let Ok(header) = hyper::header::HeaderValue::from_str(&value) {
        response.headers_mut().append(hyper::header::SET_COOKIE, header);
    }
}

async fn read_json(
    req: Request<hyper::body::Incoming>,
) -> Result<serde_json::Value, Response<Body>> {
    let content_type = req
        .headers()
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !content_type.contains("application/json") {
        return Err(json_error(415, "请用 JSON 提交"));
    }
    if let Some(len) = req
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
    {
        if len > MAX_BODY {
            return Err(json_error(400, "请求太大"));
        }
    }
    let collected = match Limited::new(req.into_body(), MAX_BODY).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Err(json_error(400, "请求读取失败，或正文超过 64KB")),
    };
    serde_json::from_slice(&collected).map_err(|_| json_error(400, "请求格式不对"))
}

fn page_html() -> String {
    include_str!("share_page.html").replace("@@TOKEN@@", REQUESTED_WITH)
}

fn admin_page_html() -> String {
    include_str!("admin_page.html").replace("@@TOKEN@@", REQUESTED_WITH)
}

fn dirs_response(query: Option<&str>) -> Response<Body> {
    let path = query_value(query, "path").unwrap_or_default();
    match dir_listing(&path) {
        Ok(value) => json_response(200, value),
        Err(message) => json_error(400, &message),
    }
}

fn query_value(query: Option<&str>, key: &str) -> Option<String> {
    let query = query?;
    for pair in query.split('&') {
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        if raw_key == key {
            return Some(
                percent_encoding::percent_decode_str(raw_value)
                    .decode_utf8_lossy()
                    .into_owned(),
            );
        }
    }
    None
}

#[derive(Serialize)]
struct DirEntry {
    name: String,
    path: String,
    kind: &'static str,
    exists: bool,
}

fn dir_listing(path: &str) -> Result<serde_json::Value, String> {
    let path = path.trim();
    if path.is_empty() {
        return Ok(roots_listing());
    }
    if path.contains('\0') || path.chars().count() > 1024 {
        return Err("目录路径不正确".to_string());
    }
    let dir = PathBuf::from(path);
    if !dir.is_absolute() {
        return Err("目录路径不正确".to_string());
    }
    if !dir.is_dir() {
        return Err("这个目录不存在".to_string());
    }
    let mut entries = Vec::new();
    let read = std::fs::read_dir(&dir).map_err(|_| "无法读取这个目录".to_string())?;
    for item in read {
        let Ok(item) = item else { continue };
        let Ok(file_type) = item.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let name = item.file_name().to_string_lossy().into_owned();
        if name.starts_with('$') || name.eq_ignore_ascii_case("System Volume Information") {
            continue;
        }
        entries.push(DirEntry {
            path: dir.join(&name).display().to_string(),
            name,
            kind: "dir",
            exists: true,
        });
        if entries.len() >= 400 {
            break;
        }
    }
    entries.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    let parent = if dir.parent().is_none() {
        String::new()
    } else {
        dir.parent()
            .map(|parent| parent.display().to_string())
            .unwrap_or_default()
    };
    Ok(serde_json::json!({
        "path": dir.display().to_string(),
        "parent": parent,
        "can_select": true,
        "entries": entries,
    }))
}

fn roots_listing() -> serde_json::Value {
    let mut entries = Vec::new();
    if let Some(home) = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME")) {
        for (name, folder) in [
            ("下载", "Downloads"),
            ("桌面", "Desktop"),
            ("文档", "Documents"),
            ("图片", "Pictures"),
            ("视频", "Videos"),
            ("音乐", "Music"),
        ] {
            let path = PathBuf::from(&home).join(folder);
            if path.is_dir() {
                entries.push(DirEntry {
                    name: name.to_string(),
                    path: path.display().to_string(),
                    kind: "place",
                    exists: true,
                });
            }
        }
    }
    let suggest = suggest_download_dir();
    if !suggest.is_empty() {
        let path = PathBuf::from(&suggest);
        let parent_exists = path.parent().is_some_and(|parent| parent.is_dir());
        if parent_exists {
            entries.push(DirEntry {
                name: "夸克分享".to_string(),
                path: suggest,
                kind: "place",
                exists: path.is_dir(),
            });
        }
    }
    push_drives(&mut entries);
    serde_json::json!({
        "path": "",
        "parent": null,
        "can_select": false,
        "entries": entries,
    })
}

#[cfg(windows)]
fn push_drives(entries: &mut Vec<DirEntry>) {
    for letter in b'A'..=b'Z' {
        let label = letter as char;
        let root = format!("{label}:\\");
        if PathBuf::from(&root).is_dir() {
            entries.push(DirEntry {
                name: format!("本地磁盘 ({label}:)"),
                path: root,
                kind: "drive",
                exists: true,
            });
        }
    }
}

#[cfg(not(windows))]
fn push_drives(entries: &mut Vec<DirEntry>) {
    if PathBuf::from("/").is_dir() {
        entries.push(DirEntry {
            name: "根目录".to_string(),
            path: "/".to_string(),
            kind: "drive",
            exists: true,
        });
    }
}

fn brief_error(full: &str) -> String {
    const HINTS: &[&str] = &[
        "网盘容量不足",
        "网盘目录不存在",
        "提取码错误",
        "提取码",
        "分享不存在",
        "分享已失效",
        "分享 ID 不正确",
        "无法解析分享链接",
        "一次只能解析一条分享链接",
        "请粘贴分享链接",
        "请选择本机保存目录",
        "管理端还没有设置本机保存目录",
        "Cookie",
    ];
    for hint in HINTS {
        if full.contains(hint) {
            return (*hint).to_string();
        }
    }
    let tidy = tidy_detail(full);
    let source = if tidy.is_empty() { full.trim() } else { tidy.as_str() };
    if let Some((_, tail)) = source.rsplit_once(':') {
        let tail = tail.trim().trim_matches('。');
        if !tail.is_empty() && tail.chars().count() <= 36 {
            return tail.to_string();
        }
    }
    let sentence = source.split(['。', '\n']).next().unwrap_or(source).trim();
    if sentence.is_empty() {
        "下载失败".to_string()
    } else if sentence.chars().count() <= 42 {
        sentence.to_string()
    } else {
        let short: String = sentence.chars().take(36).collect();
        format!("{short}…")
    }
}

fn tidy_detail(full: &str) -> String {
    let mut text = strip_placeholders(full);
    loop {
        let lower = text.to_ascii_lowercase();
        let cut = ["capacity limit exceed", "capacity limit"]
            .into_iter()
            .find_map(|noise| {
                let idx = lower.rfind(noise)?;
                let after = lower[idx + noise.len()..].trim_matches(|ch: char| {
                    matches!(ch, ' ' | '。' | '.' | '，' | ',' | ':' | '：')
                });
                if after.is_empty() { Some(idx) } else { None }
            });
        let Some(idx) = cut else { break };
        text.truncate(idx);
        text = text
            .trim()
            .trim_end_matches(['。', '.', ' ', '：', ':', '，', ','])
            .to_string();
    }
    text.trim().to_string()
}

fn strip_placeholders(full: &str) -> String {
    let chars: Vec<char> = full.chars().collect();
    let mut out = String::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '[' && chars.get(index + 1) == Some(&'{') {
            if let Some(end) = chars[index..].iter().position(|ch| *ch == ']') {
                index += end + 1;
                continue;
            }
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_share_routes() {
        assert!(!is_share_path("/", None));
        assert!(is_share_path("/share", None));
        assert!(is_share_path("/share/", None));
        assert!(is_share_path("/admin", None));
        assert!(is_share_path("/api/share", None));
        assert!(is_share_path("/api/tasks", None));
        assert!(is_share_path("/api/login", None));
        assert!(is_share_path("/api/dirs", None));
        assert!(!is_share_path("/api/share/extra", None));
        assert!(!is_share_path("/api/dirs/extra", None));
        assert!(!is_share_path("/folder/share", None));
        assert!(is_share_path("/dav/share", Some("/dav")));
        assert!(is_share_path("/dav/api/tasks", Some("/dav")));
        assert!(is_share_path("/share", Some("/dav")));
    }

    #[test]
    fn page_posts_to_the_api() {
        let html = page_html();
        assert!(html.contains("分享链接"));
        assert!(html.contains("开始下载"));
        assert!(html.contains("id=\"file-modal\""));
        assert!(html.contains("/api/files"));
        assert!(html.contains("/api/download-dir"));
        assert!(html.contains("id=\"pick-dir\""));
        assert!(html.contains("/api/tasks"));
        assert!(html.contains("/api/preview"));
        assert!(html.contains("/api/tasks/cancel"));
        assert!(html.contains("下载所选"));
        assert!(html.contains("查看详情"));
        assert!(html.contains("id=\"open-records\""));
        assert!(html.contains("id=\"record-badge\""));
        assert!(html.contains("id=\"record-modal\""));
        assert!(html.contains("id=\"log-modal\""));
        assert!(!html.contains("textContent = \"下载\""));
        assert!(html.contains("/api/login"));
        assert!(html.contains(REQUESTED_WITH));
        assert!(!html.contains("@@TOKEN@@"));
        assert!(!html.contains("选择目录"));
        assert!(!html.contains("/admin"));
        let admin = admin_page_html();
        assert!(admin.contains("网盘目录"));
        assert!(admin.contains("data-view=\"drive\""));
        assert!(admin.contains("/api/admin/drive"));
        assert!(!admin.contains("id=\"pick-dir\""));
        assert!(admin.contains("有效期"));
        assert!(admin.contains("/api/admin/users"));
        assert!(admin.contains("datetime-local"));
        assert!(admin.contains("data-view=\"cookies\""));
        assert!(admin.contains("/api/admin/cookies"));
        assert!(admin.contains("id=\"cookie-modal\""));
        assert!(admin.contains("添加 Cookie"));
        assert!(admin.contains("绑定 Cookie"));
        assert!(admin.contains("同时下载"));
        assert!(admin.contains("同时最多下载几个文件"));
        assert!(admin.contains("添加账号"));
        assert!(admin.contains("id=\"user-page-size\""));
        assert!(admin.contains("id=\"user-add-modal\""));
        assert!(admin.contains("/api/admin/users/bind"));
        assert!(admin.contains("/api/admin/users/delivery"));
        assert!(admin.contains("id=\"bind-expire\""));
        assert!(admin.contains("编辑账号"));
        assert!(admin.contains("生成图片"));
        assert!(admin.contains("id=\"delivery-modal\""));
        assert!(admin.contains("id=\"delivery-preview\""));
        assert!(admin.contains("/api/admin/users/expiry"));
        assert!(admin.contains("id=\"shell\""));
        assert!(admin.contains("id=\"gate-title\""));
        assert!(admin.contains("qd-admin-view"));
        assert!(admin.contains("操作记录"));
        assert!(admin.contains("/api/admin/logs"));
        assert!(admin.contains("id=\"saved-modal\""));
        assert!(admin.contains("保存成功"));
        assert!(admin.contains("id=\"notice-modal\""));
        assert!(admin.contains("个人中心"));
        assert!(admin.contains("修改密码"));
        assert!(admin.contains("/api/admin/password"));
        assert!(admin.contains("id=\"password-modal\""));
        assert!(admin.contains("id=\"account\""));
        assert!(!admin.contains("confirm("));
        assert!(!admin.contains("@@TOKEN@@"));
    }

    #[test]
    fn capacity_failure_is_short_and_keeps_the_detail() {
        let raw = "转存中断。如果网盘里已经出现同名文件，再跑一次同样的命令会复用它们并继续下载: 转存失败: 网盘容量不足。capacity limit[{0}]";
        let detail = tidy_detail(raw);
        assert_eq!(brief_error(&detail), "网盘容量不足");
        assert!(detail.contains("再跑一次"));
        assert!(!detail.to_ascii_lowercase().contains("capacity"));
        assert!(!detail.contains("[{0}]"));
    }

    #[test]
    fn lists_child_directories_only() {
        let root = std::env::temp_dir().join(format!("quark-dir-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("子目录")).unwrap();
        std::fs::write(root.join("note.txt"), b"x").unwrap();
        let listing = dir_listing(&root.display().to_string()).unwrap();
        let names: Vec<String> = listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"子目录".to_string()), "{names:?}");
        assert!(!names.iter().any(|name| name.contains("note")));
        assert_eq!(listing["can_select"], true);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_empty_url_and_relative_dir() {
        let err = build_args(&StartBody {
            url: "  ".to_string(),
            pwd: String::new(),
            to: String::new(),
            download: String::new(),
            cloud_only: false,
            force_save: false,
            jobs: 0,
        })
        .unwrap_err();
        assert!(err.contains("分享链接"));

        let err = build_args(&StartBody {
            url: "https://pan.quark.cn/s/abcd".to_string(),
            pwd: String::new(),
            to: String::new(),
            download: "Downloads".to_string(),
            cloud_only: false,
            force_save: false,
            jobs: 99,
        })
        .unwrap_err();
        assert!(err.contains("绝对路径"));
    }

    #[test]
    fn accepts_absolute_dir_and_cloud_only() {
        let absolute = if cfg!(windows) {
            r"D:\Downloads\夸克分享"
        } else {
            "/tmp/quark-share"
        };
        let args = build_args(&StartBody {
            url: " https://pan.quark.cn/s/abcd ".to_string(),
            pwd: " 12 ".to_string(),
            to: String::new(),
            download: absolute.to_string(),
            cloud_only: false,
            force_save: true,
            jobs: 99,
        })
        .unwrap();
        assert_eq!(args.url, "https://pan.quark.cn/s/abcd");
        assert_eq!(args.pwd, "12");
        assert_eq!(args.to, "/来自分享");
        assert_eq!(args.jobs, 8);
        assert!(args.force_save);
        assert_eq!(args.download.unwrap().display().to_string(), absolute);

        let args = build_args(&StartBody {
            url: "abcd".to_string(),
            pwd: String::new(),
            to: "/电影".to_string(),
            download: String::new(),
            cloud_only: true,
            force_save: false,
            jobs: 0,
        })
        .unwrap();
        assert!(args.download.is_none());
        assert!(args.cloud_only);
        assert_eq!(args.to, "/电影");
        assert_eq!(args.jobs, 3);
    }

    #[test]
    fn suggest_dir_is_under_downloads_when_home_exists() {
        if std::env::var_os("USERPROFILE").is_some() || std::env::var_os("HOME").is_some() {
            assert!(suggest_download_dir().contains("夸克分享"));
        }
    }

    #[test]
    fn queues_one_share_paste() {
        use dashmap::DashMap;
        let cookie = Arc::new(DashMap::new());
        cookie.insert("test".to_string(), "value".to_string());
        let drive = QuarkDrive::new(crate::drive::DriveConfig {
            api_base_url: "https://drive.quark.cn".to_string(),
            cookie,
        })
        .unwrap();
        let hub = ShareHub::new(drive);
        let paste = "我用夸克网盘给你分享了「格物心法」，点击链接或复制整段内容，打开「夸克APP」即可获取。\n链接：https://pan.quark.cn/s/Ab12Cd\n提取码：7788";
        let tasks = hub.enqueue("paste-check", paste, None, "queue").unwrap();
        let tasks = tasks.as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["phase"], "排队");
        assert!(tasks[0]["raw"].as_str().unwrap().contains("提取码"));
        let again = hub
            .enqueue("paste-check", "https://pan.quark.cn/s/Ab12Cd 7788", None, "queue")
            .unwrap();
        let again = again.as_array().unwrap();
        assert_eq!(again[0]["phase"], "失败");
        assert!(again[0]["error"].as_str().unwrap().contains("正在下载"));
        let err = hub
            .enqueue(
                "paste-check",
                "https://pan.quark.cn/s/aaaaaaaa\nhttps://pan.quark.cn/s/bbbbbbbb",
                None,
                "queue",
            )
            .unwrap_err();
        assert!(err.contains("一条"));
        let settings = portal::Settings {
            cloud_dir: "/来自分享".to_string(),
            download_dir: String::new(),
            jobs: 3,
        };
        let portal_dir = std::env::temp_dir().join(format!("qd-portal-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&portal_dir);
        let portal = Portal::open(portal_dir.join("portal.json"));
        portal
            .add_user("alice", "secret1", "2099-01-01", "", 2)
            .unwrap();
        let err = task_fetch_args(&portal, "https://pan.quark.cn/s/Ab12Cd", &settings, "alice")
            .unwrap_err();
        assert!(err.contains("请先选择本机保存目录"));
        portal
            .set_user_download_dir("alice", std::env::temp_dir().to_str().unwrap())
            .unwrap();
        let saved =
            task_fetch_args(&portal, "https://pan.quark.cn/s/Ab12Cd", &settings, "alice").unwrap();
        assert_eq!(saved.to, "/来自分享/alice");
        assert_eq!(saved.download.unwrap(), std::env::temp_dir());
        assert!(!saved.cloud_only);
        portal
            .set_user_download_dir("alice", portal::Portal::BROWSER_DIR_MARK)
            .unwrap();
        let browser =
            task_fetch_args(&portal, "https://pan.quark.cn/s/Ab12Cd", &settings, "alice").unwrap();
        assert!(browser.cloud_only);
        assert!(browser.download.is_none());
        assert_eq!(user_cloud_path(&settings, "alice").unwrap(), "/来自分享/alice");
        let custom = portal::Settings {
            cloud_dir: "/电影".to_string(),
            download_dir: String::new(),
            jobs: 3,
        };
        assert_eq!(user_cloud_path(&custom, "alice").unwrap(), "/电影/alice");
        assert!(user_cloud_path(&settings, "a/b").is_err());
        assert!(user_cloud_path(&settings, "..").is_err());
        assert_eq!(
            path_under("/来自分享/alice", "/来自分享/alice/电影").unwrap(),
            "/来自分享/alice/电影"
        );
        assert!(path_under("/来自分享/alice", "/来自分享").is_err());
        assert!(path_under("/来自分享/alice", "/来自分享/alice/../bob").is_err());
    }
}
