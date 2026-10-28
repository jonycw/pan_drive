//! Local admin password, user accounts, and download settings.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

const ADMIN_SESSION_MS: i64 = 60 * 60 * 1000;
const ADMIN_RENEW_WINDOW_MS: i64 = 30 * 60 * 1000;

#[derive(Clone, Debug)]
pub enum Role {
    Admin,
    User(String),
}

#[derive(Clone, Debug)]
struct Session {
    role: Role,
    expires_at: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Settings {
    pub cloud_dir: String,
    pub download_dir: String,
    pub jobs: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct UserView {
    pub name: String,
    pub expires_at: i64,
    pub expires_on: String,
    pub expired: bool,
    pub cookie_id: String,
    pub cookie_label: String,
    pub download_limit: u32,
    pub downloads_used: u32,
    pub created_at: i64,
    pub created_on: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct DiskUser {
    name: String,
    salt: String,
    hash: String,
    expires_at: i64,
    #[serde(default)]
    download_dir: String,
    #[serde(default)]
    cookie_id: String,
    #[serde(default = "default_download_limit")]
    download_limit: u32,
    #[serde(default)]
    downloads_used: u32,
    #[serde(default)]
    created_at: i64,
}

#[derive(Clone, Serialize, Deserialize)]
struct DiskSettings {
    cloud_dir: String,
    download_dir: String,
    jobs: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct CookieView {
    pub id: String,
    pub label: String,
    pub preview: String,
    pub value: String,
    pub status: String,
    pub error: String,
    pub active: bool,
    pub bound: usize,
}

#[derive(Clone, Debug)]
pub enum CookieChoice {
    Unmanaged,
    Use { id: String, label: String, raw: String },
}

#[derive(Clone, Serialize, Deserialize)]
struct DiskCookie {
    id: String,
    label: String,
    raw: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    error: String,
    #[serde(default)]
    active: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Disk {
    admin_salt: String,
    admin_hash: String,
    settings: DiskSettings,
    users: Vec<DiskUser>,
    #[serde(default)]
    cookies: Vec<DiskCookie>,
}

pub struct Portal {
    path: PathBuf,
    disk: Mutex<Disk>,
    sessions: Mutex<HashMap<String, Session>>,
    meta: Option<crate::meta::Meta>,
}

impl Portal {
    pub fn open(path: PathBuf) -> Self {
        let disk = read_disk(&path).unwrap_or_else(empty_disk);
        Self {
            path,
            disk: Mutex::new(disk),
            sessions: Mutex::new(HashMap::new()),
            meta: None,
        }
    }

    pub fn admin_ready(&self) -> bool {
        !lock(&self.disk).admin_hash.is_empty()
    }

    pub fn setup_admin(&self, password: &str) -> Result<(), String> {
        let mut disk = lock(&self.disk);
        if !disk.admin_hash.is_empty() {
            return Err("管理密码已经设置".to_string());
        }
        check_password(password)?;
        let salt = random_token();
        disk.admin_salt = salt.clone();
        disk.admin_hash = hash_password(&salt, password);
        self.persist(&disk)
    }

    pub fn change_admin_password(
        &self,
        old_password: &str,
        new_password: &str,
        keep_token: Option<&str>,
    ) -> Result<(), String> {
        check_password(new_password)?;
        if old_password == new_password {
            return Err("新密码不能和旧密码一样".to_string());
        }
        let mut disk = lock(&self.disk);
        if disk.admin_hash.is_empty() {
            return Err("请先设置管理密码".to_string());
        }
        if hash_password(&disk.admin_salt, old_password) != disk.admin_hash {
            return Err("旧密码不正确".to_string());
        }
        let salt = random_token();
        disk.admin_salt = salt.clone();
        disk.admin_hash = hash_password(&salt, new_password);
        self.persist(&disk)?;
        drop(disk);
        let keep = keep_token.unwrap_or("");
        let dropped: Vec<String> = {
            let mut sessions = lock(&self.sessions);
            let ids: Vec<String> = sessions
                .iter()
                .filter(|(token, session)| {
                    matches!(session.role, Role::Admin) && token.as_str() != keep
                })
                .map(|(token, _)| token.clone())
                .collect();
            for token in &ids {
                sessions.remove(token);
            }
            ids
        };
        for token in dropped {
            self.delete_session(&token);
        }
        Ok(())
    }

    pub fn login_admin(&self, password: &str) -> Result<String, String> {
        let disk = lock(&self.disk);
        if disk.admin_hash.is_empty() {
            return Err("请先设置管理密码".to_string());
        }
        if hash_password(&disk.admin_salt, password) != disk.admin_hash {
            return Err("管理密码不正确".to_string());
        }
        drop(disk);
        Ok(self.insert_session(Role::Admin))
    }

    pub fn login_user(&self, name: &str, password: &str) -> Result<(String, String), String> {
        let disk = lock(&self.disk);
        let name = name.trim();
        let Some(user) = disk.users.iter().find(|user| user.name == name) else {
            return Err("账号或密码不正确".to_string());
        };
        if hash_password(&user.salt, password) != user.hash {
            return Err("账号或密码不正确".to_string());
        }
        if user.expires_at <= now_ms() {
            return Err("账号已过期".to_string());
        }
        let expires_on = format_expiry(user.expires_at);
        let name = user.name.clone();
        drop(disk);
        let token = self.issue_user_token(&name).unwrap_or_default();
        if token.is_empty() {
            return Err("账号已过期".to_string());
        }
        Ok((token, expires_on))
    }

    pub fn issue_user_token(&self, name: &str) -> Option<String> {
        let disk = lock(&self.disk);
        let user = disk.users.iter().find(|user| user.name == name)?;
        if user.expires_at <= now_ms() {
            return None;
        }
        let sig = sign_user(&user.salt, &user.name, &user.hash);
        Some(format!("u1.{}.{}", hex::encode(user.name.as_bytes()), sig))
    }

    pub fn user_from_token(&self, token: &str) -> Option<String> {
        let rest = token.strip_prefix("u1.")?;
        let (name_hex, sig) = rest.split_once('.')?;
        let name = String::from_utf8(hex::decode(name_hex).ok()?).ok()?;
        let disk = lock(&self.disk);
        let user = disk.users.iter().find(|user| user.name == name)?;
        if user.expires_at <= now_ms() {
            return None;
        }
        if sign_user(&user.salt, &user.name, &user.hash) != sig {
            return None;
        }
        Some(name)
    }

    pub fn user_expires_at(&self, name: &str) -> Option<i64> {
        lock(&self.disk)
            .users
            .iter()
            .find(|user| user.name == name)
            .map(|user| user.expires_at)
    }

    pub fn role(&self, token: &str) -> Option<Role> {
        if let Some(name) = self.user_from_token(token) {
            return Some(Role::User(name));
        }
        let mut sessions = lock(&self.sessions);
        let session = sessions.get(token)?;
        if session.expires_at <= now_ms() {
            sessions.remove(token);
            drop(sessions);
            self.delete_session(token);
            return None;
        }
        let role = session.role.clone();
        drop(sessions);
        if let Role::User(name) = &role {
            let disk = lock(&self.disk);
            let Some(user) = disk.users.iter().find(|user| &user.name == name) else {
                return None;
            };
            if user.expires_at <= now_ms() {
                return None;
            }
        }
        Some(role)
    }

    pub fn logout(&self, token: &str) {
        lock(&self.sessions).remove(token);
        self.delete_session(token);
    }

    /// Admin sessions last 60 minutes. Activity during the final 30 minutes
    /// starts a fresh 60-minute window. Earlier activity does not extend it.
    pub fn touch_admin(&self, token: &str) -> Option<i64> {
        let (expires, changed) = {
            let mut sessions = lock(&self.sessions);
            let session = sessions.get_mut(token)?;
            if !matches!(session.role, Role::Admin) {
                return None;
            }
            let now = now_ms();
            if session.expires_at <= now {
                sessions.remove(token);
                (None, false)
            } else {
                let renew = session.expires_at - now <= ADMIN_RENEW_WINDOW_MS;
                if renew {
                    session.expires_at = now + ADMIN_SESSION_MS;
                }
                (Some(session.expires_at), renew)
            }
        };
        match expires {
            None => {
                self.delete_session(token);
                None
            }
            Some(expires_at) => {
                if changed {
                    self.save_session(token, expires_at);
                }
                Some(expires_at)
            }
        }
    }

    pub fn settings(&self) -> Settings {
        let disk = lock(&self.disk);
        Settings {
            cloud_dir: disk.settings.cloud_dir.clone(),
            download_dir: disk.settings.download_dir.clone(),
            jobs: disk.settings.jobs,
        }
    }

    /// Marker: browser saves to the user's PC (no server-side download directory).
    pub const BROWSER_DIR_MARK: &'static str = "__browser__";

    pub fn data_dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    pub fn is_browser_download(&self, name: &str) -> bool {
        self.user_download_dir(name) == Self::BROWSER_DIR_MARK
    }

    pub fn resolve_download_dir(&self, name: &str) -> Result<PathBuf, String> {
        let raw = self.user_download_dir(name);
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("请先选择本机保存目录".to_string());
        }
        if raw == Self::BROWSER_DIR_MARK {
            return Err("浏览器本机保存模式不写服务器目录".to_string());
        }
        let path = PathBuf::from(raw);
        if !path.is_absolute() {
            return Err("本机目录请选择绝对路径".to_string());
        }
        Ok(path)
    }

    pub fn user_download_dir(&self, name: &str) -> String {
        lock(&self.disk)
            .users
            .iter()
            .find(|user| user.name == name)
            .map(|user| user.download_dir.clone())
            .unwrap_or_default()
    }

    pub fn set_user_download_dir(&self, name: &str, path: &str) -> Result<String, String> {
        let path = path.trim();
        if path.is_empty() {
            return Err("请先选择本机保存目录".to_string());
        }
        if path != Self::BROWSER_DIR_MARK && !Path::new(path).is_absolute() {
            return Err("本机目录请选择绝对路径".to_string());
        }
        let mut disk = lock(&self.disk);
        let Some(user) = disk.users.iter_mut().find(|user| user.name == name) else {
            return Err("没有这个账号".to_string());
        };
        user.download_dir = path.to_string();
        let saved = user.download_dir.clone();
        self.persist(&disk)?;
        Ok(saved)
    }

    pub fn user_expiry_label(&self, name: &str) -> Option<String> {
        let disk = lock(&self.disk);
        disk.users
            .iter()
            .find(|user| user.name == name)
            .map(|user| format_expiry(user.expires_at))
    }

    pub fn update_settings(
        &self,
        cloud_dir: &str,
        download_dir: &str,
        jobs: usize,
    ) -> Result<Settings, String> {
        let cloud_dir = cloud_dir.trim();
        let cloud_dir = if cloud_dir.is_empty() {
            "/来自分享".to_string()
        } else {
            cloud_dir.to_string()
        };
        let download_dir = download_dir.trim().to_string();
        if !download_dir.is_empty() && !Path::new(&download_dir).is_absolute() {
            return Err("本机目录请选择绝对路径".to_string());
        }
        let jobs = if jobs == 0 { 3 } else { jobs.clamp(1, 8) };
        let mut disk = lock(&self.disk);
        disk.settings = DiskSettings {
            cloud_dir,
            download_dir,
            jobs,
        };
        self.persist(&disk)?;
        Ok(Settings {
            cloud_dir: disk.settings.cloud_dir.clone(),
            download_dir: disk.settings.download_dir.clone(),
            jobs: disk.settings.jobs,
        })
    }

    pub fn users(&self) -> Vec<UserView> {
        let now = now_ms();
        let disk = lock(&self.disk);
        let mut views: Vec<_> = disk
            .users
            .iter()
            .map(|user| {
                let cookie_label = disk
                    .cookies
                    .iter()
                    .find(|cookie| cookie.id == user.cookie_id)
                    .map(|cookie| cookie.label.clone())
                    .unwrap_or_default();
                UserView {
                    name: user.name.clone(),
                    expires_at: user.expires_at,
                    expires_on: format_expiry(user.expires_at),
                    expired: user.expires_at <= now,
                    cookie_id: user.cookie_id.clone(),
                    cookie_label,
                    download_limit: effective_limit(user.download_limit),
                    downloads_used: user.downloads_used,
                    created_at: user.created_at,
                    created_on: if user.created_at > 0 {
                        format_expiry(user.created_at)
                    } else {
                        String::new()
                    },
                }
            })
            .collect();
        views.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.name.cmp(&right.name))
        });
        views
    }

    pub fn fresh_user_name(&self) -> String {
        let disk = lock(&self.disk);
        for _ in 0..20 {
            let name = format!("u{}", &random_token()[..6]);
            if !disk.users.iter().any(|user| user.name == name) {
                return name;
            }
        }
        format!("u{}", &random_token()[..12])
    }

    pub fn add_user(
        &self,
        name: &str,
        password: &str,
        expires_on: &str,
        cookie_id: &str,
        download_limit: u32,
    ) -> Result<(), String> {
        let name = name.trim();
        if name.is_empty() || name.chars().count() > 32 || name.chars().any(char::is_whitespace) {
            return Err("账号名请用 1 到 32 个字符，不要有空格".to_string());
        }
        check_password(password)?;
        let expires_at = parse_expiry(expires_on)?;
        if expires_at <= now_ms() {
            return Err("有效期需要是现在以后的时间".to_string());
        }
        let cookie_id = cookie_id.trim();
        let download_limit = normalize_limit(download_limit);
        let mut disk = lock(&self.disk);
        if disk.users.iter().any(|user| user.name == name) {
            return Err("这个账号已经存在".to_string());
        }
        if cookie_id.is_empty() {
            if !disk.cookies.is_empty() {
                return Err("请选择要绑定的 Cookie".to_string());
            }
        } else {
            check_bind(&disk, cookie_id, None)?;
        }
        let salt = random_token();
        disk.users.push(DiskUser {
            name: name.to_string(),
            salt: salt.clone(),
            hash: hash_password(&salt, password),
            expires_at,
            download_dir: String::new(),
            cookie_id: cookie_id.to_string(),
            download_limit,
            downloads_used: 0,
            created_at: now_ms(),
        });
        self.persist(&disk)
    }

    pub fn update_user_binding(&self, name: &str, cookie_id: &str, download_limit: u32) -> Result<(), String> {
        let name = name.trim();
        let cookie_id = cookie_id.trim();
        if cookie_id.is_empty() {
            return Err("请选择要绑定的 Cookie".to_string());
        }
        let download_limit = normalize_limit(download_limit);
        let mut disk = lock(&self.disk);
        check_bind(&disk, cookie_id, Some(name))?;
        let Some(user) = disk.users.iter_mut().find(|user| user.name == name) else {
            return Err("没有这个账号".to_string());
        };
        user.cookie_id = cookie_id.to_string();
        user.download_limit = download_limit;
        self.persist(&disk)
    }

    pub fn ensure_can_download(&self, name: &str) -> Result<(), String> {
        self.cookie_for_user(name)?;
        Ok(())
    }

    pub fn user_parallel(&self, name: &str) -> usize {
        let disk = lock(&self.disk);
        let limit = disk
            .users
            .iter()
            .find(|user| user.name == name)
            .map(|user| user.download_limit)
            .unwrap_or(2);
        normalize_limit(limit) as usize
    }

    pub fn refund_download(&self, name: &str) {
        let mut disk = lock(&self.disk);
        let Some(user) = disk.users.iter_mut().find(|user| user.name == name) else {
            return;
        };
        if user.downloads_used == 0 {
            return;
        }
        user.downloads_used -= 1;
        let _ = self.persist(&disk);
    }

    pub fn cookie_for_user(&self, name: &str) -> Result<CookieChoice, String> {
        let disk = lock(&self.disk);
        let Some(user) = disk.users.iter().find(|user| user.name == name) else {
            return Err("没有这个账号".to_string());
        };
        if user.cookie_id.is_empty() {
            if disk.cookies.is_empty() {
                return Ok(CookieChoice::Unmanaged);
            }
            return Err("这个账号还没有绑定 Cookie".to_string());
        }
        let Some(cookie) = disk.cookies.iter().find(|cookie| cookie.id == user.cookie_id) else {
            return Err("绑定的 Cookie 已经删除".to_string());
        };
        if cookie.status == "invalid" {
            return Err("绑定的 Cookie 已失效".to_string());
        }
        Ok(CookieChoice::Use {
            id: cookie.id.clone(),
            label: cookie.label.clone(),
            raw: cookie.raw.clone(),
        })
    }

    pub fn update_user_expiry(&self, name: &str, expires_on: &str) -> Result<(), String> {
        let name = name.trim();
        let expires_at = parse_expiry(expires_on)?;
        if expires_at <= now_ms() {
            return Err("有效期需要是现在以后的时间".to_string());
        }
        let mut disk = lock(&self.disk);
        let Some(user) = disk.users.iter_mut().find(|user| user.name == name) else {
            return Err("没有这个账号".to_string());
        };
        user.expires_at = expires_at;
        self.persist(&disk)
    }

    pub fn reset_user_password(&self, name: &str, password: &str) -> Result<(), String> {
        let name = name.trim();
        check_password(password)?;
        let mut disk = lock(&self.disk);
        let Some(user) = disk.users.iter_mut().find(|user| user.name == name) else {
            return Err("没有这个账号".to_string());
        };
        let salt = random_token();
        user.salt = salt.clone();
        user.hash = hash_password(&salt, password);
        self.persist(&disk)
    }

    pub fn user_expires_on(&self, name: &str) -> Result<String, String> {
        let name = name.trim();
        let disk = lock(&self.disk);
        let Some(user) = disk.users.iter().find(|user| user.name == name) else {
            return Err("没有这个账号".to_string());
        };
        Ok(format_expiry(user.expires_at))
    }

    pub fn delete_user(&self, name: &str) -> Result<(), String> {
        let mut disk = lock(&self.disk);
        let before = disk.users.len();
        disk.users.retain(|user| user.name != name);
        if disk.users.len() == before {
            return Err("没有这个账号".to_string());
        }
        self.persist(&disk)?;
        lock(&self.sessions).retain(|_, session| match &session.role {
            Role::User(user) => user != name,
            Role::Admin => true,
        });
        Ok(())
    }

    pub fn has_cookies(&self) -> bool {
        !lock(&self.disk).cookies.is_empty()
    }

    pub fn cookies(&self) -> Vec<CookieView> {
        let disk = lock(&self.disk);
        disk.cookies
            .iter()
            .map(|cookie| {
                let bound = disk
                    .users
                    .iter()
                    .filter(|user| user.cookie_id == cookie.id)
                    .count();
                cookie_view(cookie, bound)
            })
            .collect()
    }

    pub fn add_cookie(&self, label: &str, raw: &str) -> Result<(), String> {
        let label = cookie_label(label)?;
        let raw = check_cookie_raw(raw)?;
        let mut disk = lock(&self.disk);
        if disk.cookies.len() >= 20 {
            return Err("最多保存 20 个 Cookie".to_string());
        }
        let active = !disk.cookies.iter().any(|cookie| cookie.active);
        disk.cookies.push(DiskCookie {
            id: random_token(),
            label,
            raw,
            status: "unknown".to_string(),
            error: String::new(),
            active,
        });
        self.persist(&disk)
    }

    pub fn update_cookie(&self, id: &str, label: &str, raw: Option<&str>) -> Result<(), String> {
        let label = cookie_label(label)?;
        let raw = match raw.map(str::trim).filter(|value| !value.is_empty()) {
            Some(raw) => Some(check_cookie_raw(raw)?),
            None => None,
        };
        let mut disk = lock(&self.disk);
        let Some(cookie) = disk.cookies.iter_mut().find(|cookie| cookie.id == id) else {
            return Err("没有这个 Cookie".to_string());
        };
        cookie.label = label;
        if let Some(raw) = raw {
            if cookie.raw != raw {
                cookie.raw = raw;
                cookie.status = "unknown".to_string();
                cookie.error.clear();
            }
        }
        self.persist(&disk)
    }

    pub fn delete_cookie(&self, id: &str) -> Result<(), String> {
        let mut disk = lock(&self.disk);
        let before = disk.cookies.len();
        let was_active = disk
            .cookies
            .iter()
            .any(|cookie| cookie.id == id && cookie.active);
        disk.cookies.retain(|cookie| cookie.id != id);
        if disk.cookies.len() == before {
            return Err("没有这个 Cookie".to_string());
        }
        for user in &mut disk.users {
            if user.cookie_id == id {
                user.cookie_id.clear();
            }
        }
        if was_active {
            if let Some(cookie) = disk
                .cookies
                .iter_mut()
                .find(|cookie| cookie.status != "invalid")
            {
                cookie.active = true;
            } else if let Some(cookie) = disk.cookies.first_mut() {
                cookie.active = true;
            }
        }
        self.persist(&disk)
    }

    pub fn use_cookie(&self, id: &str) -> Result<String, String> {
        let mut disk = lock(&self.disk);
        if !disk.cookies.iter().any(|cookie| cookie.id == id) {
            return Err("没有这个 Cookie".to_string());
        }
        let mut raw = String::new();
        for cookie in &mut disk.cookies {
            let chosen = cookie.id == id;
            cookie.active = chosen;
            if chosen {
                cookie.status = "unknown".to_string();
                cookie.error.clear();
                raw = cookie.raw.clone();
            }
        }
        self.persist(&disk)?;
        Ok(raw)
    }

    pub fn active_cookie(&self) -> Result<CookieChoice, String> {
        let disk = lock(&self.disk);
        if disk.cookies.is_empty() {
            return Ok(CookieChoice::Unmanaged);
        }
        let Some(cookie) = disk.cookies.iter().find(|cookie| cookie.active) else {
            return Err("还没有设为当前的 Cookie".to_string());
        };
        if cookie.status == "invalid" {
            return Err("当前 Cookie 已失效".to_string());
        }
        Ok(CookieChoice::Use {
            id: cookie.id.clone(),
            label: cookie.label.clone(),
            raw: cookie.raw.clone(),
        })
    }

    pub fn select_cookie(&self, skip: &[String]) -> Result<CookieChoice, String> {
        let mut disk = lock(&self.disk);
        if disk.cookies.is_empty() {
            return Ok(CookieChoice::Unmanaged);
        }
        let eligible = |cookie: &DiskCookie| {
            cookie.status != "invalid" && !skip.iter().any(|id| id == &cookie.id)
        };
        let index = disk
            .cookies
            .iter()
            .position(|cookie| cookie.active && eligible(cookie))
            .or_else(|| disk.cookies.iter().position(eligible));
        let Some(index) = index else {
            return Err("可用的 Cookie 都已失效".to_string());
        };
        let changed = disk
            .cookies
            .iter()
            .enumerate()
            .any(|(item, cookie)| cookie.active != (item == index));
        for (item, cookie) in disk.cookies.iter_mut().enumerate() {
            cookie.active = item == index;
        }
        let choice = CookieChoice::Use {
            id: disk.cookies[index].id.clone(),
            label: disk.cookies[index].label.clone(),
            raw: disk.cookies[index].raw.clone(),
        };
        if changed {
            self.persist(&disk)?;
        }
        Ok(choice)
    }

    pub fn note_cookie(&self, id: &str, status: &str, error: &str) {
        let mut disk = lock(&self.disk);
        let Some(cookie) = disk.cookies.iter_mut().find(|cookie| cookie.id == id) else {
            return;
        };
        cookie.status = status.to_string();
        cookie.error = error.to_string();
        let _ = self.persist(&disk);
    }

    fn insert_session(&self, role: Role) -> String {
        let token = random_token();
        let expires_at = now_ms() + ADMIN_SESSION_MS;
        lock(&self.sessions).insert(
            token.clone(),
            Session {
                role,
                expires_at,
            },
        );
        self.save_session(&token, expires_at);
        token
    }

    fn persist(&self, disk: &Disk) -> Result<(), String> {
        if let Some(meta) = &self.meta {
            meta.save_disk(&stored_disk(disk))?;
        }
        save_disk(&self.path, disk)
    }

    fn save_session(&self, token: &str, expires_at: i64) {
        if let Some(meta) = &self.meta {
            meta.save_session(token, expires_at);
        }
    }

    fn delete_session(&self, token: &str) {
        if let Some(meta) = &self.meta {
            meta.delete_session(token);
        }
    }

    pub fn save_download(&self, record: &crate::meta::DownloadRecord) {
        if let Some(meta) = &self.meta {
            if let Err(err) = meta.save_download(record) {
                tracing::error!("{err}");
            }
        }
    }

    pub fn load_downloads(&self) -> Vec<crate::meta::DownloadRecord> {
        let Some(meta) = &self.meta else {
            return Vec::new();
        };
        match meta.load_downloads() {
            Ok(rows) => rows,
            Err(err) => {
                tracing::error!("{err}");
                Vec::new()
            }
        }
    }

    pub fn log_op(&self, actor: &str, action: &str, detail: &str, ok: bool) {
        if let Some(meta) = &self.meta {
            meta.log_op(actor, action, detail, ok);
        }
    }

    pub fn recent_ops(&self, limit: u32) -> Vec<crate::meta::OpRow> {
        let Some(meta) = &self.meta else {
            return Vec::new();
        };
        match meta.recent_ops(limit) {
            Ok(rows) => rows,
            Err(err) => {
                tracing::error!("{err}");
                Vec::new()
            }
        }
    }
}

#[derive(Default)]
pub struct AppConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub data_dir: Option<String>,
    pub cookie_file: Option<String>,
    pub database: Option<crate::meta::DbConfig>,
}

#[derive(Deserialize)]
struct FileConfig {
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    data_dir: Option<String>,
    #[serde(default)]
    cookie_file: Option<String>,
    #[serde(default)]
    database: Option<crate::meta::DbConfig>,
}

pub fn read_app_config() -> Result<AppConfig, String> {
    let config_path = PathBuf::from("config.json");
    if !config_path.exists() {
        return Ok(AppConfig::default());
    }
    let text = std::fs::read_to_string(&config_path).map_err(|_| "config.json 读不了".to_string())?;
    let file: FileConfig = serde_json::from_str(&text).map_err(|_| "config.json 格式不对".to_string())?;
    Ok(AppConfig {
        host: file.host.filter(|value| !value.trim().is_empty()),
        port: file.port,
        data_dir: file.data_dir.filter(|value| !value.trim().is_empty()),
        cookie_file: file.cookie_file.filter(|value| !value.trim().is_empty()),
        database: file.database,
    })
}

pub fn open_with(path: PathBuf, database: Option<crate::meta::DbConfig>) -> Result<Arc<Portal>, String> {
    let Some(config) = database else {
        tracing::info!("没有数据库配置，账号数据仍使用 {}", path.display());
        return Ok(Arc::new(Portal::open(path)));
    };
    let meta = crate::meta::Meta::connect(&config)?;
    let portal = Portal::open(path);
    if meta.is_empty()? {
        let disk = lock(&portal.disk);
        let has_local = !disk.admin_hash.is_empty() || !disk.users.is_empty() || !disk.cookies.is_empty();
        if has_local {
            meta.save_disk(&stored_disk(&disk))?;
            tracing::info!(
                "已把本机配置导入 pan_meta，账号 {} 个，Cookie {} 个",
                disk.users.len(),
                disk.cookies.len()
            );
        }
        drop(disk);
    } else {
        let stored = meta.load_disk()?;
        {
            let mut disk = lock(&portal.disk);
            *disk = disk_from_stored(stored);
        }
        let loaded = lock(&portal.disk);
        tracing::info!(
            "已从 pan_meta 读取账号 {} 个，Cookie {} 个",
            loaded.users.len(),
            loaded.cookies.len()
        );
        drop(loaded);
        if let Ok(text) = serde_json::to_string_pretty(&*lock(&portal.disk)) {
            let _ = std::fs::write(&portal.path, text);
        }
        for (token, expires_at) in meta.load_sessions()? {
            lock(&portal.sessions).insert(
                token,
                Session {
                    role: Role::Admin,
                    expires_at,
                },
            );
        }
    }
    let portal = {
        let path = portal.path.clone();
        let disk = lock(&portal.disk).clone();
        let sessions = lock(&portal.sessions).clone();
        Arc::new(Portal {
            path,
            disk: Mutex::new(disk),
            sessions: Mutex::new(sessions),
            meta: Some(meta),
        })
    };
    Ok(portal)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

fn empty_disk() -> Disk {
    Disk {
        admin_salt: String::new(),
        admin_hash: String::new(),
        settings: DiskSettings {
            cloud_dir: "/来自分享".to_string(),
            download_dir: String::new(),
            jobs: 3,
        },
        users: Vec::new(),
        cookies: Vec::new(),
    }
}

fn stored_disk(disk: &Disk) -> crate::meta::StoredDisk {
    crate::meta::StoredDisk {
        admin_salt: disk.admin_salt.clone(),
        admin_hash: disk.admin_hash.clone(),
        cloud_dir: disk.settings.cloud_dir.clone(),
        download_dir: disk.settings.download_dir.clone(),
        jobs: disk.settings.jobs,
        users: disk
            .users
            .iter()
            .map(|user| crate::meta::StoredUser {
                name: user.name.clone(),
                salt: user.salt.clone(),
                hash: user.hash.clone(),
                expires_at: user.expires_at,
                download_dir: user.download_dir.clone(),
                cookie_id: user.cookie_id.clone(),
                download_limit: user.download_limit,
                downloads_used: user.downloads_used,
                created_at: user.created_at,
            })
            .collect(),
        cookies: disk
            .cookies
            .iter()
            .map(|cookie| crate::meta::StoredCookie {
                id: cookie.id.clone(),
                label: cookie.label.clone(),
                raw: cookie.raw.clone(),
                status: cookie.status.clone(),
                error: cookie.error.clone(),
                active: cookie.active,
            })
            .collect(),
    }
}

fn disk_from_stored(stored: crate::meta::StoredDisk) -> Disk {
    Disk {
        admin_salt: stored.admin_salt,
        admin_hash: stored.admin_hash,
        settings: DiskSettings {
            cloud_dir: stored.cloud_dir,
            download_dir: stored.download_dir,
            jobs: stored.jobs.max(1),
        },
        users: stored
            .users
            .into_iter()
            .map(|user| DiskUser {
                name: user.name,
                salt: user.salt,
                hash: user.hash,
                expires_at: user.expires_at,
                download_dir: user.download_dir,
                cookie_id: user.cookie_id,
                download_limit: user.download_limit,
                downloads_used: user.downloads_used,
                created_at: user.created_at,
            })
            .collect(),
        cookies: stored
            .cookies
            .into_iter()
            .map(|cookie| DiskCookie {
                id: cookie.id,
                label: cookie.label,
                raw: cookie.raw,
                status: cookie.status,
                error: cookie.error,
                active: cookie.active,
            })
            .collect(),
    }
}

fn read_disk(path: &Path) -> Option<Disk> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn save_disk(path: &Path, disk: &Disk) -> Result<(), String> {
    let text = serde_json::to_string_pretty(disk).map_err(|_| "保存配置失败".to_string())?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, text).map_err(|_| "保存配置失败".to_string())?;
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    std::fs::rename(&tmp, path).map_err(|_| "保存配置失败".to_string())?;
    Ok(())
}

fn cookie_label(label: &str) -> Result<String, String> {
    let label = label.trim();
    if label.is_empty() || label.chars().count() > 32 {
        return Err("Cookie 名称请用 1 到 32 个字符".to_string());
    }
    Ok(label.to_string())
}

fn check_cookie_raw(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.len() > 16 * 1024 {
        return Err("Cookie 太长".to_string());
    }
    let pairs = crate::cookie::parse_cookie(raw).map_err(|err| err.to_string())?;
    if pairs.is_empty() {
        return Err("Cookie 是空的".to_string());
    }
    Ok(raw.to_string())
}

fn default_download_limit() -> u32 {
    2
}

fn normalize_limit(limit: u32) -> u32 {
    if limit == 0 { 2 } else { limit.clamp(1, 8) }
}

fn effective_limit(limit: u32) -> u32 {
    normalize_limit(limit)
}

fn check_bind(disk: &Disk, cookie_id: &str, except: Option<&str>) -> Result<(), String> {
    let Some(cookie) = disk.cookies.iter().find(|cookie| cookie.id == cookie_id) else {
        return Err("没有这个 Cookie".to_string());
    };
    if cookie.status == "invalid" {
        return Err("这个 Cookie 已失效，不能绑定".to_string());
    }
    let bound = disk
        .users
        .iter()
        .filter(|user| user.cookie_id == cookie_id && except != Some(user.name.as_str()))
        .count();
    if bound >= 8 {
        return Err("这个 Cookie 已经绑定了 8 个账号".to_string());
    }
    Ok(())
}

fn cookie_view(cookie: &DiskCookie, bound: usize) -> CookieView {
    let names = crate::cookie::parse_cookie(&cookie.raw)
        .ok()
        .map(|pairs| {
            let shown: Vec<&str> = pairs.iter().take(3).map(|(key, _)| key.as_str()).collect();
            if pairs.is_empty() {
                "无法解析".to_string()
            } else {
                format!("{} 项：{}", pairs.len(), shown.join("、"))
            }
        })
        .unwrap_or_else(|| "无法解析".to_string());
    let status = if cookie.status.is_empty() {
        "unknown".to_string()
    } else {
        cookie.status.clone()
    };
    CookieView {
        id: cookie.id.clone(),
        label: cookie.label.clone(),
        preview: names,
        value: cookie.raw.clone(),
        status,
        error: cookie.error.clone(),
        active: cookie.active,
        bound,
    }
}

fn check_password(password: &str) -> Result<(), String> {
    if password.chars().count() < 4 {
        Err("密码至少 4 位".to_string())
    } else {
        Ok(())
    }
}

fn sign_user(salt: &str, name: &str, hash: &str) -> String {
    hash_password(salt, &format!("session:{name}:{hash}"))
}

fn hash_password(salt: &str, password: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(salt.as_bytes());
    hasher.update(b":");
    hasher.update(password.as_bytes());
    hex::encode(hasher.finalize())
}

fn random_token() -> String {
    let tick = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let addr = &tick as *const u128 as u64;
    let mut state = tick ^ u128::from(addr.rotate_left(17)) ^ u128::from(std::process::id());
    let mut bytes = [0u8; 32];
    for byte in &mut bytes {
        state = state
            .wrapping_mul(0x6C62_272E_07BB_0142)
            .wrapping_add(0xA076_1D64_78BD_642F);
        *byte = (state >> 57) as u8;
    }
    hex::encode(bytes)
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

pub fn parse_expiry(input: &str) -> Result<i64, String> {
    let input = input.trim().replace('T', " ");
    let (date, time) = input.split_once(' ').unwrap_or((input.as_str(), ""));
    let mut parts = date.split('-');
    let year: i32 = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| "有效期格式应为 2026-11-01 18:30:00".to_string())?;
    let month: u32 = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| "有效期格式应为 2026-11-01 18:30:00".to_string())?;
    let day: u32 = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| "有效期格式应为 2026-11-01 18:30:00".to_string())?;
    if parts.next().is_some() {
        return Err("有效期格式应为 2026-11-01 18:30:00".to_string());
    }
    let (hour, minute, second) = if time.is_empty() {
        (23, 59, 59)
    } else {
        parse_hms(time)?
    };
    let naive = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .ok_or_else(|| "有效期不是合法时间".to_string())?;
    let china = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
    let local = china
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| "有效期不是合法时间".to_string())?;
    Ok(local.timestamp_millis())
}

fn parse_hms(time: &str) -> Result<(u32, u32, u32), String> {
    let mut parts = time.split(':');
    let hour: u32 = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| "有效期格式应为 2026-11-01 18:30:00".to_string())?;
    let minute: u32 = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(|| "有效期格式应为 2026-11-01 18:30:00".to_string())?;
    let second: u32 = parts.next().and_then(|part| part.parse().ok()).unwrap_or(0);
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return Err("有效期不是合法时间".to_string());
    }
    Ok((hour, minute, second))
}

pub fn format_day(millis: i64) -> String {
    format_expiry(millis)
        .split_once(' ')
        .map(|(day, _)| day.to_string())
        .unwrap_or_else(|| "-".to_string())
}

pub fn format_expiry(millis: i64) -> String {
    let china = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
    chrono::DateTime::from_timestamp_millis(millis)
        .map(|time| {
            time.with_timezone(&china)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "-".to_string())
}

pub fn cookie_value(header: Option<&str>, name: &str) -> Option<String> {
    let header = header?;
    for part in header.split(';') {
        let Some((key, value)) = part.trim().split_once('=') else {
            continue;
        };
        if key == name {
            return Some(value.trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_is_end_of_china_day() {
        let millis = parse_expiry("2026-11-01").unwrap();
        assert_eq!(format_day(millis), "2026-11-01");
        assert_eq!(format_expiry(millis), "2026-11-01 23:59:59");
        let timed = parse_expiry("2026-11-01T08:30:15").unwrap();
        assert_eq!(format_expiry(timed), "2026-11-01 08:30:15");
        assert_eq!(parse_expiry("2026-11-01 08:30").unwrap(), parse_expiry("2026-11-01T08:30:00").unwrap());
        assert!(parse_expiry("2026/11/01").is_err());
    }

    #[test]
    fn user_login_respects_password_and_expiry() {
        let path = std::env::temp_dir().join(format!("quark-portal-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let portal = Portal::open(path.clone());
        portal.setup_admin("admin-pass").unwrap();
        assert!(portal.login_admin("nope").is_err());
        let token = portal.login_admin("admin-pass").unwrap();
        assert!(portal
            .change_admin_password("wrong", "admin-new", Some(&token))
            .unwrap_err()
            .contains("旧密码"));
        portal
            .change_admin_password("admin-pass", "admin-new", Some(&token))
            .unwrap();
        assert!(portal.touch_admin(&token).is_some());
        assert!(portal.login_admin("admin-pass").is_err());
        let _ = portal.login_admin("admin-new").unwrap();
        let fresh = portal.touch_admin(&token).unwrap() - now_ms();
        assert!(fresh > 59 * 60 * 1000 && fresh <= 60 * 60 * 1000 + 5_000);
        {
            let mut sessions = lock(&portal.sessions);
            sessions.get_mut(&token).unwrap().expires_at = now_ms() + 50 * 60 * 1000;
        }
        let steady = portal.touch_admin(&token).unwrap();
        assert!(steady - now_ms() < 51 * 60 * 1000);
        {
            let mut sessions = lock(&portal.sessions);
            sessions.get_mut(&token).unwrap().expires_at = now_ms() + 20 * 60 * 1000;
        }
        let renewed = portal.touch_admin(&token).unwrap() - now_ms();
        assert!(renewed > 59 * 60 * 1000 && renewed <= 60 * 60 * 1000 + 5_000);
        {
            let mut sessions = lock(&portal.sessions);
            sessions.get_mut(&token).unwrap().expires_at = now_ms() - 1_000;
        }
        assert!(portal.touch_admin(&token).is_none());
        let generated = portal.fresh_user_name();
        assert!(generated.starts_with('u') && generated.len() >= 7);
        portal.add_user(&generated, "123456", "2099-01-01", "", 2).unwrap();
        assert!(portal.login_user(&generated, "123456").is_ok());
        portal.add_user("alice", "secret1", "2099-01-01", "", 2).unwrap();
        assert!(portal.login_user("alice", "bad").is_err());
        let (token, day) = portal.login_user("alice", "secret1").unwrap();
        assert_eq!(day, "2099-01-01 23:59:59");
        let folder = std::env::temp_dir();
        let saved = portal
            .set_user_download_dir("alice", folder.to_str().unwrap())
            .unwrap();
        assert_eq!(portal.user_download_dir("alice"), saved);
        assert!(portal.set_user_download_dir("alice", "relative").is_err());
        portal.update_user_expiry("alice", "2099-06-01 12:00:00").unwrap();
        assert_eq!(portal.user_expiry_label("alice").as_deref(), Some("2099-06-01 12:00:00"));
        assert!(portal.role(&token).is_some());
        assert!(matches!(portal.role(&token), Some(Role::User(name)) if name == "alice"));
        portal.delete_user("alice").unwrap();
        assert!(portal.role(&token).is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cookie_pool_switches_away_from_invalid() {
        let path = std::env::temp_dir().join(format!("quark-portal-cookie-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let portal = Portal::open(path.clone());
        portal.add_cookie("甲", "k=one").unwrap();
        portal.add_cookie("乙", "k=two").unwrap();
        let first = portal.select_cookie(&[]).unwrap();
        let CookieChoice::Use { id, label, raw } = first else {
            panic!("expected a selected cookie");
        };
        assert_eq!(label, "甲");
        assert_eq!(raw, "k=one");
        portal.add_cookie("丙", "k=three").unwrap();
        let still = portal.select_cookie(&[]).unwrap();
        let CookieChoice::Use { label, .. } = still else {
            panic!("expected the pinned cookie");
        };
        assert_eq!(label, "甲");
        portal.note_cookie(&id, "invalid", "登录已失效");
        let next = portal.select_cookie(&[]).unwrap();
        let CookieChoice::Use { label, raw, .. } = next else {
            panic!("expected the next cookie");
        };
        assert_eq!(label, "乙");
        assert_eq!(raw, "k=two");
        let stuck = portal.select_cookie(&[]).unwrap();
        let id_a = portal.cookies().into_iter().find(|cookie| cookie.label == "乙").unwrap().id;
        for index in 1..=8 {
            portal
                .add_user(&format!("u{index}"), "secret1", "2099-01-01", &id_a, 2)
                .unwrap();
        }
        assert!(portal
            .add_user("u9", "secret1", "2099-01-01", &id_a, 2)
            .unwrap_err()
            .contains("8"));
        assert_eq!(portal.user_parallel("u1"), 2);
        portal.update_user_binding("u1", &id_a, 8).unwrap();
        assert_eq!(portal.user_parallel("u1"), 8);
        portal.update_user_binding("u1", &id_a, 100).unwrap();
        assert_eq!(portal.user_parallel("u1"), 8);
        let CookieChoice::Use { label, .. } = stuck else {
            panic!("expected the new pin");
        };
        assert_eq!(label, "乙");
        let views = portal.cookies();
        assert!(views.iter().any(|cookie| cookie.label == "甲" && cookie.status == "invalid" && !cookie.active));
        assert!(views.iter().any(|cookie| cookie.label == "乙" && cookie.active));
        assert!(views.iter().all(|cookie| cookie.preview.contains("k") && !cookie.preview.contains("one")));
        let _ = std::fs::remove_file(&path);
    }
}
