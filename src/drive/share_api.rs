//! Quark share-save API, plus the folder and streaming helpers that command uses.
//!
//! Flow: share link -> stoken -> file list -> save task -> new fids in the user's drive.
//! Download URLs still come from the existing member download API.

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::QuarkDrive;
use super::model::{CreateFolderRequest, CreateFolderResponse};

const SHARE_BASE: &str = "https://drive-pc.quark.cn";
const TOKEN_PATH: &str = "/1/clouddrive/share/sharepage/token";
const DETAIL_PATH: &str = "/1/clouddrive/share/sharepage/detail";
const SAVE_PATH: &str = "/1/clouddrive/share/sharepage/save";
const TASK_PATH: &str = "/1/clouddrive/task";
const SAVE_BATCH: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownloadStatus {
    pub skipped: bool,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareEntry {
    pub fid: String,
    pub file_name: String,
    pub pdir_fid: String,
    pub size: u64,
    pub is_dir: bool,
    pub share_fid_token: String,
}

#[derive(Clone)]
pub struct ShareSnapshot {
    pub pwd_id: String,
    pub stoken: String,
    pub title: String,
    pub is_owner: bool,
    /// Parent fid inside the share that `entries` were listed from.
    pub pdir_fid: String,
    pub entries: Vec<ShareEntry>,
}

impl std::fmt::Debug for ShareSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareSnapshot")
            .field("pwd_id", &self.pwd_id)
            .field("title", &self.title)
            .field("is_owner", &self.is_owner)
            .field("pdir_fid", &self.pdir_fid)
            .field("entries", &self.entries.len())
            .finish()
    }
}

fn none<T>() -> Option<T> {
    None
}

#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
struct ApiBody<T> {
    status: i64,
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default = "none")]
    data: Option<T>,
    #[serde(default)]
    metadata: PageMeta,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PageMeta {
    #[serde(rename = "_total", default)]
    total: u32,
    #[serde(default)]
    tq_gap: u64,
}

#[derive(Debug, Serialize)]
struct ShareTokenRequest<'a> {
    pwd_id: &'a str,
    passcode: &'a str,
}

#[derive(Debug, Deserialize)]
struct ShareTokenData {
    stoken: String,
    #[serde(default)]
    title: String,
}

#[derive(Debug, Deserialize, Default)]
struct ShareDetailData {
    #[serde(default)]
    list: Vec<ShareFile>,
    #[serde(default, deserialize_with = "de_i64_or_bool")]
    is_owner: i64,
    #[serde(default)]
    share: Option<ShareTitle>,
}

#[derive(Debug, Deserialize, Default)]
struct ShareTitle {
    #[serde(default)]
    title: String,
}

#[derive(Debug, Deserialize)]
struct ShareFile {
    fid: String,
    #[serde(
        default,
        deserialize_with = "crate::drive::model::deserialize_file_name"
    )]
    file_name: String,
    #[serde(default)]
    pdir_fid: String,
    #[serde(default, deserialize_with = "de_u64_flex")]
    size: u64,
    #[serde(default, deserialize_with = "de_bool_flex")]
    dir: bool,
    #[serde(default, deserialize_with = "de_bool_flex")]
    file: bool,
    #[serde(default)]
    share_fid_token: String,
    #[serde(default, deserialize_with = "de_u64_flex")]
    include_items: u64,
}

#[derive(Debug, Serialize)]
struct SaveShareRequest<'a> {
    fid_list: &'a [String],
    fid_token_list: &'a [String],
    to_pdir_fid: &'a str,
    pwd_id: &'a str,
    stoken: &'a str,
    pdir_fid: &'a str,
    scene: &'a str,
}

#[derive(Debug, Deserialize, Default)]
struct SaveTaskData {
    #[serde(default)]
    task_id: String,
}

#[derive(Debug, Deserialize, Default)]
struct TaskData {
    #[serde(default, alias = "task_status")]
    status: i32,
    #[serde(default)]
    task_title: String,
    #[serde(default)]
    share_id: String,
    #[serde(default)]
    save_as: Option<TaskSaveAs>,
}

#[derive(Debug, Serialize)]
struct CreateShareRequest<'a> {
    fid_list: &'a [String],
    title: &'a str,
    url_type: i32,
    expired_type: i32,
}

#[derive(Debug, Deserialize, Default)]
struct CreateShareData {
    #[serde(default)]
    task_id: String,
    #[serde(default)]
    share_id: String,
    #[serde(default)]
    task_resp: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
struct SharePasswordData {
    #[serde(default)]
    share_url: String,
    #[serde(default)]
    passcode: String,
}

#[derive(Debug, Deserialize, Default)]
struct TaskSaveAs {
    #[serde(default)]
    save_as_top_fids: Vec<String>,
    #[serde(default)]
    to_pdir_name: String,
}

impl QuarkDrive {
    pub async fn open_share(&self, pwd_id: &str, passcode: &str) -> Result<ShareSnapshot> {
        let token = self.share_token(pwd_id, passcode).await?;
        let listed = self.list_share_all(pwd_id, &token.stoken, "0").await?;
        if listed.entries.is_empty() {
            bail!("分享是空的");
        }
        let title = if !listed.title.is_empty() {
            listed.title
        } else {
            token.title
        };
        tracing::info!(
            pwd_id,
            title = %title,
            count = listed.entries.len(),
            is_owner = listed.is_owner,
            "opened share"
        );
        Ok(ShareSnapshot {
            pwd_id: pwd_id.to_string(),
            stoken: token.stoken,
            title,
            is_owner: listed.is_owner,
            pdir_fid: "0".to_string(),
            entries: listed.entries,
        })
    }

    pub async fn save_share_entries(
        &self,
        pwd_id: &str,
        stoken: &str,
        share_pdir_fid: &str,
        entries: &[ShareEntry],
        to_pdir_fid: &str,
    ) -> Result<Vec<String>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if entries.iter().all(|entry| entry.share_fid_token.is_empty()) {
            bail!("分享文件缺少转存令牌，链接可能已失效");
        }

        let mut saved = Vec::new();
        for (batch_no, chunk) in entries.chunks(SAVE_BATCH).enumerate() {
            let fid_list: Vec<String> = chunk.iter().map(|entry| entry.fid.clone()).collect();
            let token_list: Vec<String> = chunk
                .iter()
                .map(|entry| entry.share_fid_token.clone())
                .collect();
            let req = SaveShareRequest {
                fid_list: &fid_list,
                fid_token_list: &token_list,
                to_pdir_fid,
                pwd_id,
                stoken,
                pdir_fid: share_pdir_fid,
                scene: "link",
            };
            let url = build_api_url(SHARE_BASE, SAVE_PATH, &[])?;
            let body: ApiBody<SaveTaskData> = self.share_post(&url, &req).await?;
            if body.status != 200 || body.code != 0 {
                bail!(
                    "{}",
                    api_error("提交转存", body.status, body.code, &body.message)
                );
            }
            let task_id = body
                .data
                .and_then(|data| {
                    if data.task_id.is_empty() {
                        None
                    } else {
                        Some(data.task_id)
                    }
                })
                .ok_or_else(|| anyhow!("提交转存成功但没有任务 ID"))?;
            tracing::info!(
                task_id,
                batch = batch_no + 1,
                count = chunk.len(),
                "已提交转存"
            );
            let fids = self.wait_save_task(&task_id).await?;
            if fids.len() != chunk.len() {
                tracing::warn!(
                    expected = chunk.len(),
                    got = fids.len(),
                    "转存返回的文件数和提交的不一致"
                );
            }
            saved.extend(fids);
        }
        Ok(saved)
    }

    pub async fn list_dir_all(&self, pdir_fid: &str) -> Result<Vec<super::QuarkFile>> {
        let mut all = Vec::new();
        let mut seen = HashSet::new();
        let mut page = 1u32;
        loop {
            if page > 200 {
                bail!("目录条目过多，停止列举");
            }
            let (files, total) = self.get_files_by_pdir_fid(pdir_fid, page, 100).await?;
            let Some(files) = files else {
                break;
            };
            let mut added = 0usize;
            for file in files.list {
                if seen.insert(file.fid.clone()) {
                    all.push(file);
                    added += 1;
                }
            }
            if added == 0 || (total > 0 && all.len() as u32 >= total) {
                break;
            }
            page += 1;
        }
        Ok(all)
    }

    pub async fn ensure_dir_path(&self, path: &str) -> Result<(String, String)> {
        let segments = cloud_segments(path)?;
        let mut fid = "0".to_string();
        let mut built = String::new();
        for seg in segments {
            let children = self.list_dir_all(&fid).await?;
            if let Some(existing) = children.into_iter().find(|file| file.file_name == seg) {
                if !existing.dir {
                    bail!("云盘里「{seg}」已经存在，而且不是文件夹");
                }
                fid = existing.fid;
            } else {
                fid = self.create_folder_fid(&fid, &seg).await?;
            }
            built.push('/');
            built.push_str(&seg);
        }
        if built.is_empty() {
            built = "/".to_string();
        }
        Ok((fid, built))
    }

    pub async fn lookup_dir_path(&self, path: &str) -> Result<Option<(String, String)>> {
        let segments = cloud_segments(path)?;
        let mut fid = "0".to_string();
        let mut built = String::new();
        for seg in segments {
            let children = self.list_dir_all(&fid).await?;
            let Some(existing) = children.into_iter().find(|file| file.file_name == seg) else {
                return Ok(None);
            };
            if !existing.dir {
                bail!("云盘里「{seg}」已经存在，而且不是文件夹");
            }
            fid = existing.fid;
            built.push('/');
            built.push_str(&seg);
        }
        if built.is_empty() {
            built = "/".to_string();
        }
        Ok(Some((fid, built)))
    }

    /// Create a passwordless Quark share link for one file/folder.
    pub async fn create_public_share(&self, fid: &str, title: &str) -> Result<String> {
        let url = build_api_url(SHARE_BASE, "/1/clouddrive/share", &[])?;
        let fid_list = [fid.to_string()];
        let req = CreateShareRequest {
            fid_list: &fid_list,
            title,
            url_type: 1,
            expired_type: 1,
        };
        let body: ApiBody<CreateShareData> = self.share_post(&url, &req).await?;
        if body.status != 200 || body.code != 0 {
            bail!(
                "{}",
                api_error("创建分享", body.status, body.code, &body.message)
            );
        }
        let data = body.data.unwrap_or_default();
        let share_id = if !data.share_id.is_empty() {
            data.share_id
        } else if let Some(sync_id) = sync_share_id(data.task_resp.as_ref()) {
            sync_id
        } else if !data.task_id.is_empty() {
            self.wait_share_task(&data.task_id).await?
        } else {
            bail!("创建分享没有返回 share_id");
        };
        let pwd_url = build_api_url(SHARE_BASE, "/1/clouddrive/share/password", &[])?;
        let pwd_body: ApiBody<SharePasswordData> = self
            .share_post(&pwd_url, &serde_json::json!({ "share_id": share_id }))
            .await?;
        if pwd_body.status != 200 || pwd_body.code != 0 {
            bail!(
                "{}",
                api_error(
                    "获取分享链接",
                    pwd_body.status,
                    pwd_body.code,
                    &pwd_body.message
                )
            );
        }
        let info = pwd_body
            .data
            .ok_or_else(|| anyhow!("获取分享链接没有返回数据"))?;
        if info.share_url.is_empty() {
            bail!("获取分享链接为空");
        }
        if info.passcode.is_empty() {
            Ok(info.share_url)
        } else {
            Ok(format!("{}?pwd={}", info.share_url, info.passcode))
        }
    }

    pub async fn create_folder_fid(&self, parent_file_id: &str, name: &str) -> Result<String> {
        tracing::debug!(parent_file_id, name, "create folder");
        let req = CreateFolderRequest {
            pdir_fid: parent_file_id.to_string(),
            file_name: name.to_string(),
            dir_path: String::new(),
            dir_init_lock: false,
        };
        let res: CreateFolderResponse = self
            .post_request(
                format!(
                    "{}/1/clouddrive/file?pr=ucpro&fr=pc",
                    self.config.api_base_url
                ),
                &req,
                None,
            )
            .await?
            .context("创建文件夹没有返回内容")?;
        if res.status != 200 || res.data.fid.is_empty() {
            bail!("创建文件夹失败: {}", res.message);
        }
        Ok(res.data.fid)
    }

    pub async fn download_to_file(
        &self,
        url: &str,
        dest: &Path,
        expected_size: u64,
    ) -> Result<DownloadStatus> {
        self.download_to_file_with_progress(url, dest, expected_size, |_| {}, || false)
            .await
    }

    pub async fn download_to_file_with_progress<F, S>(
        &self,
        url: &str,
        dest: &Path,
        expected_size: u64,
        mut on_progress: F,
        mut should_stop: S,
    ) -> Result<DownloadStatus>
    where
        F: FnMut(u64) + Send,
        S: FnMut() -> bool + Send,
    {
        use tokio::io::AsyncWriteExt;

        if let Some(parent) = dest.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context("创建本地目录失败")?;
            }
        }

        let mut offset = 0u64;
        if expected_size > 0 && dest.exists() {
            let len = tokio::fs::metadata(dest).await?.len();
            if len == expected_size {
                return Ok(DownloadStatus {
                    skipped: true,
                    bytes: len,
                });
            }
            if len < expected_size {
                offset = len;
            }
        }

        let cookie = self.resolve_cookies().await;
        let mut use_range = offset > 0;
        let mut response = loop {
            let mut req = self.stream_client.get(url).header("Cookie", &cookie);
            if use_range {
                req = req.header(reqwest::header::RANGE, format!("bytes={offset}-"));
            }
            let response = req.send().await.context("连接下载地址失败")?;
            if use_range && response.status() == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                use_range = false;
                offset = 0;
                continue;
            }
            break response;
        };

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            let snippet: String = text.chars().take(180).collect();
            bail!("下载失败 HTTP {status}: {snippet}");
        }

        let append = use_range && status == reqwest::StatusCode::PARTIAL_CONTENT;
        if use_range && status == reqwest::StatusCode::OK {
            offset = 0;
        }

        let mut file = if append {
            tokio::fs::OpenOptions::new()
                .append(true)
                .open(dest)
                .await?
        } else {
            tokio::fs::File::create(dest).await?
        };

        let mut written = if append { offset } else { 0 };
        if written > 0 {
            on_progress(written);
        }
        while let Some(chunk) = response.chunk().await.context("读取下载数据失败")? {
            if should_stop() {
                bail!("已取消");
            }
            file.write_all(&chunk).await.context("写入本地文件失败")?;
            written += chunk.len() as u64;
            on_progress(written);
        }
        file.flush().await.context("保存本地文件失败")?;

        if expected_size > 0 && written != expected_size {
            bail!("下载不完整: 期望 {expected_size} 字节，实际写入 {written} 字节");
        }
        Ok(DownloadStatus {
            skipped: false,
            bytes: written,
        })
    }

    async fn share_token(&self, pwd_id: &str, passcode: &str) -> Result<ShareTokenData> {
        let url = build_api_url(SHARE_BASE, TOKEN_PATH, &[])?;
        let body: ApiBody<ShareTokenData> = self
            .share_post(&url, &ShareTokenRequest { pwd_id, passcode })
            .await?;
        if body.status != 200 || body.code != 0 {
            bail!(
                "{}",
                api_error("获取分享令牌", body.status, body.code, &body.message)
            );
        }
        let data = body
            .data
            .ok_or_else(|| anyhow!("获取分享令牌失败: {}", body.message))?;
        if data.stoken.is_empty() {
            bail!("获取分享令牌失败: 返回的 stoken 是空的");
        }
        Ok(data)
    }

    async fn list_share_all(
        &self,
        pwd_id: &str,
        stoken: &str,
        pdir_fid: &str,
    ) -> Result<ListedShare> {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        let mut title = String::new();
        let mut is_owner = false;
        let mut page = 1u32;
        loop {
            if page > 200 {
                bail!("分享根目录文件过多");
            }
            let body = self.list_share_page(pwd_id, stoken, pdir_fid, page).await?;
            if body.status != 200 || body.code != 0 {
                bail!(
                    "{}",
                    api_error("读取分享", body.status, body.code, &body.message)
                );
            }
            let total = body.metadata.total;
            let data = body.data.unwrap_or_default();
            if page == 1 {
                is_owner = data.is_owner == 1;
                title = data.share.unwrap_or_default().title;
            }
            let mut added = 0usize;
            for file in data.list {
                if seen.insert(file.fid.clone()) {
                    entries.push(to_entry(file));
                    added += 1;
                }
            }
            if added == 0 || (total > 0 && entries.len() as u32 >= total) {
                break;
            }
            page += 1;
        }
        Ok(ListedShare {
            entries,
            title,
            is_owner,
        })
    }

    async fn list_share_page(
        &self,
        pwd_id: &str,
        stoken: &str,
        pdir_fid: &str,
        page: u32,
    ) -> Result<ApiBody<ShareDetailData>> {
        let page_s = page.to_string();
        let url = build_api_url(
            SHARE_BASE,
            DETAIL_PATH,
            &[
                ("pwd_id", pwd_id),
                ("stoken", stoken),
                ("pdir_fid", pdir_fid),
                ("force", "0"),
                ("_page", &page_s),
                ("_size", "50"),
                ("_sort", "file_type:asc,updated_at:desc"),
            ],
        )?;
        self.share_get(&url).await
    }

    async fn wait_share_task(&self, task_id: &str) -> Result<String> {
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut attempt = 0u32;
        let mut next_gap = 400u64;
        loop {
            if Instant::now() >= deadline {
                bail!("创建分享超时: {task_id}");
            }
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(next_gap)).await;
            let retry_index = attempt.to_string();
            let url = build_api_url(
                SHARE_BASE,
                TASK_PATH,
                &[("task_id", task_id), ("retry_index", &retry_index)],
            )?;
            let body = self.share_get::<TaskData>(&url).await?;
            next_gap = body.metadata.tq_gap.clamp(300, 2000);
            if body.status != 200 || body.code != 0 {
                bail!(
                    "{}",
                    api_error("创建分享", body.status, body.code, &body.message)
                );
            }
            let data = body
                .data
                .ok_or_else(|| anyhow!("创建分享任务没有返回数据"))?;
            if data.status >= 3 {
                bail!(
                    "创建分享失败：{}",
                    if body.message.is_empty() {
                        data.task_title
                    } else {
                        body.message
                    }
                );
            }
            if data.status == 2 {
                if data.share_id.is_empty() {
                    bail!("创建分享完成但没有 share_id");
                }
                return Ok(data.share_id);
            }
        }
    }

    async fn wait_save_task(&self, task_id: &str) -> Result<Vec<String>> {
        let deadline = Instant::now() + Duration::from_secs(600);
        let mut attempt = 0u32;
        let mut empty_done = 0u32;
        let mut consecutive_errors = 0u32;
        let mut next_gap = 400u64;
        loop {
            if Instant::now() >= deadline {
                bail!("转存任务超时（10 分钟）: {task_id}");
            }
            attempt += 1;
            tokio::time::sleep(Duration::from_millis(next_gap)).await;
            let retry_index = attempt.to_string();
            let url = build_api_url(
                SHARE_BASE,
                TASK_PATH,
                &[("task_id", task_id), ("retry_index", &retry_index)],
            )?;
            let body = match self.share_get::<TaskData>(&url).await {
                Ok(body) => {
                    consecutive_errors = 0;
                    body
                }
                Err(err) => {
                    consecutive_errors += 1;
                    tracing::warn!(
                        error = %err,
                        consecutive_errors,
                        "查询转存任务失败"
                    );
                    if consecutive_errors >= 5 {
                        return Err(err);
                    }
                    next_gap = 1000;
                    continue;
                }
            };
            next_gap = body.metadata.tq_gap.clamp(300, 3000);
            if body.status != 200 || body.code != 0 {
                bail!(
                    "{}",
                    api_error("转存", body.status, body.code, &body.message)
                );
            }
            let data = body
                .data
                .ok_or_else(|| anyhow!("转存任务没有返回数据: {}", body.message))?;
            if attempt == 1 || attempt % 5 == 0 {
                tracing::info!(
                    attempt,
                    task_id,
                    status = data.status,
                    title = %data.task_title,
                    "转存任务进行中"
                );
            }
            if data.status >= 3 {
                bail!(
                    "转存失败，任务状态 {}：{}",
                    data.status,
                    if body.message.is_empty() {
                        data.task_title
                    } else {
                        body.message
                    }
                );
            }
            if data.status == 2 {
                let save_as = data.save_as.unwrap_or_default();
                if !save_as.to_pdir_name.is_empty() {
                    tracing::info!(folder = %save_as.to_pdir_name, "转存目标目录");
                }
                if !save_as.save_as_top_fids.is_empty() {
                    tracing::info!(
                        task_id,
                        count = save_as.save_as_top_fids.len(),
                        "转存任务完成"
                    );
                    return Ok(save_as.save_as_top_fids);
                }
                empty_done += 1;
                if empty_done >= 5 {
                    bail!("转存任务已结束，但没有返回新文件 ID: {task_id}");
                }
            }
        }
    }

    async fn share_get<T: DeserializeOwned>(&self, url: &str) -> Result<ApiBody<T>> {
        let cookie = self.resolve_cookies().await;
        let response = self
            .stream_client
            .get(url)
            .header("Cookie", cookie)
            .header("Accept", "application/json, text/plain, */*")
            .header("Accept-Language", "zh-CN,zh;q=0.9")
            .send()
            .await
            .context("请求夸克分享接口失败")?;
        self.read_api(response).await
    }

    async fn share_post<T, B>(&self, url: &str, body: &B) -> Result<ApiBody<T>>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let cookie = self.resolve_cookies().await;
        let response = self
            .stream_client
            .post(url)
            .header("Cookie", cookie)
            .header("Accept", "application/json, text/plain, */*")
            .header("Accept-Language", "zh-CN,zh;q=0.9")
            .json(body)
            .send()
            .await
            .context("请求夸克分享接口失败")?;
        self.read_api(response).await
    }

    async fn read_api<T: DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<ApiBody<T>> {
        self.update_cookie_from_response(&response).await;
        let status = response.status();
        let text = response.text().await.context("读取夸克响应失败")?;
        if text.is_empty() {
            bail!("夸克接口返回空响应 (HTTP {status})");
        }
        serde_json::from_str::<ApiBody<T>>(&text).with_context(|| {
            let snippet: String = text.chars().take(300).collect();
            format!("解析夸克响应失败 (HTTP {status}): {snippet}")
        })
    }
}

struct ListedShare {
    entries: Vec<ShareEntry>,
    title: String,
    is_owner: bool,
}

fn to_entry(file: ShareFile) -> ShareEntry {
    let is_dir = file.dir || (!file.file && file.size == 0 && file.include_items > 0);
    ShareEntry {
        fid: file.fid,
        file_name: if file.file_name.is_empty() {
            "unnamed".to_string()
        } else {
            file.file_name
        },
        pdir_fid: file.pdir_fid,
        size: file.size,
        is_dir,
        share_fid_token: file.share_fid_token,
    }
}

fn sync_share_id(task_resp: Option<&Value>) -> Option<String> {
    let value = task_resp?;
    value
        .pointer("/data/share_id")
        .and_then(|item| item.as_str())
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .or_else(|| {
            value
                .get("share_id")
                .and_then(|item| item.as_str())
                .filter(|id| !id.is_empty())
                .map(str::to_string)
        })
}

fn cloud_segments(path: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for seg in path.split(['/', '\\']) {
        let seg = seg.trim();
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            bail!("云盘路径不能包含 ..");
        }
        out.push(seg.to_string());
    }
    Ok(out)
}

fn api_error(action: &str, status: i64, code: i64, message: &str) -> String {
    let hint = match code {
        32003 => "网盘容量不足",
        41013 => "网盘目录不存在",
        _ => "",
    };
    if !hint.is_empty() {
        return format!("{action}失败: {hint}");
    }
    let message = message.trim();
    if message.is_empty() {
        format!("{action}失败 (status={status}, code={code})")
    } else {
        format!("{action}失败: {message} (status={status}, code={code})")
    }
}

fn build_api_url(base: &str, path: &str, extra: &[(&str, &str)]) -> Result<String> {
    let mut url = reqwest::Url::parse(&format!("{base}{path}"))?;
    {
        let mut query = url.query_pairs_mut();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let dt = (now % 5000 + 100).to_string();
        let ts = now.to_string();
        query.append_pair("pr", "ucpro");
        query.append_pair("fr", "pc");
        query.append_pair("uc_param_str", "");
        query.append_pair("__dt", &dt);
        query.append_pair("__t", &ts);
        for (key, value) in extra {
            query.append_pair(key, value);
        }
    }
    Ok(url.to_string())
}

fn de_i64_or_bool<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Bool(flag) => Ok(if flag { 1 } else { 0 }),
        Value::Number(number) => Ok(number.as_i64().unwrap_or(0)),
        Value::Null => Ok(0),
        _ => Ok(0),
    }
}

fn de_u64_flex<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Number(number) => Ok(number.as_u64().unwrap_or(0)),
        Value::String(text) => Ok(text.parse().unwrap_or(0)),
        Value::Null => Ok(0),
        _ => Ok(0),
    }
}

fn de_bool_flex<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Bool(flag) => Ok(flag),
        Value::Number(number) => Ok(number.as_i64().unwrap_or(0) != 0),
        Value::String(text) => Ok(text == "1" || text.eq_ignore_ascii_case("true")),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_stoken_in_query() {
        let url = build_api_url(
            "https://drive-pc.quark.cn",
            "/1/clouddrive/share/sharepage/detail",
            &[("stoken", "a+b/c=")],
        )
        .unwrap();
        assert!(url.contains("pr=ucpro"), "{url}");
        assert!(url.contains("stoken=a%2Bb%2Fc%3D"), "{url}");
    }

    #[test]
    fn cloud_path_segments() {
        assert_eq!(
            cloud_segments("/来自分享/电影").unwrap(),
            vec!["来自分享".to_string(), "电影".to_string()]
        );
        assert_eq!(
            cloud_segments(r"\来自分享\电影").unwrap(),
            vec!["来自分享".to_string(), "电影".to_string()]
        );
        assert!(cloud_segments("/").unwrap().is_empty());
        assert!(cloud_segments("/a/./b/").unwrap() == vec!["a".to_string(), "b".to_string()]);
        assert!(cloud_segments("/a/../b").is_err());
    }

    #[test]
    fn parses_share_detail_and_html_name() {
        let json = r#"{
            "status": 200,
            "code": 0,
            "message": "ok",
            "timestamp": 1,
            "data": {
                "is_owner": true,
                "share": {"title": "测试分享"},
                "list": [
                    {
                        "fid": "abc",
                        "file_name": "a &amp; b.mp4",
                        "pdir_fid": "0",
                        "size": "12",
                        "dir": false,
                        "file": true,
                        "share_fid_token": "tok"
                    },
                    {
                        "fid": "dir1",
                        "file_name": "目录",
                        "dir": 1,
                        "file": 0,
                        "size": 0,
                        "share_fid_token": "tok2",
                        "include_items": "3"
                    }
                ]
            },
            "metadata": {"_total": 2, "tq_gap": 500}
        }"#;
        let body: ApiBody<ShareDetailData> = serde_json::from_str(json).unwrap();
        assert_eq!(body.metadata.total, 2);
        let data = body.data.unwrap();
        assert_eq!(data.is_owner, 1);
        assert_eq!(data.share.unwrap().title, "测试分享");
        let entries: Vec<_> = data.list.into_iter().map(to_entry).collect();
        assert_eq!(entries[0].file_name, "a & b.mp4");
        assert_eq!(entries[0].size, 12);
        assert!(!entries[0].is_dir);
        assert!(entries[1].is_dir);
        assert_eq!(entries[1].share_fid_token, "tok2");
    }

    #[test]
    fn parses_finished_save_task() {
        let json = r#"{
            "status": 200,
            "code": 0,
            "message": "ok",
            "timestamp": 1,
            "data": {
                "task_id": "t1",
                "status": 2,
                "task_title": "分享-转存",
                "save_as": {
                    "save_as_top_fids": ["fidnew"],
                    "to_pdir_fid": "0",
                    "to_pdir_name": "来自分享"
                }
            },
            "metadata": {"tq_gap": 500}
        }"#;
        let body: ApiBody<TaskData> = serde_json::from_str(json).unwrap();
        let data = body.data.unwrap();
        assert_eq!(data.status, 2);
        assert_eq!(
            data.save_as.unwrap().save_as_top_fids,
            vec!["fidnew".to_string()]
        );
    }

    #[test]
    fn capacity_error_mentions_space() {
        let json = r#"{
            "status": 200,
            "code": 32003,
            "message": "capacity limit exceed",
            "timestamp": 1,
            "data": null,
            "metadata": {}
        }"#;
        let body: ApiBody<TaskData> = serde_json::from_str(json).unwrap();
        assert!(body.data.is_none());
        let text = api_error("转存", body.status, body.code, &body.message);
        assert!(text.contains("容量"), "{text}");
        assert!(!text.to_ascii_lowercase().contains("capacity"), "{text}");
    }
}
