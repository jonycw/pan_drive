//! Share link -> save into the signed-in Quark drive -> download with that account.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Args;
use tokio::sync::Semaphore;

use crate::drive::{DownloadStatus, QuarkDrive, QuarkFile, ShareEntry, ShareSnapshot};

/// 分享链接转存到自己的夸克网盘，再下载到本地目录。
#[derive(Args, Debug)]
#[command(after_help = r#"示例:
  quarkdrive-webdav fetch -u https://pan.quark.cn/s/xxxx -o D:\downloads
  quarkdrive-webdav fetch -u "https://pan.quark.cn/s/xxxx?pwd=1234" --to /电影 -o D:\movies

同一命令再跑一次时，网盘里同名文件会直接复用，本地已经下完整的文件会跳过。
转存会占用网盘容量，下载后不会自动删除云端文件。
下载速度取决于 quark.cookie 里这个账号的会员权益。
服务跑起来之后，浏览器打开 http://127.0.0.1:<端口>/ 用账号登录后粘贴链接。管理页是 /admin。"#)]
pub struct FetchArgs {
    /// 夸克分享链接，或分享 ID
    #[arg(short = 'u', long, env = "QUARK_SHARE_URL")]
    pub url: String,
    /// 提取码。链接里已有 pwd= 或「提取码」时可以不填
    #[arg(
        long,
        visible_alias = "passcode",
        env = "QUARK_SHARE_PWD",
        default_value = ""
    )]
    pub pwd: String,
    /// 转存到这个网盘目录，不存在会自动创建
    #[arg(long, default_value = "/来自分享")]
    pub to: String,
    /// 下载到这个本地目录
    #[arg(short = 'o', long, env = "QUARK_DOWNLOAD_DIR")]
    pub download: Option<PathBuf>,
    /// 同时下载的文件数，1 到 8
    #[arg(long, default_value_t = 3)]
    pub jobs: usize,
    /// 只转存，不下载
    #[arg(long)]
    pub cloud_only: bool,
    /// 目标目录已有同名文件时仍再转存一份
    #[arg(long)]
    pub force_save: bool,
    /// 只转存这些分享文件。空表示整份分享。
    #[arg(skip)]
    pub only_fids: Vec<String>,
}

struct Node {
    fid: String,
    name: String,
    is_dir: bool,
    size: u64,
}

enum Job {
    Dir(PathBuf),
    File {
        rel: PathBuf,
        fid: String,
        size: u64,
    },
}

struct FileJob {
    rel: PathBuf,
    fid: String,
    size: u64,
}

pub trait FetchReport: Send + Sync {
    fn log(&self, line: &str);
    fn phase(&self, phase: &str);
    fn cloud_path(&self, path: &str) {
        let _ = path;
    }
    fn file(&self, name: &str, written: u64, total: u64, state: &str);
    fn cancelled(&self) -> bool {
        false
    }
}

struct StdioReport;

impl FetchReport for StdioReport {
    fn log(&self, line: &str) {
        println!("{line}");
    }

    fn phase(&self, _phase: &str) {}

    fn file(&self, name: &str, written: u64, total: u64, state: &str) {
        let _ = written;
        match state {
            "start" => println!("↓ {name}  {}", fmt_size(total)),
            "skipped" => println!("· {name}  已存在，跳过"),
            _ => {}
        }
    }
}

fn stop_if_cancelled(report: &dyn FetchReport) -> Result<()> {
    if report.cancelled() {
        bail!("已取消");
    }
    Ok(())
}

pub async fn run(drive: &QuarkDrive, args: &FetchArgs) -> Result<()> {
    run_with_report(drive, args, Arc::new(StdioReport)).await
}

pub async fn run_with_report(
    drive: &QuarkDrive,
    args: &FetchArgs,
    report: Arc<dyn FetchReport>,
) -> Result<()> {
    let (pwd_id, passcode) = parse_share_input(&args.url, &args.pwd)?;
    let download_to = match (&args.download, args.cloud_only) {
        (_, true) => None,
        (Some(path), false) => Some(path.clone()),
        (None, false) => {
            bail!("请用 --download / -o 指定本地目录，或加上 --cloud-only 只转存");
        }
    };
    let parallel = if (1..=8).contains(&args.jobs) {
        args.jobs
    } else {
        let adjusted = args.jobs.clamp(1, 8);
        report.log(&format!("并行数已调整为 {adjusted}"));
        adjusted
    };

    if let Some(dir) = &download_to {
        report.log(&format!(
            "分享链接 → 转存到网盘「{}」→ 下载到 {}",
            args.to,
            dir.display()
        ));
        report.log("下载走 quark.cookie 里这个账号的会员权限，速度取决于这个账号。");
    } else {
        report.log(&format!("分享链接 → 转存到网盘「{}」（不下载）", args.to));
    }
    report.log("转存文件会留在网盘里并占用容量，下载完成后不会自动删除。");

    report.phase("读取分享");
    let mut snapshot: ShareSnapshot = drive.open_share(&pwd_id, &passcode).await?;
    if !args.only_fids.is_empty() {
        let allow: HashSet<&str> = args.only_fids.iter().map(String::as_str).collect();
        snapshot.entries.retain(|entry| allow.contains(entry.fid.as_str()));
    }
    if snapshot.entries.is_empty() {
        bail!("没有选择要下载的文件");
    }
    stop_if_cancelled(report.as_ref())?;
    let title = if snapshot.title.is_empty() {
        pwd_id.clone()
    } else {
        snapshot.title.clone()
    };
    report.log(&format!("分享「{title}」共 {} 项", snapshot.entries.len()));
    for entry in snapshot.entries.iter().take(30) {
        if entry.is_dir {
            report.log(&format!("  目录  {}", entry.file_name));
        } else {
            report.log(&format!(
                "  文件  {}  {}",
                entry.file_name,
                fmt_size(entry.size)
            ));
        }
    }
    if snapshot.entries.len() > 30 {
        report.log(&format!("  ... 还有 {} 项", snapshot.entries.len() - 30));
    }

    stop_if_cancelled(report.as_ref())?;
    report.phase("准备目录");
    let (dest_fid, dest_path) = drive.ensure_dir_path(&args.to).await?;
    report.cloud_path(&dest_path);
    report.log(&format!("网盘目录 {dest_path}"));

    let existing = drive.list_dir_all(&dest_fid).await?;
    let (reused, todo) = split_existing(&snapshot.entries, &existing, args.force_save);
    if !reused.is_empty() {
        report.log(&format!(
            "目标目录已有 {} 项同名文件，直接复用，不再转存",
            reused.len()
        ));
    }

    let mut nodes: Vec<Node> = reused.into_iter().map(node_from_quark).collect();
    if !todo.is_empty() {
        stop_if_cancelled(report.as_ref())?;
        report.phase("转存");
        report.log(&format!("开始转存 {} 项...", todo.len()));
        let saved_fids = match drive
            .save_share_entries(
                &snapshot.pwd_id,
                &snapshot.stoken,
                &snapshot.pdir_fid,
                &todo,
                &dest_fid,
            )
            .await
        {
            Ok(fids) => fids,
            Err(err) if snapshot.is_owner => {
                report.log(&format!(
                    "转存失败（这是当前账号自己的分享），改为直接下载原文件: {err}"
                ));
                nodes.extend(todo.iter().cloned().map(|entry| node_from_share(&entry)));
                Vec::new()
            }
            Err(err) => {
                return Err(err).context(
                    "转存中断。如果网盘里已经出现同名文件，再跑一次同样的命令会复用它们并继续下载",
                );
            }
        };
        if !saved_fids.is_empty() {
            let saved = resolve_saved(drive, &dest_fid, &saved_fids, report.as_ref()).await?;
            report.log(&format!("转存完成 {} 项", saved.len()));
            for file in saved.iter().take(30) {
                let suffix = if file.dir { "/" } else { "" };
                report.log(&format!("  已转存 {}{suffix}", file.file_name));
            }
            nodes.extend(saved.into_iter().map(node_from_quark));
        }
    } else if nodes.is_empty() {
        bail!("没有可转存或下载的文件");
    } else {
        report.log("无需再次转存");
    }

    let Some(root) = download_to else {
        report.log(&format!("已写入网盘 {dest_path}，共 {} 项", nodes.len()));
        for node in nodes.iter().take(30) {
            let suffix = if node.is_dir { "/" } else { "" };
            report.log(&format!("  {}{suffix}", node.name));
        }
        return Ok(());
    };

    stop_if_cancelled(report.as_ref())?;
    report.phase("下载");
    let jobs = dedup_jobs(collect_jobs(drive, nodes).await?);
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for job in jobs {
        match job {
            Job::Dir(rel) => dirs.push(rel),
            Job::File { rel, fid, size } => files.push(FileJob { rel, fid, size }),
        }
    }

    tokio::fs::create_dir_all(&root)
        .await
        .with_context(|| format!("无法创建本地目录 {}", root.display()))?;
    for rel in &dirs {
        tokio::fs::create_dir_all(root.join(rel))
            .await
            .with_context(|| format!("无法创建本地目录 {}", root.join(rel).display()))?;
    }

    if files.is_empty() {
        report.log(&format!(
            "没有文件需要下载（只有空目录），本地目录 {}",
            root.display()
        ));
        return Ok(());
    }

    let total: u64 = files.iter().map(|file| file.size).sum();
    report.log(&format!(
        "待下载 {} 个文件，合计 {}，并行 {parallel}",
        files.len(),
        fmt_size(total)
    ));

    let sem = Arc::new(Semaphore::new(parallel));
    let mut set = tokio::task::JoinSet::new();
    for file in files {
        let sem = sem.clone();
        let drive = drive.clone();
        let root = root.clone();
        let report = report.clone();
        if report.cancelled() {
            break;
        }
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("download semaphore");
            download_one(&drive, &root, file, report).await
        });
    }

    let started = Instant::now();
    let mut downloaded = 0u32;
    let mut skipped = 0u32;
    let mut failed = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(status)) => {
                if status.skipped {
                    skipped += 1;
                } else {
                    downloaded += 1;
                }
            }
            Ok(Err(err)) => {
                let message = format!("{err:#}");
                report.log(&format!("✗ {message}"));
                failed.push(message);
            }
            Err(err) => {
                let message = format!("任务异常: {err}");
                report.log(&format!("✗ {message}"));
                failed.push(message);
            }
        }
    }

    report.log(&format!(
        "完成: 新下载 {downloaded}，跳过 {skipped}，失败 {}，用时 {:.0} 秒，目录 {}",
        failed.len(),
        started.elapsed().as_secs_f64(),
        root.display()
    ));
    if !failed.is_empty() {
        bail!("{} 个文件下载失败", failed.len());
    }
    Ok(())
}

async fn download_one(
    drive: &QuarkDrive,
    root: &Path,
    file: FileJob,
    report: Arc<dyn FetchReport>,
) -> Result<DownloadStatus> {
    let dest = root.join(&file.rel);
    let display = file.rel.display().to_string();
    if file.size > 0 && dest.is_file() {
        if let Ok(meta) = tokio::fs::metadata(&dest).await {
            if meta.len() == file.size {
                report.file(&display, meta.len(), file.size, "skipped");
                return Ok(DownloadStatus {
                    skipped: true,
                    bytes: meta.len(),
                });
            }
        }
    }

    report.file(&display, 0, file.size, "start");
    let started = Instant::now();
    let url = match drive.get_download_url(&file.fid).await {
        Ok(url) => url,
        Err(err) => {
            report.file(&display, 0, file.size, "error");
            return Err(err).with_context(|| format!("获取下载地址失败: {display}"));
        }
    };
    let progress_report = report.clone();
    let stop_report = report.clone();
    let progress_name = display.clone();
    let total = file.size;
    let status = match drive
        .download_to_file_with_progress(
            &url,
            &dest,
            file.size,
            move |written| {
                progress_report.file(&progress_name, written, total, "progress");
            },
            move || stop_report.cancelled(),
        )
        .await
    {
        Ok(status) => status,
        Err(err) => {
            report.file(&display, 0, file.size, "error");
            return Err(err).with_context(|| format!("下载失败: {display}"));
        }
    };
    if status.skipped {
        report.file(&display, status.bytes, file.size, "skipped");
    } else {
        let secs = started.elapsed().as_secs_f64().max(0.001);
        let speed = status.bytes as f64 / secs;
        report.log(&format!(
            "✓ {display}  {}  {:.1} 秒  {}/s",
            fmt_size(status.bytes),
            secs,
            fmt_size(speed as u64)
        ));
        report.file(&display, status.bytes, file.size, "done");
    }
    Ok(status)
}

async fn resolve_saved(
    drive: &QuarkDrive,
    parent: &str,
    fids: &[String],
    report: &dyn FetchReport,
) -> Result<Vec<QuarkFile>> {
    use std::collections::HashMap;

    if fids.is_empty() {
        bail!("转存完成但没有返回新文件");
    }
    let mut missing = fids.to_vec();
    for attempt in 1..=15 {
        let children = drive.list_dir_all(parent).await?;
        let map: HashMap<String, QuarkFile> = children
            .into_iter()
            .map(|file| (file.fid.clone(), file))
            .collect();
        if fids.iter().all(|fid| map.contains_key(fid)) {
            return Ok(fids
                .iter()
                .filter_map(|fid| map.get(fid).cloned())
                .collect());
        }
        missing = fids
            .iter()
            .filter(|fid| !map.contains_key(fid.as_str()))
            .cloned()
            .collect();
        if attempt == 15 {
            break;
        }
        report.log(&format!(
            "等待转存文件出现在目录里（{attempt}/15），还缺 {} 个",
            missing.len()
        ));
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    bail!("转存已提交，但目录里还没有这些文件: {}", missing.join(", "))
}

async fn collect_jobs(drive: &QuarkDrive, nodes: Vec<Node>) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    let mut stack = Vec::new();
    for node in nodes.into_iter().rev() {
        stack.push((node, PathBuf::new()));
    }
    while let Some((node, parent)) = stack.pop() {
        let name = sanitize_component(&node.name);
        let rel = if parent.as_os_str().is_empty() {
            PathBuf::from(&name)
        } else {
            parent.join(&name)
        };
        if !is_safe_rel(&rel) {
            bail!("文件名无法保存到本地: {}", node.name);
        }
        if node.is_dir {
            jobs.push(Job::Dir(rel.clone()));
            let children = drive
                .list_dir_all(&node.fid)
                .await
                .with_context(|| format!("列举云盘目录失败: {}", node.name))?;
            for child in children.into_iter().rev() {
                stack.push((node_from_quark(child), rel.clone()));
            }
        } else {
            jobs.push(Job::File {
                rel,
                fid: node.fid,
                size: node.size,
            });
        }
    }
    Ok(jobs)
}

fn node_from_quark(file: QuarkFile) -> Node {
    Node {
        fid: file.fid,
        name: file.file_name,
        is_dir: file.dir,
        size: file.size,
    }
}

fn node_from_share(entry: &ShareEntry) -> Node {
    Node {
        fid: entry.fid.clone(),
        name: entry.file_name.clone(),
        is_dir: entry.is_dir,
        size: entry.size,
    }
}

fn split_existing(
    entries: &[ShareEntry],
    existing: &[QuarkFile],
    force_save: bool,
) -> (Vec<QuarkFile>, Vec<ShareEntry>) {
    if force_save {
        return (Vec::new(), entries.to_vec());
    }
    let mut pool = existing.to_vec();
    let mut reuse = Vec::new();
    let mut todo = Vec::new();
    for entry in entries {
        let found = pool.iter().position(|file| {
            file.file_name == entry.file_name
                && file.dir == entry.is_dir
                && (file.dir || file.size == entry.size)
        });
        if let Some(index) = found {
            reuse.push(pool.remove(index));
        } else {
            todo.push(entry.clone());
        }
    }
    (reuse, todo)
}

fn dedup_jobs(jobs: Vec<Job>) -> Vec<Job> {
    let mut used = HashSet::new();
    let mut out = Vec::new();
    for job in jobs {
        match job {
            Job::Dir(rel) => {
                used.insert(rel.clone());
                out.push(Job::Dir(rel));
            }
            Job::File { rel, fid, size } => {
                let rel = unique_path(&mut used, rel);
                out.push(Job::File { rel, fid, size });
            }
        }
    }
    out
}

fn unique_path(used: &mut HashSet<PathBuf>, rel: PathBuf) -> PathBuf {
    if used.insert(rel.clone()) {
        return rel;
    }
    let stem = rel.file_stem().and_then(|s| s.to_str()).unwrap_or("file");
    let ext = rel.extension().and_then(|s| s.to_str()).unwrap_or("");
    let parent = rel.parent().unwrap_or_else(|| Path::new(""));
    for index in 2..10_000 {
        let name = if ext.is_empty() {
            format!("{stem}_{index}")
        } else {
            format!("{stem}_{index}.{ext}")
        };
        let candidate = parent.join(name);
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    rel
}

fn is_safe_rel(path: &Path) -> bool {
    path.components()
        .all(|component| matches!(component, std::path::Component::Normal(_)))
}

fn sanitize_component(name: &str) -> String {
    let mut out = String::new();
    for ch in name.chars() {
        match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => out.push('_'),
            ch if (ch as u32) < 32 => out.push('_'),
            ch => out.push(ch),
        }
    }
    let trimmed = out.trim().trim_end_matches(['.', ' ']);
    let mut name = if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "unnamed".to_string()
    } else {
        trimmed.to_string()
    };
    name = avoid_reserved(name);
    if name.chars().count() > 80 {
        name = truncate_name(name, 80);
    }
    name
}

fn avoid_reserved(name: String) -> String {
    const RESERVED: &[&str] = &[
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    let stem = name.split('.').next().unwrap_or(name.as_str());
    if !RESERVED.iter().any(|item| stem.eq_ignore_ascii_case(item)) {
        return name;
    }
    match name.split_once('.') {
        Some((stem, ext)) if !ext.is_empty() => format!("{stem}_.{ext}"),
        _ => format!("{name}_"),
    }
}

fn truncate_name(name: String, max_chars: usize) -> String {
    let ext = name.rsplit_once('.').and_then(|(_, ext)| {
        let count = ext.chars().count();
        if count > 0 && count <= 10 {
            Some(ext.to_string())
        } else {
            None
        }
    });
    if let Some(ext) = ext {
        let keep = max_chars.saturating_sub(ext.chars().count() + 1);
        let prefix: String = name.chars().take(keep).collect();
        format!("{prefix}.{ext}")
    } else {
        name.chars().take(max_chars).collect()
    }
}

pub(crate) fn parse_share_input(input: &str, passcode_override: &str) -> Result<(String, String)> {
    let input = input.trim();
    if input.is_empty() {
        bail!("分享链接是空的");
    }
    let ids = share_link_ids(input);
    let pwd_id = if ids.len() > 1 {
        bail!("一次只能解析一条分享链接");
    } else if let Some(id) = ids.first() {
        id.clone()
    } else if !input.chars().any(char::is_whitespace) && input.chars().all(|ch| ch.is_ascii_alphanumeric()) {
        input.to_string()
    } else {
        bail!("无法解析分享链接，请粘贴 https://pan.quark.cn/s/ 开头的链接");
    };
    if pwd_id.len() < 4 {
        bail!("分享 ID 不正确: {pwd_id}");
    }
    let passcode = if !passcode_override.trim().is_empty() {
        passcode_override.trim().to_string()
    } else {
        extract_passcode(input)
    };
    Ok((pwd_id, passcode))
}

fn take_id(rest: &str) -> String {
    rest.chars()
        .take_while(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

fn share_link_ids(input: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = input;
    while let Some(index) = rest.find("/s/") {
        let after = &rest[index + 3..];
        let id = take_id(after);
        if id.len() >= 4 && !ids.iter().any(|seen| seen == &id) {
            ids.push(id.clone());
        }
        let step = if id.is_empty() {
            3 + rest[index + 3..]
                .chars()
                .next()
                .map(char::len_utf8)
                .unwrap_or(0)
        } else {
            3 + id.len()
        };
        rest = &rest[index + step..];
    }
    ids
}

fn extract_passcode(input: &str) -> String {
    if let Some(pos) = input.find("pwd=") {
        return take_query_value(&input[pos + 4..]);
    }
    for key in ["提取码：", "提取码:", "提取码 ", "密码：", "密码:"] {
        if let Some(pos) = input.find(key) {
            let rest = input[pos + key.len()..].trim_start();
            let code: String = rest
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric())
                .collect();
            if !code.is_empty() {
                return code;
            }
        }
    }
    trailing_passcode(input)
}

fn trailing_passcode(input: &str) -> String {
    let Some(index) = input.find("/s/") else {
        return String::new();
    };
    let after = &input[index + 3..];
    let id_len = after
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric())
        .map(char::len_utf8)
        .sum::<usize>();
    let rest = after[id_len..].trim_start();
    let rest = if let Some(query) = rest.strip_prefix('?') {
        let Some(space) = query.find(char::is_whitespace) else {
            return String::new();
        };
        query[space..].trim()
    } else {
        rest.trim()
    };
    let rest = rest.trim_start_matches(|ch: char| matches!(ch, '，' | ',' | '。' | '；' | ';' | ':' | '：'));
    let rest = rest.trim();
    if rest.is_empty() || rest.starts_with("提取码") || rest.starts_with("密码") || rest.starts_with("pwd")
    {
        return String::new();
    }
    let code: String = rest
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric())
        .collect();
    if !(2..=12).contains(&code.chars().count()) {
        return String::new();
    }
    let leftover = rest[code.len()..].trim();
    if leftover.is_empty() || leftover.chars().all(|ch| matches!(ch, '，' | ',' | '。' | '；' | ';')) {
        code
    } else {
        String::new()
    }
}

fn take_query_value(rest: &str) -> String {
    let raw: String = rest
        .chars()
        .take_while(|ch| !matches!(ch, '&' | '#' | ' ' | '"' | '\'' | '，' | '。'))
        .collect();
    percent_encoding::percent_decode_str(&raw)
        .decode_utf8_lossy()
        .into_owned()
}

fn fmt_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quark_file(name: &str, dir: bool, size: u64) -> QuarkFile {
        let mut file = QuarkFile::new_root();
        file.fid = format!("fid-{name}");
        file.file_name = name.to_string();
        file.dir = dir;
        file.file = !dir;
        file.size = size;
        file
    }

    fn entry(name: &str, dir: bool, size: u64) -> ShareEntry {
        ShareEntry {
            fid: format!("share-{name}"),
            file_name: name.to_string(),
            pdir_fid: "0".to_string(),
            size,
            is_dir: dir,
            share_fid_token: "tok".to_string(),
        }
    }

    #[test]
    fn parses_url_pwd_and_chinese_code() {
        let (id, pwd) = parse_share_input("https://pan.quark.cn/s/Ab12Cd?pwd=99xx", "").unwrap();
        assert_eq!(id, "Ab12Cd");
        assert_eq!(pwd, "99xx");

        let (id, pwd) =
            parse_share_input("链接：https://pan.quark.cn/s/ab12cd34 提取码：7788", "").unwrap();
        assert_eq!(id, "ab12cd34");
        assert_eq!(pwd, "7788");
    }

    #[test]
    fn explicit_passcode_wins_and_raw_id_works() {
        let (id, pwd) =
            parse_share_input("https://pan.quark.cn/s/ab12cd34?pwd=1111", "7788").unwrap();
        assert_eq!(id, "ab12cd34");
        assert_eq!(pwd, "7788");

        let (id, pwd) = parse_share_input("ab12cd34", "").unwrap();
        assert_eq!(id, "ab12cd34");
        assert_eq!(pwd, "");
        assert!(parse_share_input("https://example.com/nope", "").is_err());

        let (id, pwd) = parse_share_input("https://pan.quark.cn/s/Ab12Cd 7788", "").unwrap();
        assert_eq!(id, "Ab12Cd");
        assert_eq!(pwd, "7788");

        let paste = "我用夸克网盘给你分享了「格物心法」，点击链接或复制整段内容，打开「夸克APP」即可获取。\n链接：https://pan.quark.cn/s/ba91d9ffbf7a\n提取码：32TY";
        let (id, pwd) = parse_share_input(paste, "").unwrap();
        assert_eq!(id, "ba91d9ffbf7a");
        assert_eq!(pwd, "32TY");

        let repeated = "链接：https://pan.quark.cn/s/ba91d9ffbf7a\nhttps://pan.quark.cn/s/ba91d9ffbf7a\n提取码：32TY";
        let (id, pwd) = parse_share_input(repeated, "").unwrap();
        assert_eq!(id, "ba91d9ffbf7a");
        assert_eq!(pwd, "32TY");

        let err = parse_share_input(
            "https://pan.quark.cn/s/aaaaaaaa\nhttps://pan.quark.cn/s/bbbbbbbb",
            "",
        )
        .unwrap_err();
        assert!(err.to_string().contains("一条"));
    }

    #[test]
    fn sanitizes_windows_names() {
        assert_eq!(sanitize_component("ok.txt"), "ok.txt");
        assert_eq!(sanitize_component("a:b"), "a_b");
        assert_eq!(sanitize_component(".."), "unnamed");
        assert_eq!(sanitize_component("con"), "con_");
        assert_eq!(sanitize_component("CON.txt"), "CON_.txt");
        assert_eq!(sanitize_component(" file. "), "file");
    }

    #[test]
    fn reuses_same_name_and_size() {
        let entries = vec![entry("a.mp4", false, 10), entry("目录", true, 0)];
        let existing = vec![quark_file("a.mp4", false, 10), quark_file("目录", true, 0)];
        let (reuse, todo) = split_existing(&entries, &existing, false);
        assert!(todo.is_empty());
        assert_eq!(reuse.len(), 2);

        let (reuse, todo) = split_existing(&entries, &existing, true);
        assert!(reuse.is_empty());
        assert_eq!(todo.len(), 2);
    }

    #[test]
    fn different_size_is_saved_again() {
        let entries = vec![entry("a.mp4", false, 10)];
        let existing = vec![quark_file("a.mp4", false, 99)];
        let (reuse, todo) = split_existing(&entries, &existing, false);
        assert!(reuse.is_empty());
        assert_eq!(todo.len(), 1);
    }

    #[test]
    fn duplicate_file_names_get_a_suffix() {
        let jobs = vec![
            Job::File {
                rel: PathBuf::from("a.txt"),
                fid: "1".into(),
                size: 1,
            },
            Job::File {
                rel: PathBuf::from("a.txt"),
                fid: "2".into(),
                size: 1,
            },
        ];
        let jobs = dedup_jobs(jobs);
        match &jobs[1] {
            Job::File { rel, .. } => assert_eq!(rel, &PathBuf::from("a_2.txt")),
            Job::Dir(_) => panic!("expected file"),
        }
    }
}
