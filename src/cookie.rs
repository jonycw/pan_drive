//! Cookie file loader. Saving the file updates the running process within about a second.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

static WATCH_PAUSED: AtomicBool = AtomicBool::new(false);
static SAVED_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);

pub fn set_file_watch_paused(paused: bool) {
    WATCH_PAUSED.store(paused, Ordering::Relaxed);
}

pub fn saved_file_text() -> Option<String> {
    let path = SAVED_PATH.lock().ok()?.clone()?;
    std::fs::read_to_string(path).ok()
}

pub fn install_cookie_text(map: &DashMap<String, String>, text: &str) -> Result<(), String> {
    let pairs = parse_cookie(text).map_err(|err| err.to_string())?;
    if pairs.is_empty() {
        return Err("Cookie 是空的".to_string());
    }
    apply_pairs(map, &pairs);
    Ok(())
}

use anyhow::{Context, Result, bail};
use dashmap::DashMap;

pub fn load_cookie(
    cookie_file: &Path,
    inline: Option<&str>,
) -> Result<Arc<DashMap<String, String>>> {
    let path = resolve_cookie_path(cookie_file);
    if path.is_file() {
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("读取 Cookie 文件失败: {}", path.display()))?;
        let pairs = parse_cookie(&text)?;
        if pairs.is_empty() {
            bail!("Cookie 文件是空的: {}", path.display());
        }
        let map = Arc::new(DashMap::new());
        apply_pairs(&map, &pairs);
        if let Ok(mut saved) = SAVED_PATH.lock() {
            *saved = Some(path.clone());
        }
        if inline.is_some() {
            tracing::info!("已读取 Cookie 文件 {}，忽略 QUARK_COOKIE", path.display());
        } else {
            tracing::info!("已读取 Cookie 文件 {}", path.display());
        }
        watch_cookie_file(path, map.clone(), text);
        return Ok(map);
    }

    if let Some(inline) = inline {
        let pairs = parse_cookie(inline)?;
        if pairs.is_empty() {
            bail!("QUARK_COOKIE 是空的");
        }
        let map = Arc::new(DashMap::new());
        apply_pairs(&map, &pairs);
        tracing::info!(
            "使用 QUARK_COOKIE。要热更新，把 Cookie 写到 {}",
            path.display()
        );
        return Ok(map);
    }

    tracing::info!(
        "还没有 Cookie 文件 {}。可以在管理端添加 Cookie，或之后把文件放到这里",
        path.display()
    );
    let map = Arc::new(DashMap::new());
    if let Ok(mut saved) = SAVED_PATH.lock() {
        *saved = Some(path.clone());
    }
    watch_cookie_file(path, map.clone(), String::new());
    Ok(map)
}

pub fn parse_cookie(text: &str) -> Result<Vec<(String, String)>> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut pairs = Vec::new();
    for raw_line in text.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line
            .strip_prefix("cookie:")
            .or_else(|| line.strip_prefix("Cookie:"))
        {
            line = rest.trim();
        }
        for part in line.split(';') {
            let part = part.trim();
            if part.is_empty() || part.starts_with('#') {
                continue;
            }
            let Some((key, value)) = part.split_once('=') else {
                bail!("Cookie 项缺少等号: {part}");
            };
            let key = key.trim();
            if key.is_empty() {
                bail!("Cookie 名是空的");
            }
            let value = trim_matching_quotes(value.trim());
            pairs.push((key.to_string(), value.to_string()));
        }
    }
    Ok(pairs)
}

fn resolve_cookie_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn trim_matching_quotes(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() >= 2
        && (bytes[0] == b'"' || bytes[0] == b'\'')
        && bytes[0] == bytes[bytes.len() - 1]
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

fn apply_pairs(map: &DashMap<String, String>, pairs: &[(String, String)]) {
    let keep: HashSet<&str> = pairs.iter().map(|(key, _)| key.as_str()).collect();
    map.retain(|key, _| keep.contains(key.as_str()));
    for (key, value) in pairs {
        map.insert(key.clone(), value.clone());
    }
}

fn watch_cookie_file(path: PathBuf, map: Arc<DashMap<String, String>>, mut last_text: String) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut broken = false;
        loop {
            ticker.tick().await;
            if WATCH_PAUSED.load(Ordering::Relaxed) {
                continue;
            }
            match tokio::fs::read_to_string(&path).await {
                Ok(text) => {
                    broken = false;
                    if text == last_text {
                        continue;
                    }
                    match parse_cookie(&text) {
                        Ok(pairs) if pairs.is_empty() => {
                            tracing::warn!("Cookie 文件为空，继续使用上一份: {}", path.display());
                        }
                        Ok(pairs) => {
                            apply_pairs(&map, &pairs);
                            last_text = text;
                            tracing::info!("Cookie 已热更新: {}", path.display());
                        }
                        Err(err) => {
                            tracing::warn!("Cookie 文件无法解析，继续使用上一份: {err}");
                        }
                    }
                }
                Err(err) => {
                    if !broken {
                        tracing::warn!("读取 Cookie 文件失败，继续使用上一份: {err}");
                        broken = true;
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_header_and_lines() {
        let pairs =
            parse_cookie("\u{feff}# comment\n__pus=abc; __puus=\"a=b\"\n_UP_D_=hello\n").unwrap();
        assert_eq!(
            pairs,
            vec![
                ("__pus".to_string(), "abc".to_string()),
                ("__puus".to_string(), "a=b".to_string()),
                ("_UP_D_".to_string(), "hello".to_string()),
            ]
        );
    }

    #[test]
    fn parses_cookie_prefix_and_rejects_bad_item() {
        let pairs = parse_cookie("Cookie: a=1; b=2").unwrap();
        assert_eq!(pairs.len(), 2);
        assert!(parse_cookie("not-a-cookie").is_err());
    }

    #[test]
    fn reload_replaces_removed_keys() {
        let map = DashMap::new();
        apply_pairs(
            &map,
            &[
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ],
        );
        apply_pairs(&map, &[("a".to_string(), "3".to_string())]);
        assert_eq!(map.get("a").map(|v| v.clone()), Some("3".to_string()));
        assert!(map.get("b").is_none());
    }
}
