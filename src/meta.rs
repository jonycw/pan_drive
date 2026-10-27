//! pan_meta is the durable store for accounts, cookies, sessions, downloads, and audits.

use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{OptsBuilder, Pool, Row, TxOpts, params};
use serde::Deserialize;

use crate::portal::now_ms;

#[derive(Clone, Deserialize)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

#[derive(Clone)]
pub struct Meta {
    pool: Pool,
}

#[derive(Clone)]
pub struct StoredUser {
    pub name: String,
    pub salt: String,
    pub hash: String,
    pub expires_at: i64,
    pub download_dir: String,
    pub cookie_id: String,
    pub download_limit: u32,
    pub downloads_used: u32,
    pub created_at: i64,
}

#[derive(Clone)]
pub struct StoredCookie {
    pub id: String,
    pub label: String,
    pub raw: String,
    pub status: String,
    pub error: String,
    pub active: bool,
}

#[derive(Clone)]
pub struct StoredDisk {
    pub admin_salt: String,
    pub admin_hash: String,
    pub cloud_dir: String,
    pub download_dir: String,
    pub jobs: usize,
    pub users: Vec<StoredUser>,
    pub cookies: Vec<StoredCookie>,
}

#[derive(Clone)]
pub struct DownloadRecord {
    pub id: u64,
    pub owner: String,
    pub raw: String,
    pub share_id: String,
    pub only_fids: Vec<String>,
    pub phase: String,
    pub progress: String,
    pub file_name: String,
    pub percent: u64,
    pub speed: String,
    pub written: u64,
    pub total: u64,
    pub error: Option<String>,
    pub lines: Vec<String>,
    pub counted: bool,
}

#[derive(Clone)]
pub struct OpRow {
    pub at_ms: i64,
    pub actor: String,
    pub action: String,
    pub detail: String,
    pub ok: bool,
}

impl Meta {
    pub fn connect(config: &DbConfig) -> Result<Self, String> {
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some(config.host.clone()))
            .tcp_port(config.port)
            .user(Some(config.user.clone()))
            .pass(Some(config.password.clone()))
            .db_name(Some(config.database.clone()))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(Duration::from_secs(8)))
            .read_timeout(Some(Duration::from_secs(20)))
            .write_timeout(Some(Duration::from_secs(20)));
        let pool = Pool::new(opts).map_err(db_err)?;
        let meta = Self { pool };
        meta.ensure_schema()?;
        let mut conn = meta.conn()?;
        conn.query_drop("SELECT 1").map_err(db_err)?;
        tracing::info!(
            "已连接 {} {}:{}/{}",
            "pan_meta",
            config.host,
            config.port,
            config.database
        );
        Ok(meta)
    }

    pub fn is_empty(&self) -> Result<bool, String> {
        let mut conn = self.conn()?;
        let admin: Option<i64> = conn
            .query_first("SELECT COUNT(*) FROM admin_account")
            .map_err(db_err)?;
        let users: Option<i64> = conn.query_first("SELECT COUNT(*) FROM users").map_err(db_err)?;
        let cookies: Option<i64> = conn.query_first("SELECT COUNT(*) FROM cookies").map_err(db_err)?;
        Ok(admin.unwrap_or(0) == 0 && users.unwrap_or(0) == 0 && cookies.unwrap_or(0) == 0)
    }

    pub fn save_disk(&self, disk: &StoredDisk) -> Result<(), String> {
        let mut conn = self.conn()?;
        let mut tx = conn.start_transaction(TxOpts::default()).map_err(db_err)?;
        tx.exec_drop("DELETE FROM app_settings", ()).map_err(db_err)?;
        tx.exec_drop(
            "INSERT INTO app_settings (id, cloud_dir, download_dir, jobs) VALUES (1, ?, ?, ?)",
            (&disk.cloud_dir, &disk.download_dir, disk.jobs as i64),
        )
        .map_err(db_err)?;
        tx.exec_drop("DELETE FROM admin_account", ()).map_err(db_err)?;
        if !disk.admin_hash.is_empty() {
            tx.exec_drop(
                "INSERT INTO admin_account (id, salt, hash) VALUES (1, ?, ?)",
                (&disk.admin_salt, &disk.admin_hash),
            )
            .map_err(db_err)?;
        }
        tx.exec_drop("DELETE FROM users", ()).map_err(db_err)?;
        for user in &disk.users {
            tx.exec_drop(
                "INSERT INTO users (name, salt, hash, expires_at, download_dir, cookie_id, download_limit, downloads_used, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    &user.name,
                    &user.salt,
                    &user.hash,
                    user.expires_at,
                    &user.download_dir,
                    &user.cookie_id,
                    user.download_limit as i64,
                    user.downloads_used as i64,
                    user.created_at,
                ),
            )
            .map_err(db_err)?;
        }
        tx.exec_drop("DELETE FROM cookies", ()).map_err(db_err)?;
        for cookie in &disk.cookies {
            tx.exec_drop(
                "INSERT INTO cookies (id, label, raw, status, error, active) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    &cookie.id,
                    &cookie.label,
                    &cookie.raw,
                    &cookie.status,
                    &cookie.error,
                    i32::from(cookie.active),
                ),
            )
            .map_err(db_err)?;
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn load_disk(&self) -> Result<StoredDisk, String> {
        let mut conn = self.conn()?;
        let settings = conn
            .query_first::<(String, String, i64), _>(
                "SELECT cloud_dir, download_dir, jobs FROM app_settings WHERE id = 1",
            )
            .map_err(db_err)?;
        let admin = conn
            .query_first::<(String, String), _>("SELECT salt, hash FROM admin_account WHERE id = 1")
            .map_err(db_err)?;
        let users = conn
            .query_map(
                "SELECT name, salt, hash, expires_at, download_dir, cookie_id, download_limit, downloads_used, created_at FROM users",
                |(name, salt, hash, expires_at, download_dir, cookie_id, download_limit, downloads_used, created_at): (String, String, String, i64, String, String, i64, i64, i64)| {
                    StoredUser {
                        name,
                        salt,
                        hash,
                        expires_at,
                        download_dir,
                        cookie_id,
                        download_limit: download_limit.max(0) as u32,
                        downloads_used: downloads_used.max(0) as u32,
                        created_at,
                    }
                },
            )
            .map_err(db_err)?;
        let cookies = conn
            .query_map(
                "SELECT id, label, raw, status, error, active FROM cookies",
                |(id, label, raw, status, error, active): (String, String, String, String, String, i64)| {
                    StoredCookie {
                        id,
                        label,
                        raw,
                        status,
                        error,
                        active: active != 0,
                    }
                },
            )
            .map_err(db_err)?;
        let (cloud_dir, download_dir, jobs) = settings.unwrap_or_else(|| ("/来自分享".to_string(), String::new(), 3));
        let (admin_salt, admin_hash) = admin.unwrap_or_default();
        Ok(StoredDisk {
            admin_salt,
            admin_hash,
            cloud_dir,
            download_dir,
            jobs: jobs.max(1) as usize,
            users,
            cookies,
        })
    }

    pub fn save_session(&self, token: &str, expires_at: i64) {
        let mut conn = match self.conn() {
            Ok(conn) => conn,
            Err(err) => {
                tracing::error!("{err}");
                return;
            }
        };
        if let Err(err) = conn.exec_drop(
            "INSERT INTO admin_sessions (token, expires_at) VALUES (?, ?) ON DUPLICATE KEY UPDATE expires_at = VALUES(expires_at)",
            (token, expires_at),
        ) {
            tracing::error!("{}", db_err(err));
        }
    }

    pub fn delete_session(&self, token: &str) {
        let mut conn = match self.conn() {
            Ok(conn) => conn,
            Err(_) => return,
        };
        let _ = conn.exec_drop("DELETE FROM admin_sessions WHERE token = ?", (token,));
    }

    pub fn load_sessions(&self) -> Result<Vec<(String, i64)>, String> {
        let mut conn = self.conn()?;
        let now = now_ms();
        conn.exec_drop("DELETE FROM admin_sessions WHERE expires_at <= ?", (now,))
            .map_err(db_err)?;
        conn.exec_map(
            "SELECT token, expires_at FROM admin_sessions",
            (),
            |(token, expires_at): (String, i64)| (token, expires_at),
        )
        .map_err(db_err)
    }

    pub fn save_download(&self, record: &DownloadRecord) -> Result<(), String> {
        let only_fids = serde_json::to_string(&record.only_fids).unwrap_or_else(|_| "[]".to_string());
        let lines = serde_json::to_string(&record.lines).unwrap_or_else(|_| "[]".to_string());
        let mut conn = self.conn()?;
        let mut tx = conn.start_transaction(TxOpts::default()).map_err(db_err)?;
        tx.exec_drop("DELETE FROM download_records WHERE id = ?", (record.id as i64,))
            .map_err(db_err)?;
        tx.exec_drop(
            "INSERT INTO download_records (id, owner, raw, share_id, only_fids, phase, progress, file_name, percent, speed, written_bytes, total_bytes, error_text, lines_json, counted, updated_at) VALUES (:id, :owner, :raw, :share_id, :only_fids, :phase, :progress, :file_name, :percent, :speed, :written_bytes, :total_bytes, :error_text, :lines_json, :counted, :updated_at)",
            params! {
                "id" => record.id as i64,
                "owner" => &record.owner,
                "raw" => &record.raw,
                "share_id" => &record.share_id,
                "only_fids" => only_fids,
                "phase" => &record.phase,
                "progress" => clip(&record.progress, 500),
                "file_name" => clip(&record.file_name, 400),
                "percent" => record.percent as i64,
                "speed" => clip(&record.speed, 32),
                "written_bytes" => record.written as i64,
                "total_bytes" => record.total as i64,
                "error_text" => record.error.as_deref(),
                "lines_json" => lines,
                "counted" => i32::from(record.counted),
                "updated_at" => now_ms(),
            },
        )
        .map_err(db_err)?;
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    pub fn load_downloads(&self) -> Result<Vec<DownloadRecord>, String> {
        let mut conn = self.conn()?;
        let mut rows = conn
            .exec_map(
                "SELECT id, owner, raw, share_id, only_fids, phase, progress, file_name, percent, speed, written_bytes, total_bytes, error_text, lines_json, counted FROM download_records ORDER BY id DESC LIMIT 200",
                (),
                |row: Row| row_to_download(&row),
            )
            .map_err(db_err)?;
        rows.reverse();
        Ok(rows)
    }

    pub fn log_op(&self, actor: &str, action: &str, detail: &str, ok: bool) {
        let mut conn = match self.conn() {
            Ok(conn) => conn,
            Err(_) => return,
        };
        let _ = conn.exec_drop(
            "INSERT INTO operation_logs (at_ms, actor, action, detail, ok) VALUES (?, ?, ?, ?, ?)",
            (now_ms(), clip(actor, 64), clip(action, 64), clip(detail, 1000), i32::from(ok)),
        );
    }

    pub fn recent_ops(&self, limit: u32) -> Result<Vec<OpRow>, String> {
        let mut conn = self.conn()?;
        let limit = limit.clamp(1, 200) as i64;
        conn.exec_map(
            "SELECT at_ms, actor, action, detail, ok FROM operation_logs ORDER BY id DESC LIMIT ?",
            (limit,),
            |(at_ms, actor, action, detail, ok): (i64, String, String, String, i64)| OpRow {
                at_ms,
                actor,
                action,
                detail,
                ok: ok != 0,
            },
        )
        .map_err(db_err)
    }

    fn conn(&self) -> Result<mysql::PooledConn, String> {
        self.pool.get_conn().map_err(db_err)
    }

    fn ensure_schema(&self) -> Result<(), String> {
        let mut conn = self.conn()?;
        for statement in SCHEMA {
            conn.query_drop(statement).map_err(db_err)?;
        }
        drop(conn);
        self.ensure_user_created_at()
    }

    fn ensure_user_created_at(&self) -> Result<(), String> {
        let mut conn = self.conn()?;
        let count: Option<i64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users' AND COLUMN_NAME = 'created_at'",
                (),
            )
            .map_err(db_err)?;
        if count.unwrap_or(0) == 0 {
            conn.query_drop("ALTER TABLE users ADD COLUMN created_at BIGINT NOT NULL DEFAULT 0")
                .map_err(db_err)?;
        }
        Ok(())
    }
}

fn row_to_download(row: &Row) -> DownloadRecord {
    let only_text: String = row.get("only_fids").unwrap_or_default();
    let lines_text: String = row.get("lines_json").unwrap_or_default();
    let error = row.get::<Option<String>, _>("error_text").unwrap_or(None);
    DownloadRecord {
        id: row.get::<i64, _>("id").unwrap_or(0).max(0) as u64,
        owner: row.get("owner").unwrap_or_default(),
        raw: row.get("raw").unwrap_or_default(),
        share_id: row.get("share_id").unwrap_or_default(),
        only_fids: serde_json::from_str(&only_text).unwrap_or_default(),
        phase: row.get("phase").unwrap_or_default(),
        progress: row.get("progress").unwrap_or_default(),
        file_name: row.get("file_name").unwrap_or_default(),
        percent: row.get::<i64, _>("percent").unwrap_or(0).max(0) as u64,
        speed: row.get("speed").unwrap_or_default(),
        written: row.get::<i64, _>("written_bytes").unwrap_or(0).max(0) as u64,
        total: row.get::<i64, _>("total_bytes").unwrap_or(0).max(0) as u64,
        error: error.filter(|text| !text.is_empty()),
        lines: serde_json::from_str(&lines_text).unwrap_or_default(),
        counted: row.get::<i64, _>("counted").unwrap_or(0) != 0,
    }
}

fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        text.chars().take(max).collect()
    }
}

fn db_err(err: mysql::Error) -> String {
    match &err {
        mysql::Error::MySqlError(server) => {
            tracing::error!("pan_meta {} {}", server.code, server.message);
        }
        other => tracing::error!("pan_meta {other}"),
    }
    "数据库保存失败".to_string()
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS app_settings (
        id TINYINT PRIMARY KEY,
        cloud_dir VARCHAR(512) NOT NULL,
        download_dir VARCHAR(1024) NOT NULL,
        jobs INT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS admin_account (
        id TINYINT PRIMARY KEY,
        salt VARCHAR(128) NOT NULL,
        hash VARCHAR(128) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS users (
        name VARCHAR(64) PRIMARY KEY,
        salt VARCHAR(128) NOT NULL,
        hash VARCHAR(128) NOT NULL,
        expires_at BIGINT NOT NULL,
        download_dir VARCHAR(1024) NOT NULL,
        cookie_id VARCHAR(64) NOT NULL,
        download_limit INT NOT NULL,
        downloads_used INT NOT NULL,
        created_at BIGINT NOT NULL DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS cookies (
        id VARCHAR(64) PRIMARY KEY,
        label VARCHAR(64) NOT NULL,
        raw MEDIUMTEXT NOT NULL,
        status VARCHAR(32) NOT NULL,
        error TEXT NOT NULL,
        active TINYINT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS admin_sessions (
        token VARCHAR(128) PRIMARY KEY,
        expires_at BIGINT NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS operation_logs (
        id BIGINT PRIMARY KEY AUTO_INCREMENT,
        at_ms BIGINT NOT NULL,
        actor VARCHAR(64) NOT NULL,
        action VARCHAR(64) NOT NULL,
        detail VARCHAR(1000) NOT NULL,
        ok TINYINT NOT NULL,
        KEY idx_op_at (at_ms)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS download_records (
        id BIGINT PRIMARY KEY,
        owner VARCHAR(64) NOT NULL,
        raw MEDIUMTEXT NOT NULL,
        share_id VARCHAR(128) NOT NULL,
        only_fids TEXT NOT NULL,
        phase VARCHAR(32) NOT NULL,
        progress VARCHAR(1024) NOT NULL,
        file_name VARCHAR(512) NOT NULL,
        percent INT NOT NULL,
        speed VARCHAR(32) NOT NULL,
        written_bytes BIGINT NOT NULL,
        total_bytes BIGINT NOT NULL,
        error_text MEDIUMTEXT NULL,
        lines_json MEDIUMTEXT NOT NULL,
        counted TINYINT NOT NULL,
        updated_at BIGINT NOT NULL,
        KEY idx_dl_owner (owner, id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
];
