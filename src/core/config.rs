//! Production Configuration - All tunables in one place

use anyhow::{bail, Context, Result};
use std::env::var;
use std::time::Duration;

/// LiveQuery Server Configuration
#[derive(Debug, Clone)]
pub struct Config {
    // === Server ===
    pub server_id: String,
    pub log_level: String,
    pub shutdown_timeout_secs: u64,

    // === Database ===
    pub db_url: String,
    pub db_pool_size: u32,
    pub db_timeout_secs: u64,

    // === NATS ===
    pub nats_url: String,
    pub nats_prefix: String,

    // === Subscriptions ===
    pub client_timeout_secs: u64,
    pub cleanup_interval_secs: u64,
    pub max_subscriptions: usize,

    // === WAL ===
    pub wal_slot: String,
    pub wal_publication: String,
}

impl Config {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let cfg = Self {
            // Server
            server_id: var("SERVER_ID").unwrap_or_else(|_| gen_id()),
            log_level: var("LOG_LEVEL")
                .or_else(|_| var("RUST_LOG"))
                .unwrap_or_else(|_| "info".into()),
            shutdown_timeout_secs: env("SHUTDOWN_TIMEOUT_SECS", 30),

            // Database
            db_url: var("DATABASE_URL")
                .or_else(|_| var("POSTGRES_URL"))
                .context("DATABASE_URL required")?,
            db_pool_size: env("DB_POOL_SIZE", 16),
            db_timeout_secs: env("DB_TIMEOUT_SECS", 30),

            // NATS
            nats_url: var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into()),
            nats_prefix: var("NATS_PREFIX").unwrap_or_else(|_| "livequery".into()),

            // Subscriptions
            client_timeout_secs: env("CLIENT_TIMEOUT_SECS", 30),
            cleanup_interval_secs: env("CLEANUP_INTERVAL_SECS", 10),
            max_subscriptions: env("MAX_SUBSCRIPTIONS", 10000),

            // WAL
            wal_slot: var("WAL_SLOT").unwrap_or_else(|_| "livequery_slot".into()),
            wal_publication: var("WAL_PUBLICATION").unwrap_or_else(|_| "livequery_pub".into()),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate configuration values
    fn validate(&self) -> Result<()> {
        macro_rules! check {
            ($cond:expr, $msg:expr) => {
                if $cond {
                    bail!($msg);
                }
            };
        }
        check!(self.db_pool_size == 0, "DB_POOL_SIZE must be > 0");
        check!(self.db_pool_size > 100, "DB_POOL_SIZE must be <= 100");
        check!(self.db_timeout_secs == 0, "DB_TIMEOUT_SECS must be > 0");
        check!(
            self.client_timeout_secs < 5,
            "CLIENT_TIMEOUT_SECS must be >= 5"
        );
        check!(
            self.cleanup_interval_secs == 0,
            "CLEANUP_INTERVAL_SECS must be > 0"
        );
        check!(self.max_subscriptions == 0, "MAX_SUBSCRIPTIONS must be > 0");
        check!(
            self.shutdown_timeout_secs == 0,
            "SHUTDOWN_TIMEOUT_SECS must be > 0"
        );
        Ok(())
    }

    // === Duration helpers ===

    #[inline]
    pub fn client_timeout(&self) -> Duration {
        Duration::from_secs(self.client_timeout_secs)
    }
    #[inline]
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_secs)
    }
    #[inline]
    pub fn db_timeout(&self) -> Duration {
        Duration::from_secs(self.db_timeout_secs)
    }
    #[inline]
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.shutdown_timeout_secs)
    }

    /// Subscription-specific events subject: livequery.{sub_id}.events
    #[inline]
    pub fn sub_events_subject(&self, sub_id: &str) -> String {
        format!("{}.{}.events", self.nats_prefix, sub_id)
    }

    /// Safe URL for logging (hides password)
    pub fn db_url_safe(&self) -> String {
        self.db_url
            .find('@')
            .and_then(|a| {
                self.db_url[..a]
                    .rfind(':')
                    .map(|c| format!("{}****{}", &self.db_url[..c + 1], &self.db_url[a..]))
            })
            .unwrap_or_else(|| self.db_url.clone())
    }

    /// Log all config (safe)
    pub fn log_summary(&self) {
        use tracing::info;
        info!("╭─ LiveQuery Config ─────────────────────╮");
        info!("│ server_id: {:<27} │", &self.server_id);
        info!("│ db:        {:<27} │", self.db_url_safe());
        info!("│ db_pool:   {:<27} │", self.db_pool_size);
        info!("│ nats:      {:<27} │", &self.nats_url);
        info!("│ prefix:    {:<27} │", &self.nats_prefix);
        info!("│ wal_slot:  {:<27} │", &self.wal_slot);
        info!(
            "│ timeout:   {}s cleanup={}s{:<13} │",
            self.client_timeout_secs, self.cleanup_interval_secs, ""
        );
        info!("╰────────────────────────────────────────╯");
    }
}

#[inline]
fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[inline]
fn gen_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("lq-{:x}", ts & 0xFFFFFF)
}

/// Builder pattern for Config (useful for testing)
#[derive(Default)]
#[allow(dead_code)] // API for testing/embedding
pub struct ConfigBuilder {
    server_id: Option<String>,
    log_level: Option<String>,
    shutdown_timeout_secs: Option<u64>,
    db_url: Option<String>,
    db_pool_size: Option<u32>,
    db_timeout_secs: Option<u64>,
    nats_url: Option<String>,
    nats_prefix: Option<String>,
    client_timeout_secs: Option<u64>,
    cleanup_interval_secs: Option<u64>,
    max_subscriptions: Option<usize>,
    wal_slot: Option<String>,
    wal_publication: Option<String>,
}

#[allow(dead_code)] // API for testing/embedding
impl ConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn server_id(mut self, v: impl Into<String>) -> Self {
        self.server_id = Some(v.into());
        self
    }
    pub fn log_level(mut self, v: impl Into<String>) -> Self {
        self.log_level = Some(v.into());
        self
    }
    pub fn shutdown_timeout_secs(mut self, v: u64) -> Self {
        self.shutdown_timeout_secs = Some(v);
        self
    }
    pub fn db_url(mut self, v: impl Into<String>) -> Self {
        self.db_url = Some(v.into());
        self
    }
    pub fn db_pool_size(mut self, v: u32) -> Self {
        self.db_pool_size = Some(v);
        self
    }
    pub fn db_timeout_secs(mut self, v: u64) -> Self {
        self.db_timeout_secs = Some(v);
        self
    }
    pub fn nats_url(mut self, v: impl Into<String>) -> Self {
        self.nats_url = Some(v.into());
        self
    }
    pub fn nats_prefix(mut self, v: impl Into<String>) -> Self {
        self.nats_prefix = Some(v.into());
        self
    }
    pub fn client_timeout_secs(mut self, v: u64) -> Self {
        self.client_timeout_secs = Some(v);
        self
    }
    pub fn cleanup_interval_secs(mut self, v: u64) -> Self {
        self.cleanup_interval_secs = Some(v);
        self
    }
    pub fn max_subscriptions(mut self, v: usize) -> Self {
        self.max_subscriptions = Some(v);
        self
    }
    pub fn wal_slot(mut self, v: impl Into<String>) -> Self {
        self.wal_slot = Some(v.into());
        self
    }
    pub fn wal_publication(mut self, v: impl Into<String>) -> Self {
        self.wal_publication = Some(v.into());
        self
    }

    pub fn build(self) -> Result<Config> {
        let cfg = Config {
            server_id: self.server_id.unwrap_or_else(gen_id),
            log_level: self.log_level.unwrap_or_else(|| "info".into()),
            shutdown_timeout_secs: self.shutdown_timeout_secs.unwrap_or(30),
            db_url: self
                .db_url
                .ok_or_else(|| anyhow::anyhow!("db_url is required"))?,
            db_pool_size: self.db_pool_size.unwrap_or(16),
            db_timeout_secs: self.db_timeout_secs.unwrap_or(30),
            nats_url: self
                .nats_url
                .unwrap_or_else(|| "nats://localhost:4222".into()),
            nats_prefix: self.nats_prefix.unwrap_or_else(|| "livequery".into()),
            client_timeout_secs: self.client_timeout_secs.unwrap_or(30),
            cleanup_interval_secs: self.cleanup_interval_secs.unwrap_or(10),
            max_subscriptions: self.max_subscriptions.unwrap_or(10000),
            wal_slot: self.wal_slot.unwrap_or_else(|| "livequery_slot".into()),
            wal_publication: self
                .wal_publication
                .unwrap_or_else(|| "livequery_pub".into()),
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

impl Config {
    /// Create a builder for Config
    #[allow(dead_code)] // API for testing/embedding
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::new()
    }
}
