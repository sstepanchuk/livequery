use anyhow::Result;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::core::Config;

/// Initialize tracing. Env: LOG_FORMAT=compact|pretty|json
pub fn init(cfg: &Config) -> Result<()> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cfg.log_level));

    let builder = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(false)
        .with_line_number(false);

    match std::env::var("LOG_FORMAT").as_deref() {
        Ok("json") => builder.json().finish().try_init()?,
        Ok("pretty") => builder.pretty().finish().try_init()?,
        _ => builder.compact().finish().try_init()?,
    }
    Ok(())
}
