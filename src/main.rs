//! LiveQuery Server - Production Ready

mod core;
mod infra;
mod telemetry;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, error, warn};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    
    let cfg = Arc::new(core::Config::from_env()?);
    
    // Init tracing
    telemetry::init(&cfg)?;
    
    info!("LiveQuery v{} [{}]", VERSION, cfg.server_id);
    cfg.log_summary();
    
    // Init components
    let subs = Arc::new(core::SubscriptionManager::new(cfg.max_subscriptions));
    let db = Arc::new(infra::DbPool::new(&cfg)?);
    
    // Verify DB connection
    info!("Connecting to database...");
    db.ping().await?;
    info!("Database connected");
    
    // Connect to NATS with retry
    info!("Connecting to NATS...");
    let nats = infra::NatsHandler::connect(cfg.clone(), subs.clone()).await?;
    info!("NATS connected");
    
    // Shutdown signal
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    
    // Task: NATS message handler
    let h1 = tokio::spawn({
        let (n, d, mut rx) = (nats.clone(), db.clone(), shutdown_tx.subscribe());
        async move {
            tokio::select! {
                r = n.run(d) => { if let Err(e) = r { error!("NATS task: {e}"); } }
                _ = rx.recv() => { info!("NATS task stopped"); }
            }
        }
    });
    
    // Task: WAL streaming (native pgoutput protocol)
    let h2 = tokio::spawn({
        let (d, c, s, n, mut rx) = (db.clone(), cfg.clone(), subs.clone(), nats.clone(), shutdown_tx.subscribe());
        async move {
            let mut streamer = infra::WalStreamer::new(d, c);
            tokio::select! {
                res = streamer.run(s, n) => {
                    if let Err(e) = res { error!("WAL stream error: {e}"); }
                }
                _ = rx.recv() => { info!("WAL task stopped"); }
            }
        }
    });
    
    // Task: Client cleanup
    let h3 = tokio::spawn({
        let (s, c, mut rx) = (subs.clone(), cfg.clone(), shutdown_tx.subscribe());
        async move {
            let mut tick = tokio::time::interval(c.cleanup_interval());
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let removed = s.cleanup(c.client_timeout());
                        if !removed.is_empty() { info!("Cleanup: {} clients", removed.len()); }
                    }
                    _ = rx.recv() => { info!("Cleanup task stopped"); break; }
                }
            }
        }
    });
    
    // Task: Periodic stats logging
    let h4 = tokio::spawn({
        let (s, d, mut rx) = (subs.clone(), db.clone(), shutdown_tx.subscribe());
        async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let (active, avail, max) = d.pool_status();
                        let (queries, errs, avg_ms) = d.query_stats();
                        let (sub_count, query_count) = s.stats();
                        info!("Stats: subs={} queries={} pool={}/{}/{} db_queries={} errors={} avg={}ms",
                            sub_count, query_count, active, avail, max, queries, errs, avg_ms);
                    }
                    _ = rx.recv() => { break; }
                }
            }
        }
    });
    
    info!("✓ Ready - press Ctrl+C to stop");
    
    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutting down (timeout={}s)...", cfg.shutdown_timeout_secs);
    
    // Signal all tasks to stop
    let _ = shutdown_tx.send(());
    
    // Wait for graceful shutdown with timeout
    let shutdown = async {
        let _ = tokio::join!(h1, h2, h3, h4);
    };
    
    if tokio::time::timeout(cfg.shutdown_timeout(), shutdown).await.is_err() {
        warn!("Shutdown timeout, forcing exit");
    }
    
    info!("Goodbye!");
    Ok(())
}
