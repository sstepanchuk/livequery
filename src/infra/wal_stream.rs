//! Native PostgreSQL Logical Replication Streaming

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace};
use url::Url;

use crate::core::event::SubscriptionMode;
use crate::core::query::{EvalResult, WhereFilter};
use crate::core::row::RowData;
use crate::core::{Config, SubscriptionManager};
use crate::infra::pgoutput::{PgOutputDecoder, WalChange};
use crate::infra::{DbPool, NatsHandler};

const MAX_CONCURRENT: usize = 8;

/// Streaming WAL reader using native PostgreSQL replication protocol
pub struct WalStreamer {
    cfg: Arc<Config>,
    db: Arc<DbPool>,
    decoder: PgOutputDecoder,
    stats: WalStats,
}

struct WalStats {
    processed: AtomicU64,
    requeries: AtomicU64,
    skipped: AtomicU64,
}

impl WalStreamer {
    pub fn new(db: Arc<DbPool>, cfg: Arc<Config>) -> Self {
        Self {
            cfg,
            db,
            decoder: PgOutputDecoder::new(),
            stats: WalStats {
                processed: AtomicU64::new(0),
                requeries: AtomicU64::new(0),
                skipped: AtomicU64::new(0),
            },
        }
    }

    pub async fn run(&mut self, subs: Arc<SubscriptionManager>, nats: NatsHandler) -> Result<()> {
        info!("WAL streaming mode starting");
        let config = self.build_config()?;

        let mut retry_delay = Duration::from_secs(1);
        const MAX_DELAY: Duration = Duration::from_secs(60);

        loop {
            match self.stream_loop(&config, &subs, &nats).await {
                Ok(()) => {
                    info!("WAL stream ended, reconnecting...");
                    retry_delay = Duration::from_secs(1); // Reset on success
                }
                Err(e) => {
                    error!("WAL error: {e}, retry in {:?}", retry_delay);
                    tokio::time::sleep(retry_delay).await;
                    // Exponential backoff with cap
                    retry_delay = (retry_delay * 2).min(MAX_DELAY);
                }
            }
        }
    }

    fn build_config(&self) -> Result<ReplicationConfig> {
        // Use url crate for robust parsing
        let parsed = Url::parse(&self.cfg.db_url)
            .map_err(|e| anyhow::anyhow!("Invalid database URL: {}", e))?;

        let user = if parsed.username().is_empty() {
            "postgres".to_string()
        } else {
            parsed.username().to_string()
        };
        let password = parsed.password().unwrap_or("").to_string();
        let host = parsed.host_str().unwrap_or("localhost").to_string();
        let port = parsed.port().unwrap_or(5432);

        Ok(ReplicationConfig {
            host,
            port,
            user,
            password,
            database: parsed.path().trim_start_matches('/').to_string(),
            tls: TlsConfig::disabled(),
            slot: self.cfg.wal_slot.clone(),
            publication: self.cfg.wal_publication.clone(),
            start_lsn: Lsn::from(0u64),
            stop_at_lsn: None,
            status_interval: Duration::from_secs(10),
            idle_wakeup_interval: Duration::from_secs(30),
            buffer_events: 8192,
        })
    }

    async fn stream_loop(
        &mut self,
        config: &ReplicationConfig,
        subs: &Arc<SubscriptionManager>,
        nats: &NatsHandler,
    ) -> Result<()> {
        let mut client = ReplicationClient::connect(config.clone()).await?;
        info!(
            "WAL connected slot={} pub={}",
            self.cfg.wal_slot, self.cfg.wal_publication
        );

        let mut tx: FxHashMap<Arc<str>, Vec<RowData>> = FxHashMap::default();
        let mut in_tx = false;

        while let Some(event) = client.recv().await? {
            match event {
                ReplicationEvent::XLogData { wal_end, data, .. } => {
                    self.stats.processed.fetch_add(1, Relaxed);

                    if let Some(change) = self.decoder.decode(&data) {
                        match change {
                            WalChange::Begin => {
                                in_tx = true;
                                tx.clear();
                            }
                            WalChange::Commit => {
                                if !tx.is_empty() {
                                    self.process(&tx, subs, nats).await;
                                    tx.clear();
                                }
                                in_tx = false;
                                client.update_applied_lsn(wal_end);
                            }
                            WalChange::Insert { rel, row } | WalChange::Update { rel, row } => {
                                if let Some(t) = self.decoder.get_table(rel) {
                                    if subs.has_table(t) {
                                        tx.entry(Arc::from(t)).or_default().push(row);
                                    }
                                }
                            }
                            WalChange::Delete { rel } => {
                                if let Some(t) = self.decoder.get_table(rel) {
                                    if subs.has_table(t) {
                                        tx.entry(Arc::from(t)).or_default();
                                    }
                                }
                            }
                            WalChange::Truncate { rels } => {
                                for r in rels {
                                    if let Some(t) = self.decoder.get_table(r) {
                                        if subs.has_table(t) {
                                            tx.entry(Arc::from(t)).or_default();
                                        }
                                    }
                                }
                            }
                            WalChange::Other => {}
                        }
                    }

                    if !in_tx && !tx.is_empty() {
                        self.process(&tx, subs, nats).await;
                        tx.clear();
                        client.update_applied_lsn(wal_end);
                    }
                }
                ReplicationEvent::KeepAlive { .. } => trace!("keepalive"),
                ReplicationEvent::StoppedAt { reached } => {
                    info!("WAL stopped at {reached:?}");
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn process(
        &self,
        changes: &FxHashMap<Arc<str>, Vec<RowData>>,
        subs: &SubscriptionManager,
        nats: &NatsHandler,
    ) {
        let mut to_requery: FxHashSet<Arc<str>> = FxHashSet::default();
        let mut skipped = 0usize;

        for (table, rows) in changes {
            subs.for_table_queries(table, |qid| {
                if !to_requery.insert(qid.clone()) {
                    return;
                }

                let Some(q) = subs.get_query(qid) else {
                    to_requery.remove(qid);
                    return;
                };

                // WHERE filter optimization
                if q.is_simple
                    && !rows.is_empty()
                    && !matches!(q.filter, WhereFilter::None)
                    && !rows
                        .iter()
                        .any(|r| !matches!(q.filter.eval_row(r), EvalResult::NoMatch))
                {
                    skipped += 1;
                    self.stats.skipped.fetch_add(1, Relaxed);
                    debug!("skip {}", &qid[..8.min(qid.len())]);
                    to_requery.remove(qid);
                }
            });
        }

        if to_requery.is_empty() {
            return;
        }

        info!(
            "WAL {} tables → {} queries, {} skip",
            changes.len(),
            to_requery.len(),
            skipped
        );
        self.stats
            .requeries
            .fetch_add(to_requery.len() as u64, Relaxed);

        futures::stream::iter(to_requery)
            .for_each_concurrent(MAX_CONCURRENT, |qid| self.requery(qid, subs, nats))
            .await;
    }

    async fn requery(&self, qid: Arc<str>, subs: &SubscriptionManager, nats: &NatsHandler) {
        let Some(q) = subs.get_query(&qid) else {
            return;
        };
        let (sql, cols) = (q.query.clone(), q.cols.clone());
        let sub_ids: Vec<_> = q.subscribers.read().iter().cloned().collect();
        drop(q);

        if sub_ids.is_empty() {
            return;
        }

        let Ok(rows) = self.db.query_rows_typed(&sql).await else {
            return;
        };
        let Some(q) = subs.get_query(&qid) else {
            return;
        };

        let events = q.snap.write().diff_rows(rows, &cols);
        if events.is_empty() {
            return;
        }

        let (mut ev_subs, mut snap_subs) = (Vec::new(), Vec::new());
        for sid in sub_ids {
            if let Some(s) = subs.get_sub(&sid) {
                match s.mode {
                    SubscriptionMode::Events => ev_subs.push(sid),
                    SubscriptionMode::Snapshot => snap_subs.push(sid),
                }
            }
        }

        if ev_subs.is_empty() && snap_subs.is_empty() {
            return;
        }

        // Lazy: only read snapshot if we have snapshot subscribers
        let snap_rows = if !snap_subs.is_empty() {
            Some(q.snap.read().get_all_rows())
        } else {
            None
        };
        let Some(batch) = q.make_batch(events) else {
            return;
        };
        drop(q);

        // Batch publish: collect all messages and flush once (reduces syscalls)
        let mut messages: Vec<(&str, Bytes)> = Vec::with_capacity(ev_subs.len() + snap_subs.len());

        if !ev_subs.is_empty() {
            let bytes = Bytes::from(serde_json::to_vec(&batch).unwrap_or_default());
            for sid in &ev_subs {
                messages.push((sid.as_ref(), bytes.clone()));
            }
        }

        if let Some(rows) = snap_rows {
            let bytes = Bytes::from(
                serde_json::to_vec(
                    &serde_json::json!({ "seq": batch.seq, "ts": batch.ts, "rows": rows }),
                )
                .unwrap_or_default(),
            );
            for sid in &snap_subs {
                messages.push((sid.as_ref(), bytes.clone()));
            }
        }

        // Single flush for all messages
        let _ = nats.publish_batch(&messages).await;
    }
}
