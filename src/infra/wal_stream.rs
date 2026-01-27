//! Native PostgreSQL Logical Replication Streaming

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent, Lsn, TlsConfig};
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;
use tracing::{debug, error, info, trace};

use crate::core::{Config, SubscriptionManager};
use crate::core::query::{EvalResult, WhereFilter};
use crate::core::event::SubscriptionMode;
use crate::core::row::RowData;
use crate::infra::{DbPool, NatsHandler};
use crate::infra::pgoutput::{PgOutputDecoder, WalChange};

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
            cfg, db,
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
        
        loop {
            match self.stream_loop(&config, &subs, &nats).await {
                Ok(()) => info!("WAL stream ended, reconnecting..."),
                Err(e) => {
                    error!("WAL error: {e}, retry in 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    fn build_config(&self) -> Result<ReplicationConfig> {
        let url = &self.cfg.db_url;
        let stripped = url.trim_start_matches("postgres://").trim_start_matches("postgresql://");
        let parts: Vec<&str> = stripped.split('@').collect();
        
        let (user, password) = if parts.len() >= 2 {
            let auth: Vec<&str> = parts[0].split(':').collect();
            (auth.first().unwrap_or(&"postgres").to_string(),
             auth.get(1).map(|s| s.to_string()).unwrap_or_default())
        } else {
            ("postgres".into(), String::new())
        };
        
        let host_part = parts.last().unwrap_or(&"localhost:5432/postgres");
        let (host_port, db) = host_part.split_once('/').unwrap_or((host_part, "postgres"));
        let (host, port_str) = host_port.split_once(':').unwrap_or((host_port, "5432"));
        
        Ok(ReplicationConfig {
            host: host.into(),
            port: port_str.parse().unwrap_or(5432),
            user,
            password,
            database: db.into(),
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
        info!("WAL connected slot={} pub={}", self.cfg.wal_slot, self.cfg.wal_publication);
        
        let mut tx: FxHashMap<Arc<str>, Vec<RowData>> = FxHashMap::default();
        let mut in_tx = false;
        
        while let Some(event) = client.recv().await? {
            match event {
                ReplicationEvent::XLogData { wal_end, data, .. } => {
                    self.stats.processed.fetch_add(1, Relaxed);
                    
                    if let Some(change) = self.decoder.decode(&data) {
                        match change {
                            WalChange::Begin => { in_tx = true; tx.clear(); }
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
                                    if subs.has_table(t) { tx.entry(Arc::from(t)).or_default(); }
                                }
                            }
                            WalChange::Truncate { rels } => {
                                for r in rels {
                                    if let Some(t) = self.decoder.get_table(r) {
                                        if subs.has_table(t) { tx.entry(Arc::from(t)).or_default(); }
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
                if !to_requery.insert(qid.clone()) { return; }
                
                let Some(q) = subs.get_query(qid) else {
                    to_requery.remove(qid);
                    return;
                };
                
                // WHERE filter optimization
                if q.is_simple && !rows.is_empty() && !matches!(q.filter, WhereFilter::None) {
                    if !rows.iter().any(|r| !matches!(q.filter.eval_row(r), EvalResult::NoMatch)) {
                        skipped += 1;
                        self.stats.skipped.fetch_add(1, Relaxed);
                        debug!("skip {}", &qid[..8.min(qid.len())]);
                        to_requery.remove(qid);
                    }
                }
            });
        }
        
        if to_requery.is_empty() { return; }
        
        info!("WAL {} tables → {} queries, {} skip", changes.len(), to_requery.len(), skipped);
        self.stats.requeries.fetch_add(to_requery.len() as u64, Relaxed);
        
        futures::stream::iter(to_requery)
            .for_each_concurrent(MAX_CONCURRENT, |qid| self.requery(qid, subs, nats))
            .await;
    }

    async fn requery(&self, qid: Arc<str>, subs: &SubscriptionManager, nats: &NatsHandler) {
        let Some(q) = subs.get_query(&qid) else { return };
        let (sql, cols) = (q.query.clone(), q.cols.clone());
        let sub_ids: Vec<_> = q.subscribers.read().iter().cloned().collect();
        drop(q);
        
        if sub_ids.is_empty() { return; }
        
        let Ok(rows) = self.db.query_rows_typed(&sql).await else { return };
        let Some(q) = subs.get_query(&qid) else { return };
        
        let events = q.snap.write().diff_rows(rows, &cols);
        if events.is_empty() { return; }
        
        let (mut ev_subs, mut snap_subs) = (Vec::new(), Vec::new());
        for sid in sub_ids {
            if let Some(s) = subs.get_sub(&sid) {
                match s.mode {
                    SubscriptionMode::Events => ev_subs.push(sid),
                    SubscriptionMode::Snapshot => snap_subs.push(sid),
                }
            }
        }
        
        if ev_subs.is_empty() && snap_subs.is_empty() { return; }
        
        let snap_rows = (!snap_subs.is_empty()).then(|| q.snap.read().get_all_rows());
        let Some(batch) = q.make_batch(events) else { return };
        drop(q);
        
        if !ev_subs.is_empty() {
            let bytes = Bytes::from(serde_json::to_vec(&batch).unwrap_or_default());
            for sid in ev_subs { let _ = nats.publish_bytes(&sid, bytes.clone()).await; }
        }
        
        if let Some(rows) = snap_rows {
            let bytes = Bytes::from(serde_json::to_vec(&serde_json::json!({ "seq": batch.seq, "ts": batch.ts, "rows": rows })).unwrap_or_default());
            for sid in snap_subs { let _ = nats.publish_bytes(&sid, bytes.clone()).await; }
        }
    }
}
