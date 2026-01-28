//! NATS Pub/Sub Handler - subscription-specific subjects

use anyhow::{Context, Result};
use async_nats::Client;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use tracing::{info, warn};

#[inline]
fn sub_id_from_subject<'a>(prefix: &str, subject: &'a str, tail: &str) -> Option<&'a str> {
    // Expected: {prefix}.{subscription_id}.{tail}
    let rest = subject
        .strip_prefix(prefix)?
        .strip_prefix('.')?
        .strip_suffix(tail)?
        .strip_suffix('.')?;
    if rest.is_empty() {
        None
    } else {
        Some(rest)
    }
}

use crate::core::event::*;
use crate::core::subscription::SubscriptionManager;
use crate::core::Config;
use crate::infra::DbPool;

#[derive(Clone)]
pub struct NatsHandler {
    pub nc: Client,
    cfg: Arc<Config>,
    subs: Arc<SubscriptionManager>,
    msgs_in: Arc<AtomicU64>,
    msgs_out: Arc<AtomicU64>,
}

impl NatsHandler {
    pub async fn connect(cfg: Arc<Config>, subs: Arc<SubscriptionManager>) -> Result<Self> {
        let nc = async_nats::connect(&cfg.nats_url)
            .await
            .context("NATS connect")?;
        Ok(Self {
            nc,
            cfg,
            subs,
            msgs_in: Arc::new(AtomicU64::new(0)),
            msgs_out: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn run(&self, db: Arc<DbPool>) -> Result<()> {
        // Subscribe to wildcard subjects: livequery.*.subscribe, etc.
        let prefix = self.cfg.nats_prefix.trim_end_matches('.');
        let mut sub_subscribe = self.nc.subscribe(format!("{}.*.subscribe", prefix)).await?;
        let mut sub_unsubscribe = self
            .nc
            .subscribe(format!("{}.*.unsubscribe", prefix))
            .await?;
        let mut sub_heartbeat = self.nc.subscribe(format!("{}.*.heartbeat", prefix)).await?;
        let mut sub_health = self.nc.subscribe(format!("{}.health", prefix)).await?;
        info!(
            "NATS listening on {}.*.{{subscribe|unsubscribe|heartbeat}} + {}.health",
            prefix, prefix
        );

        loop {
            tokio::select! {
                Some(m) = sub_subscribe.next() => {
                    self.msgs_in.fetch_add(1, Relaxed);
                    let sub_id = sub_id_from_subject(&self.cfg.nats_prefix, m.subject.as_str(), "subscribe");
                    let r = self.on_subscribe(&m.payload, &db, sub_id).await;
                    self.reply(&m.reply, &r).await;
                }
                Some(m) = sub_unsubscribe.next() => {
                    self.msgs_in.fetch_add(1, Relaxed);
                    let sub_id = sub_id_from_subject(&self.cfg.nats_prefix, m.subject.as_str(), "unsubscribe");
                    if let Some(id) = sub_id {
                        let ok = self.subs.unsubscribe(id);
                        self.reply(&m.reply, &serde_json::json!({"success": ok})).await;
                    } else {
                        self.reply(&m.reply, &serde_json::json!({"success": false, "error": "Missing subscription_id in subject"})).await;
                    }
                }
                Some(m) = sub_heartbeat.next() => {
                    self.msgs_in.fetch_add(1, Relaxed);
                    let sub_id = sub_id_from_subject(&self.cfg.nats_prefix, m.subject.as_str(), "heartbeat");
                    if let Some(id) = sub_id {
                        let ok = self.subs.heartbeat(id);
                        self.reply(&m.reply, &serde_json::json!({"success": ok})).await;
                    } else {
                        self.reply(&m.reply, &serde_json::json!({"success": false, "error": "Missing subscription_id in subject"})).await;
                    }
                }
                Some(m) = sub_health.next() => {
                    self.msgs_in.fetch_add(1, Relaxed);
                    let (subs, queries) = self.subs.stats();
                    let (msgs_in, msgs_out) = self.stats();
                    self.reply(&m.reply, &serde_json::json!({
                        "status": "healthy",
                        "server_id": self.cfg.server_id,
                        "subscriptions": subs,
                        "queries": queries,
                        "msgs_in": msgs_in,
                        "msgs_out": msgs_out
                    })).await;
                }
            }
        }
    }

    async fn on_subscribe(
        &self,
        payload: &[u8],
        db: &DbPool,
        subject_sub_id: Option<&str>,
    ) -> SubscribeResponse {
        let req: SubscribeRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(_) => return SubscribeResponse::err("Invalid request JSON"),
        };
        let sub_id = subject_sub_id
            .map(String::from)
            .unwrap_or(req.subscription_id);
        let (query, identity_columns, mode) = (req.query, req.identity_columns, req.mode);

        info!("Sub [{}] {:.60}", sub_id, query);

        // Subscribe with client-provided subscription_id
        let result = match self.subs.subscribe(&sub_id, &query, identity_columns, mode) {
            Ok(r) => r,
            Err(e) => return SubscribeResponse::err(&e),
        };

        // Subject format: livequery.{subscription_id}.events
        let subject = self.cfg.sub_events_subject(&sub_id);
        let sub_id = result.subscription_id.to_string();

        if result.is_new_query {
            // New query - execute and initialize snapshot
            let rows = match db.query_rows_typed(&query).await {
                Ok(r) => r,
                Err(e) => {
                    self.subs.unsubscribe(&sub_id);
                    return SubscribeResponse::err(&format!("Query failed: {e}"));
                }
            };

            let query = self.subs.get_query(&result.query_id);
            return match mode {
                SubscriptionMode::Events => {
                    let snapshot = query
                        .map(|q| q.snap.write().init_rows(rows, &q.cols))
                        .unwrap_or_default();
                    SubscribeResponse::ok_events(sub_id, subject, true, 0, snapshot)
                }
                SubscriptionMode::Snapshot => {
                    let rows = query
                        .map(|q| q.snap.write().init_rows_snapshot(rows, &q.cols))
                        .unwrap_or_default();
                    SubscribeResponse::ok_snapshot(sub_id, subject, true, 0, rows)
                }
            };
        }

        // Existing query - return current snapshot
        let Some(query) = self.subs.get_query(&result.query_id) else {
            return SubscribeResponse::err("Query not found");
        };
        let sub = self.subs.get_sub(&sub_id);
        let mode = sub.map(|s| s.mode).unwrap_or_default();

        match mode {
            SubscriptionMode::Events => {
                let rows = query.snap.read().get_all_rows();
                let snapshot: Vec<_> = rows
                    .into_iter()
                    .map(|d| SubscribeEvent::insert_arc(0, d))
                    .collect();
                SubscribeResponse::ok_events(sub_id, subject, false, result.seq, snapshot)
            }
            SubscriptionMode::Snapshot => {
                let rows = query.snap.read().get_all_rows();
                SubscribeResponse::ok_snapshot(sub_id, subject, false, result.seq, rows)
            }
        }
    }

    async fn reply<T: serde::Serialize>(&self, reply_to: &Option<async_nats::Subject>, data: &T) {
        if let Some(subj) = reply_to {
            if let Ok(bytes) = serde_json::to_vec(data) {
                self.msgs_out.fetch_add(1, Relaxed);
                if let Err(e) = self.nc.publish(subj.clone(), bytes.into()).await {
                    warn!("Reply error: {e}");
                }
            }
        }
    }

    /// Publish pre-serialized bytes to subscription subject (zero-copy)
    #[inline]
    pub async fn publish_bytes(&self, sub_id: &str, bytes: Bytes) -> Result<()> {
        let subject = self.cfg.sub_events_subject(sub_id);
        self.msgs_out.fetch_add(1, Relaxed);
        self.nc.publish(subject, bytes).await?;
        Ok(())
    }

    /// Batch publish - accumulate messages and flush once (reduces syscalls)
    #[inline]
    pub async fn publish_batch(&self, messages: &[(&str, Bytes)]) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }
        for (sub_id, bytes) in messages {
            let subject = self.cfg.sub_events_subject(sub_id);
            self.nc.publish(subject, bytes.clone()).await?;
        }
        self.msgs_out.fetch_add(messages.len() as u64, Relaxed);
        self.nc.flush().await?;
        Ok(())
    }

    /// Get stats: (messages_in, messages_out)
    #[inline]
    pub fn stats(&self) -> (u64, u64) {
        (self.msgs_in.load(Relaxed), self.msgs_out.load(Relaxed))
    }
}
