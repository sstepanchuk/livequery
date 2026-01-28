//! Event Types - Materialize-compatible format

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Subscription delivery mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionMode {
    /// Deliver individual INSERT/UPDATE/DELETE events (default)
    #[default]
    Events,
    /// Deliver full snapshot on every change
    Snapshot,
}

/// Single change event (insert=+1, delete=-1)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeEvent {
    pub mz_timestamp: i64,
    pub mz_diff: i8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Arc<Value>>,
}

#[allow(dead_code)]
impl SubscribeEvent {
    #[inline(always)]
    pub fn insert(t: i64, d: Value) -> Self {
        Self {
            mz_timestamp: t,
            mz_diff: 1,
            data: Some(Arc::new(d)),
        }
    }
    #[inline(always)]
    pub fn delete(t: i64, d: Value) -> Self {
        Self {
            mz_timestamp: t,
            mz_diff: -1,
            data: Some(Arc::new(d)),
        }
    }
    #[inline(always)]
    pub fn insert_arc(t: i64, d: Arc<Value>) -> Self {
        Self {
            mz_timestamp: t,
            mz_diff: 1,
            data: Some(d),
        }
    }
    #[inline(always)]
    pub fn delete_arc(t: i64, d: Arc<Value>) -> Self {
        Self {
            mz_timestamp: t,
            mz_diff: -1,
            data: Some(d),
        }
    }
}

/// Batch of events with sequence number and server timestamp
#[derive(Debug, Clone, Serialize, Default)]
pub struct EventBatch {
    pub seq: u64,
    /// Server timestamp in milliseconds (for client latency calculation)
    pub ts: u64,
    pub events: Vec<SubscribeEvent>,
}

impl EventBatch {
    #[inline(always)]
    pub fn new(seq: u64, events: Vec<SubscribeEvent>) -> Self {
        Self {
            seq,
            ts: ts_millis(),
            events,
        }
    }
}

/// Current time in milliseconds since UNIX epoch
#[inline(always)]
pub fn ts_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// === Request Types ===

#[derive(Debug, Deserialize)]
pub struct SubscribeRequest {
    pub subscription_id: String,
    pub query: String,
    #[serde(default)]
    pub identity_columns: Option<Vec<String>>,
    #[serde(default)]
    pub mode: SubscriptionMode,
}

// === Response Types ===

#[derive(Debug, Serialize, Default)]
pub struct SubscribeResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub is_new: bool,
    pub seq: u64,
    pub mode: SubscriptionMode,
    /// Initial snapshot for events mode
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub snapshot: Vec<SubscribeEvent>,
    /// Initial data for snapshot mode
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub rows: Vec<Arc<Value>>,
}

impl SubscribeResponse {
    /// Events mode response with diff events
    #[inline]
    pub fn ok_events(
        sub_id: String,
        subject: String,
        is_new: bool,
        seq: u64,
        snapshot: Vec<SubscribeEvent>,
    ) -> Self {
        Self {
            success: true,
            subscription_id: Some(sub_id),
            subject: Some(subject),
            error: None,
            is_new,
            seq,
            mode: SubscriptionMode::Events,
            snapshot,
            rows: vec![],
        }
    }
    /// Snapshot mode response with full rows
    #[inline]
    pub fn ok_snapshot(
        sub_id: String,
        subject: String,
        is_new: bool,
        seq: u64,
        rows: Vec<Arc<Value>>,
    ) -> Self {
        Self {
            success: true,
            subscription_id: Some(sub_id),
            subject: Some(subject),
            error: None,
            is_new,
            seq,
            mode: SubscriptionMode::Snapshot,
            snapshot: vec![],
            rows,
        }
    }
    #[inline]
    pub fn err(msg: &str) -> Self {
        Self {
            success: false,
            error: Some(msg.into()),
            ..Default::default()
        }
    }
}
