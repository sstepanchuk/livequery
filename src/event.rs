//! Subscribe Event - Materialize-compatible event structure

use serde::{Deserialize, Serialize};

/// A single event in the subscription stream
/// 
/// Following Materialize semantics:
/// - `mz_timestamp`: Logical timestamp
/// - `mz_diff`: +1 for insert, -1 for delete
/// - `mz_progressed`: Heartbeat flag
/// - `data`: Row data as JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeEvent {
    pub mz_timestamp: i64,
    pub mz_diff: i32,
    pub mz_progressed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl SubscribeEvent {
    /// Create INSERT event (+1)
    pub fn insert(timestamp: i64, data: serde_json::Value) -> Self {
        Self { mz_timestamp: timestamp, mz_diff: 1, mz_progressed: false, data: Some(data) }
    }
    
    /// Create DELETE event (-1)
    pub fn delete(timestamp: i64, data: serde_json::Value) -> Self {
        Self { mz_timestamp: timestamp, mz_diff: -1, mz_progressed: false, data: Some(data) }
    }
    
    /// Create progress/heartbeat event
    pub fn progress(timestamp: i64) -> Self {
        Self { mz_timestamp: timestamp, mz_diff: 0, mz_progressed: true, data: None }
    }
    
    /// Get data as JsonB for PostgreSQL return
    pub fn data_as_jsonb(&self) -> pgrx::JsonB {
        pgrx::JsonB(self.data.clone().unwrap_or(serde_json::json!({})))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_event_creation() {
        let insert = SubscribeEvent::insert(100, serde_json::json!({"id": 1}));
        assert_eq!(insert.mz_diff, 1);
        assert!(!insert.mz_progressed);
        
        let delete = SubscribeEvent::delete(100, serde_json::json!({"id": 1}));
        assert_eq!(delete.mz_diff, -1);
        
        let progress = SubscribeEvent::progress(100);
        assert_eq!(progress.mz_diff, 0);
        assert!(progress.mz_progressed);
        assert!(progress.data.is_none());
    }
}
