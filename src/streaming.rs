//! Streaming utilities
//!
//! Minimal utilities for streaming - timestamp and GUC only.
//! Main streaming logic is in unified_subscribe.rs

use pgrx::pg_sys;
use pgrx::GucSetting;

/// GUC: Heartbeat interval in milliseconds
pub static HEARTBEAT_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(1000);

/// Get current timestamp for events
pub fn get_current_timestamp() -> i64 {
    unsafe { pg_sys::GetCurrentTransactionStartTimestamp() }
}
