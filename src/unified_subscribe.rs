//! Unified Reactive Subscribe - Snapshot-Diff pattern for ANY SQL query.

use pgrx::prelude::*;
use pgrx::pg_sys;
use std::collections::HashMap;
use std::ffi::CStr;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use crate::event::SubscribeEvent;
use crate::query_analyzer::analyze_query;
use crate::query_dedup;
use crate::shmem;
use crate::streaming::{get_current_timestamp, HEARTBEAT_INTERVAL_MS};
use crate::types::datum_to_json;

#[inline]
fn row_to_json(row: &pgrx::spi::SpiHeapTupleData) -> Option<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for i in 1..=row.columns() {
        let col_name = get_column_name(i).unwrap_or_else(|| format!("col_{}", i));
        let type_oid = row.get_datum_by_ordinal(i).map(|e| e.oid()).unwrap_or(pg_sys::InvalidOid);
        map.insert(col_name, datum_to_json(row, i, type_oid));
    }
    if map.is_empty() { None } else { Some(serde_json::Value::Object(map)) }
}

#[inline]
fn get_column_name(ordinal: usize) -> Option<String> {
    unsafe {
        let tuptable = pg_sys::SPI_tuptable;
        if tuptable.is_null() { return None; }
        let tupdesc = (*tuptable).tupdesc;
        if tupdesc.is_null() { return None; }
        let name_ptr = pg_sys::SPI_fname(tupdesc, ordinal as i32);
        if name_ptr.is_null() { return None; }
        let name = CStr::from_ptr(name_ptr).to_str().ok()?.to_string();
        pg_sys::pfree(name_ptr as *mut _);
        Some(name)
    }
}

/// Snapshot state - stores current query results keyed by row identity
pub struct Snapshot {
    rows: HashMap<String, serde_json::Value>,
    identity_columns: Option<Vec<String>>,
}

impl Snapshot {
    pub fn new(identity_columns: Option<Vec<String>>) -> Self {
        Self {
            rows: HashMap::new(),
            identity_columns,
        }
    }

    #[inline]
    fn compute_identity(&self, row: &serde_json::Value) -> String {
        if let Some(cols) = &self.identity_columns {
            if !cols.is_empty() {
                return cols.iter()
                    .filter_map(|col| row.get(col).map(|v| v.to_string()))
                    .collect::<Vec<_>>()
                    .join("|");
            }
        }
        let mut hasher = DefaultHasher::new();
        row.to_string().hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    pub fn execute_and_diff(&mut self, query: &str) -> Result<Vec<SubscribeEvent>, String> {
        let timestamp = get_current_timestamp();
        let mut new_rows: HashMap<String, serde_json::Value> = HashMap::new();

        // Execute query
        Spi::connect(|client| {
            match client.select(query, None, None) {
                Ok(table) => {
                    for row in table {
                        if let Some(json) = row_to_json(&row) {
                            let id = self.compute_identity(&json);
                            new_rows.insert(id, json);
                        }
                    }
                    Ok(())
                }
                Err(e) => Err(format!("Query failed: {:?}", e)),
            }
        })?;

        let mut events = Vec::new();

        // DELETED: in old but not in new
        events.extend(self.rows.iter()
            .filter(|(id, _)| !new_rows.contains_key(*id))
            .map(|(_, row)| SubscribeEvent::delete(timestamp, row.clone())));

        // INSERTED/UPDATED
        for (id, new_row) in &new_rows {
            match self.rows.get(id) {
                None => events.push(SubscribeEvent::insert(timestamp, new_row.clone())),
                Some(old) if old != new_row => {
                    events.push(SubscribeEvent::delete(timestamp, old.clone()));
                    events.push(SubscribeEvent::insert(timestamp, new_row.clone()));
                }
                _ => {}
            }
        }

        self.rows = new_rows;
        Ok(events)
    }
}

pub struct UnifiedSubscription {
    query: String,
    tables: Vec<String>,
    snapshot: Snapshot,
    slot_index: usize,
}

impl UnifiedSubscription {
    pub fn new(query: &str, identity_columns: Option<Vec<String>>) -> Result<Self, String> {
        let analysis = analyze_query(query);
        if !analysis.is_valid {
            return Err(analysis.incompatibility_reason.unwrap_or("Invalid SQL".into()));
        }
        if analysis.referenced_tables.is_empty() {
            return Err("Query must reference at least one table".into());
        }

        // Try reuse existing subscription
        if let Some(existing) = query_dedup::find_existing_subscription(query) {
            pgrx::info!("pg_subscribe: Reusing slot {}, {} clients", existing.slot_index, existing.client_count);
            return Ok(Self {
                query: query.into(),
                tables: Self::extract_table_names(&analysis),
                snapshot: Snapshot::new(identity_columns),
                slot_index: existing.slot_index,
            });
        }

        // New subscription
        let slot_index = shmem::allocate_slot(&uuid::Uuid::new_v4().to_string())
            .ok_or("No available subscription slots")?;
        query_dedup::register_subscription(query, slot_index);

        Ok(Self {
            query: query.into(),
            tables: Self::extract_table_names(&analysis),
            snapshot: Snapshot::new(identity_columns),
            slot_index,
        })
    }

    #[inline]
    fn extract_table_names(analysis: &crate::query_analyzer::QueryAnalysis) -> Vec<String> {
        analysis.referenced_tables.iter()
            .map(|t| t.schema.as_ref().map(|s| format!("{}.{}", s, t.table)).unwrap_or_else(|| t.table.clone()))
            .collect()
    }

    pub fn initialize(&mut self) -> Result<Vec<SubscribeEvent>, String> {
        for table in &self.tables {
            if let Err(e) = crate::trigger::register_subscription_for_table(table, self.slot_index) {
                pgrx::warning!("Trigger registration failed for {}: {}", table, e);
            }
        }
        self.snapshot.execute_and_diff(&self.query)
    }

    #[inline]
    pub fn check_changes(&mut self) -> Result<Vec<SubscribeEvent>, String> {
        self.snapshot.execute_and_diff(&self.query)
    }
}

impl Drop for UnifiedSubscription {
    fn drop(&mut self) {
        // Release from dedup registry - returns true if we should fully cleanup
        let should_cleanup = query_dedup::release_subscription(self.slot_index);
        
        if should_cleanup {
            // Last client - cleanup triggers and release slot
            if let Err(e) = crate::trigger::cleanup_shared_triggers_for_slot(self.slot_index) {
                pgrx::warning!("Trigger cleanup failed: {}", e);
            }
            shmem::release_slot(self.slot_index);
        }
        // If not should_cleanup, other clients are still using this subscription
    }
}

pub struct UnifiedEventIterator {
    subscription: UnifiedSubscription,
    pending: Vec<SubscribeEvent>,
    initialized: bool,
}

impl UnifiedEventIterator {
    pub fn new(subscription: UnifiedSubscription) -> Self {
        Self { subscription, pending: Vec::new(), initialized: false }
    }
}

impl Iterator for UnifiedEventIterator {
    type Item = SubscribeEvent;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(event) = self.pending.pop() {
            return Some(event);
        }

        if !self.initialized {
            self.initialized = true;
            if let Ok(mut events) = self.subscription.initialize() {
                events.reverse();
                self.pending = events;
                return self.pending.pop();
            }
        }

        loop {
            unsafe {
                if pg_sys::InterruptPending != 0 {
                    pg_sys::ProcessInterrupts();
                    return None;
                }
            }

            if shmem::pop_event(self.subscription.slot_index).is_some() {
                if let Ok(mut events) = self.subscription.check_changes() {
                    if !events.is_empty() {
                        events.reverse();
                        self.pending = events;
                        return self.pending.pop();
                    }
                }
            }

            unsafe {
                let result = pg_sys::WaitLatch(
                    pg_sys::MyLatch,
                    (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH) as i32,
                    HEARTBEAT_INTERVAL_MS.get() as i64,
                    pg_sys::PG_WAIT_EXTENSION,
                );
                if (result & pg_sys::WL_LATCH_SET as i32) != 0 {
                    pg_sys::ResetLatch(pg_sys::MyLatch);
                }
                if (result & pg_sys::WL_POSTMASTER_DEATH as i32) != 0 {
                    return None;
                }
            }
        }
    }
}

pub fn create_unified_subscription(
    query: &str,
    identity_columns: Option<Vec<String>>,
) -> TableIterator<
    'static,
    (
        name!(mz_timestamp, i64),
        name!(mz_diff, i32),
        name!(mz_progressed, bool),
        name!(data, pgrx::JsonB),
    ),
> {
    match UnifiedSubscription::new(query, identity_columns) {
        Ok(sub) => {
            let iter = UnifiedEventIterator::new(sub);
            TableIterator::new(iter.map(|event| {
                (
                    event.mz_timestamp,
                    event.mz_diff,
                    event.mz_progressed,
                    event.data_as_jsonb(),
                )
            }))
        }
        Err(e) => {
            pgrx::error!("Failed to create subscription: {}", e);
        }
    }
}

pub fn create_snapshot_subscription(
    query: &str,
    identity_columns: Option<Vec<String>>,
) -> TableIterator<
    'static,
    (
        name!(mz_timestamp, i64),
        name!(mz_diff, i32),
        name!(mz_progressed, bool),
        name!(data, pgrx::JsonB),
    ),
> {
    let mut snapshot = Snapshot::new(identity_columns);
    
    match snapshot.execute_and_diff(query) {
        Ok(events) => {
            TableIterator::new(events.into_iter().map(|event| {
                (
                    event.mz_timestamp,
                    event.mz_diff,
                    event.mz_progressed,
                    event.data_as_jsonb(),
                )
            }))
        }
        Err(e) => {
            pgrx::error!("Snapshot failed: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_computation_with_columns() {
        let snapshot = Snapshot::new(Some(vec!["id".to_string(), "user_id".to_string()]));
        let row = serde_json::json!({"id": 1, "user_id": 5, "name": "test"});
        let id = snapshot.compute_identity(&row);
        assert_eq!(id, "1|5");
    }

    #[test]
    fn test_identity_computation_hash() {
        let snapshot = Snapshot::new(None);
        let row1 = serde_json::json!({"id": 1, "name": "test"});
        let row2 = serde_json::json!({"id": 1, "name": "test"});
        let row3 = serde_json::json!({"id": 2, "name": "other"});

        assert_eq!(snapshot.compute_identity(&row1), snapshot.compute_identity(&row2));
        assert_ne!(snapshot.compute_identity(&row1), snapshot.compute_identity(&row3));
    }
}
