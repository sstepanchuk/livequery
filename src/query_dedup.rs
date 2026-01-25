//! Query Deduplication - multiple clients share 1 subscription for identical queries.
//! Uses PostgreSQL md5() for hashing, regexp_replace() for normalization.

use pgrx::prelude::*;
use pgrx::lwlock::PgLwLock;
use pgrx::shmem::{PGRXSharedMemory, PgSharedMemoryInitialization};
use crate::shmem::MAX_SLOTS;

pub const MAX_QUERY_LEN: usize = 2048;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct QueryRegistryEntry {
    pub active: bool,
    pub query_hash: u64,
    pub query_normalized: [u8; MAX_QUERY_LEN],
    pub query_len: u16,
    pub slot_index: usize,
    pub client_count: u32,
}

impl Default for QueryRegistryEntry {
    fn default() -> Self {
        Self {
            active: false,
            query_hash: 0,
            query_normalized: [0u8; MAX_QUERY_LEN],
            query_len: 0,
            slot_index: 0,
            client_count: 0,
        }
    }
}

unsafe impl PGRXSharedMemory for QueryRegistryEntry {}

impl QueryRegistryEntry {
    #[inline]
    pub fn get_query(&self) -> String {
        String::from_utf8_lossy(&self.query_normalized[..self.query_len as usize]).into_owned()
    }
    
    #[inline]
    pub fn set_query(&mut self, query: &str) {
        let len = query.len().min(MAX_QUERY_LEN);
        self.query_normalized = [0u8; MAX_QUERY_LEN];
        self.query_normalized[..len].copy_from_slice(&query.as_bytes()[..len]);
        self.query_len = len as u16;
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct QueryRegistry {
    pub entries: [QueryRegistryEntry; MAX_SLOTS],
    pub total_deduplicated: u64,
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self {
            entries: [QueryRegistryEntry::default(); MAX_SLOTS],
            total_deduplicated: 0,
        }
    }
}

unsafe impl PGRXSharedMemory for QueryRegistry {}

static QUERY_REGISTRY: PgLwLock<QueryRegistry> = PgLwLock::new();

pub fn init_query_registry() {
    pgrx::pg_shmem_init!(QUERY_REGISTRY);
}

/// Normalize query using PostgreSQL regexp_replace + btrim
#[inline]
pub fn normalize_query(query: &str) -> String {
    Spi::get_one_with_args::<String>(
        "SELECT btrim(regexp_replace($1, E'\\\\s+', ' ', 'g'))",
        vec![(PgBuiltInOids::TEXTOID.oid(), query.into_datum())],
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| query.to_string())
}

/// Compute hash using PostgreSQL md5(), returns first 16 hex chars as u64
#[inline]
pub fn compute_query_hash(query: &str) -> u64 {
    let normalized = normalize_query(query);
    Spi::get_one_with_args::<String>(
        "SELECT md5($1)",
        vec![(PgBuiltInOids::TEXTOID.oid(), normalized.into_datum())],
    )
    .ok()
    .flatten()
    .and_then(|s| u64::from_str_radix(&s[..16.min(s.len())], 16).ok())
    .unwrap_or(0)
}

pub struct SubscriptionLookup {
    pub slot_index: usize,
    pub client_count: u32,
}

/// Find existing subscription by query hash, increment client count if found
pub fn find_existing_subscription(query: &str) -> Option<SubscriptionLookup> {
    let normalized = normalize_query(query);
    let query_hash = compute_query_hash(&normalized);
    
    // First check with shared lock
    let slot_idx = {
        let registry = QUERY_REGISTRY.share();
        registry.entries.iter().position(|e| 
            e.active && e.query_hash == query_hash && e.get_query() == normalized
        )
    }?;
    
    // Found - upgrade to exclusive and increment
    let mut registry = QUERY_REGISTRY.exclusive();
    registry.entries[slot_idx].client_count += 1;
    registry.total_deduplicated += 1;
    
    Some(SubscriptionLookup {
        slot_index: registry.entries[slot_idx].slot_index,
        client_count: registry.entries[slot_idx].client_count,
    })
}

/// Register new subscription in dedup registry
pub fn register_subscription(query: &str, slot_index: usize) {
    let normalized = normalize_query(query);
    let query_hash = compute_query_hash(&normalized);
    
    let mut registry = QUERY_REGISTRY.exclusive();
    
    if let Some(entry) = registry.entries.iter_mut().find(|e| !e.active) {
        entry.active = true;
        entry.query_hash = query_hash;
        entry.set_query(&normalized);
        entry.slot_index = slot_index;
        entry.client_count = 1;
    } else {
        pgrx::warning!("Query dedup registry full");
    }
}

/// Release client from subscription. Returns true if last client (cleanup needed)
pub fn release_subscription(slot_index: usize) -> bool {
    let mut registry = QUERY_REGISTRY.exclusive();
    
    if let Some(entry) = registry.entries.iter_mut().find(|e| e.active && e.slot_index == slot_index) {
        entry.client_count = entry.client_count.saturating_sub(1);
        if entry.client_count == 0 {
            entry.active = false;
            return true;
        }
        return false;
    }
    true
}

/// Get stats: (total_deduplicated, active_queries, total_clients)
pub fn get_dedup_stats() -> (u64, u32, u32) {
    let registry = QUERY_REGISTRY.share();
    let (active, clients) = registry.entries.iter()
        .filter(|e| e.active)
        .fold((0u32, 0u32), |(a, c), e| (a + 1, c + e.client_count));
    (registry.total_deduplicated, active, clients)
}

/// List active deduplicated queries: (query, slot_index, client_count)
pub fn list_deduplicated_queries() -> Vec<(String, usize, u32)> {
    QUERY_REGISTRY.share().entries.iter()
        .filter(|e| e.active)
        .map(|e| (e.get_query(), e.slot_index, e.client_count))
        .collect()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    
    #[pg_test]
    fn test_normalize_query() {
        assert_eq!(normalize_query("  SELECT   *   FROM   users  "), "SELECT * FROM users");
    }
    
    #[pg_test]
    fn test_query_hash_consistency() {
        let h1 = compute_query_hash("SELECT * FROM users");
        let h2 = compute_query_hash("SELECT  *  FROM  users");
        assert_eq!(h1, h2);
        assert_ne!(h1, compute_query_hash("SELECT * FROM orders"));
    }
}
