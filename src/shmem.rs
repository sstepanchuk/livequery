//! PostgreSQL Shared Memory Implementation
//!
//! This module implements TRUE PostgreSQL shared memory using pgrx primitives.
//! Data is shared across ALL backend processes.
//!
//! Requirements:
//! - Extension must be loaded via shared_preload_libraries in postgresql.conf
//! - PostgreSQL restart required after configuration change

use pgrx::prelude::*;
use pgrx::pg_sys;
use pgrx::pg_shmem_init;
use pgrx::lwlock::PgLwLock;
use pgrx::atomics::PgAtomic;
use pgrx::shmem::{PGRXSharedMemory, PgSharedMemoryInitialization};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::event::SubscribeEvent;

// ============================================================================
// Constants
// ============================================================================

/// Maximum number of concurrent subscriptions
/// Supports up to 64 (limited by bitmap in TableRegistry)
pub const MAX_SLOTS: usize = 64;

/// Maximum events per slot buffer
pub const MAX_EVENTS_PER_SLOT: usize = 32;

/// Maximum payload size per event (bytes)
pub const MAX_EVENT_PAYLOAD: usize = 1024;

/// Maximum number of tables that can have shared triggers
pub const MAX_TRACKED_TABLES: usize = 32;

/// Maximum table name length
pub const MAX_TABLE_NAME_LEN: usize = 128;

// ============================================================================
// Shared Memory Structures
// ============================================================================

/// Single event in shared memory (fixed size for heapless)
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ShmemEvent {
    /// Timestamp
    pub timestamp: i64,
    /// Diff (+1 insert, -1 delete, 0 progress)
    pub diff: i32,
    /// Payload length
    pub payload_len: u32,
    /// JSON payload (fixed size buffer)
    pub payload: [u8; MAX_EVENT_PAYLOAD],
    /// Is this slot occupied
    pub occupied: bool,
}

impl Default for ShmemEvent {
    fn default() -> Self {
        Self {
            timestamp: 0,
            diff: 0,
            payload_len: 0,
            payload: [0u8; MAX_EVENT_PAYLOAD],
            occupied: false,
        }
    }
}

unsafe impl PGRXSharedMemory for ShmemEvent {}

impl ShmemEvent {
    pub fn from_subscribe_event(event: &SubscribeEvent) -> Self {
        let json = serde_json::to_string(&event.data).unwrap_or_default();
        let json_bytes = json.as_bytes();
        let len = std::cmp::min(json_bytes.len(), MAX_EVENT_PAYLOAD);
        
        let mut payload = [0u8; MAX_EVENT_PAYLOAD];
        payload[..len].copy_from_slice(&json_bytes[..len]);
        
        Self {
            timestamp: event.mz_timestamp,
            diff: event.mz_diff,
            payload_len: len as u32,
            payload,
            occupied: true,
        }
    }
    
    pub fn to_subscribe_event(&self) -> Option<SubscribeEvent> {
        if !self.occupied {
            return None;
        }
        
        let json_str = std::str::from_utf8(&self.payload[..self.payload_len as usize]).ok()?;
        let data: Option<serde_json::Value> = serde_json::from_str(json_str).ok();
        
        Some(SubscribeEvent {
            mz_timestamp: self.timestamp,
            mz_diff: self.diff,
            mz_progressed: self.diff == 0,
            data,
        })
    }
}

/// Subscription slot info
#[derive(Clone, Copy)]
#[repr(C)]
pub struct SlotInfo {
    /// Is slot active
    pub active: bool,
    /// Subscription ID (UUID as bytes)
    pub subscription_id: [u8; 36],
    /// Backend PID
    pub backend_pid: u32,
    /// Created timestamp
    pub created_at: u64,
    /// Events sent count
    pub events_sent: u64,
    /// Ring buffer head (write position)
    pub head: usize,
    /// Ring buffer tail (read position)  
    pub tail: usize,
    /// Number of events in buffer
    pub count: usize,
}

impl Default for SlotInfo {
    fn default() -> Self {
        Self {
            active: false,
            subscription_id: [0u8; 36],
            backend_pid: 0,
            created_at: 0,
            events_sent: 0,
            head: 0,
            tail: 0,
            count: 0,
        }
    }
}

unsafe impl PGRXSharedMemory for SlotInfo {}

impl SlotInfo {
    pub fn get_subscription_id(&self) -> String {
        let end = self.subscription_id.iter().position(|&b| b == 0).unwrap_or(36);
        String::from_utf8_lossy(&self.subscription_id[..end]).to_string()
    }
    
    pub fn set_subscription_id(&mut self, id: &str) {
        let bytes = id.as_bytes();
        let len = std::cmp::min(bytes.len(), 36);
        self.subscription_id = [0u8; 36];
        self.subscription_id[..len].copy_from_slice(&bytes[..len]);
    }
}

/// Global statistics
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct GlobalStats {
    pub total_subscriptions: u64,
    pub total_events: u64,
    pub active_slots: u32,
}

unsafe impl PGRXSharedMemory for GlobalStats {}

/// Table subscription registry - tracks which slots are interested in which tables
/// This enables shared triggers (1 per table) with fan-out to all interested subscriptions
#[derive(Clone, Copy)]
#[repr(C)]
pub struct TableRegistry {
    /// Table name (null-terminated)
    pub table_name: [u8; MAX_TABLE_NAME_LEN],
    /// Is this entry active
    pub active: bool,
    /// Bitmap of interested slot indices (supports up to 64 slots)
    pub interested_slots: u64,
    /// Trigger installed flag
    pub trigger_installed: bool,
}

impl Default for TableRegistry {
    fn default() -> Self {
        Self {
            table_name: [0u8; MAX_TABLE_NAME_LEN],
            active: false,
            interested_slots: 0,
            trigger_installed: false,
        }
    }
}

unsafe impl PGRXSharedMemory for TableRegistry {}

impl TableRegistry {
    pub fn get_table_name(&self) -> String {
        let end = self.table_name.iter().position(|&b| b == 0).unwrap_or(MAX_TABLE_NAME_LEN);
        String::from_utf8_lossy(&self.table_name[..end]).to_string()
    }
    
    pub fn set_table_name(&mut self, name: &str) {
        let bytes = name.as_bytes();
        let len = std::cmp::min(bytes.len(), MAX_TABLE_NAME_LEN - 1);
        self.table_name = [0u8; MAX_TABLE_NAME_LEN];
        self.table_name[..len].copy_from_slice(&bytes[..len]);
    }
    
    /// Add a slot to the interested list
    pub fn add_slot(&mut self, slot_index: usize) {
        if slot_index < 64 {
            self.interested_slots |= 1u64 << slot_index;
        }
    }
    
    /// Remove a slot from the interested list
    pub fn remove_slot(&mut self, slot_index: usize) {
        if slot_index < 64 {
            self.interested_slots &= !(1u64 << slot_index);
        }
    }
    
    /// Check if a slot is interested
    pub fn is_slot_interested(&self, slot_index: usize) -> bool {
        if slot_index < 64 {
            (self.interested_slots & (1u64 << slot_index)) != 0
        } else {
            false
        }
    }
    
    pub fn get_interested_slots(&self) -> Vec<usize> {
        (0..MAX_SLOTS).filter(|&i| self.is_slot_interested(i)).collect()
    }
    
    /// Check if any slots are interested
    pub fn has_interested_slots(&self) -> bool {
        self.interested_slots != 0
    }
}

/// All table registries
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AllTableRegistries {
    pub tables: [TableRegistry; MAX_TRACKED_TABLES],
}

impl Default for AllTableRegistries {
    fn default() -> Self {
        Self {
            tables: [TableRegistry::default(); MAX_TRACKED_TABLES],
        }
    }
}

unsafe impl PGRXSharedMemory for AllTableRegistries {}

/// Single subscription slot with its event buffer
#[derive(Clone, Copy)]
#[repr(C)]
pub struct SubscriptionSlotData {
    pub info: SlotInfo,
    pub events: [ShmemEvent; MAX_EVENTS_PER_SLOT],
}

impl Default for SubscriptionSlotData {
    fn default() -> Self {
        Self {
            info: SlotInfo::default(),
            events: [ShmemEvent::default(); MAX_EVENTS_PER_SLOT],
        }
    }
}

unsafe impl PGRXSharedMemory for SubscriptionSlotData {}

/// All subscription slots
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AllSlots {
    pub slots: [SubscriptionSlotData; MAX_SLOTS],
    pub stats: GlobalStats,
}

impl Default for AllSlots {
    fn default() -> Self {
        Self {
            slots: [SubscriptionSlotData::default(); MAX_SLOTS],
            stats: GlobalStats::default(),
        }
    }
}

unsafe impl PGRXSharedMemory for AllSlots {}


// ============================================================================
// Static Shared Memory Declarations
// ============================================================================

/// Main shared memory structure protected by LwLock
static SHMEM_SLOTS: PgLwLock<AllSlots> = PgLwLock::new();

/// Table registry for shared triggers
static TABLE_REGISTRY: PgLwLock<AllTableRegistries> = PgLwLock::new();

/// Atomic counter for fast stats (no lock needed)
static EVENTS_COUNTER: PgAtomic<AtomicU64> = PgAtomic::new();

use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
static SHMEM_INITIALIZED: AtomicBool = AtomicBool::new(false);

// ============================================================================
// Initialization
// ============================================================================

/// Initialize shared memory - call from _PG_init()
pub fn init_shmem() {
    // Check if we're being loaded via shared_preload_libraries
    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        // Not in shared_preload_libraries - use fallback mode
        pgrx::warning!(
            "pg_subscribe: Not loaded via shared_preload_libraries. \
             Cross-backend communication will be limited. \
             Add to postgresql.conf: shared_preload_libraries = 'pg_subscribe'"
        );
        return;
    }
    
    // Register shared memory
    pg_shmem_init!(SHMEM_SLOTS);
    pg_shmem_init!(TABLE_REGISTRY);
    pg_shmem_init!(EVENTS_COUNTER);
    
    SHMEM_INITIALIZED.store(true, AtomicOrdering::SeqCst);
    
    pgrx::info!("pg_subscribe: Shared memory initialized ({} slots, {} events/slot)", 
                MAX_SLOTS, MAX_EVENTS_PER_SLOT);
}

/// Check if proper shared memory is available
#[inline]
pub fn is_shmem_available() -> bool {
    SHMEM_INITIALIZED.load(AtomicOrdering::SeqCst)
}

// ============================================================================
// Public API
// ============================================================================

/// Allocate a subscription slot
pub fn allocate_slot(subscription_id: &str) -> Option<usize> {
    if !is_shmem_available() {
        pgrx::warning!("pg_subscribe: Shared memory not available");
        return None;
    }
    
    let backend_pid = unsafe { pg_sys::MyProcPid as u32 };
    let mut slots = SHMEM_SLOTS.exclusive();
    
    let idx = slots.slots.iter().position(|s| !s.info.active)?;
    
    let slot = &mut slots.slots[idx];
    slot.info = SlotInfo {
        active: true,
        backend_pid,
        created_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        ..Default::default()
    };
    slot.info.set_subscription_id(subscription_id);
    slot.events.iter_mut().for_each(|e| e.occupied = false);
    
    slots.stats.total_subscriptions += 1;
    slots.stats.active_slots += 1;
    
    Some(idx)
}

/// Release a subscription slot
pub fn release_slot(slot_index: usize) {
    if !is_shmem_available() || slot_index >= MAX_SLOTS {
        return;
    }
    
    let mut slots = SHMEM_SLOTS.exclusive();
    if slots.slots[slot_index].info.active {
        slots.slots[slot_index].info.active = false;
        slots.stats.active_slots = slots.stats.active_slots.saturating_sub(1);
    }
}

/// Push an event to a slot's buffer
pub fn push_event(slot_index: usize, event: &SubscribeEvent) -> bool {
    if !is_shmem_available() || slot_index >= MAX_SLOTS {
        return false;
    }
    
    let mut slots = SHMEM_SLOTS.exclusive();
    let slot = &mut slots.slots[slot_index];
    
    if !slot.info.active {
        return false;
    }
    
    // Check if buffer is full
    if slot.info.count >= MAX_EVENTS_PER_SLOT {
        // Overwrite oldest (move tail)
        slot.info.tail = (slot.info.tail + 1) % MAX_EVENTS_PER_SLOT;
        slot.info.count -= 1;
    }
    
    // Write event at head
    slot.events[slot.info.head] = ShmemEvent::from_subscribe_event(event);
    slot.info.head = (slot.info.head + 1) % MAX_EVENTS_PER_SLOT;
    slot.info.count += 1;
    slot.info.events_sent += 1;
    
    // Update global counter (atomic, no lock needed after release)
    drop(slots);
    EVENTS_COUNTER.get().fetch_add(1, Ordering::Relaxed);
    
    true
}

/// Pop an event from a slot's buffer
pub fn pop_event(slot_index: usize) -> Option<SubscribeEvent> {
    if !is_shmem_available() || slot_index >= MAX_SLOTS {
        return None;
    }
    
    let mut slots = SHMEM_SLOTS.exclusive();
    let slot = &mut slots.slots[slot_index];
    
    if !slot.info.active || slot.info.count == 0 {
        return None;
    }
    
    // Read event at tail
    let event = slot.events[slot.info.tail].to_subscribe_event();
    slot.events[slot.info.tail].occupied = false;
    slot.info.tail = (slot.info.tail + 1) % MAX_EVENTS_PER_SLOT;
    slot.info.count -= 1;
    
    event
}

/// Get slot info (used by trigger for NOTIFY channel)
pub fn get_slot_info(slot_index: usize) -> Option<SlotInfo> {
    if !is_shmem_available() || slot_index >= MAX_SLOTS {
        return None;
    }
    
    let slots = SHMEM_SLOTS.share();
    if slots.slots[slot_index].info.active {
        Some(slots.slots[slot_index].info)
    } else {
        None
    }
}

/// Get global statistics
pub fn get_stats() -> (u64, u64, u32) {
    if !is_shmem_available() {
        return (0, 0, 0);
    }
    
    let slots = SHMEM_SLOTS.share();
    let total_events = EVENTS_COUNTER.get().load(Ordering::Relaxed);
    
    (slots.stats.total_subscriptions, total_events, slots.stats.active_slots)
}

/// Get statistics as table iterator (for pg_subscribe_stats())
pub fn get_statistics() -> TableIterator<'static, (name!(stat_name, String), name!(stat_value, i64))> {
    let (total_subs, total_events, active_subs) = get_stats();
    let shmem_available = is_shmem_available();
    
    // Get dedup stats
    let (total_deduped, active_queries, total_clients) = crate::query_dedup::get_dedup_stats();
    
    let stats = vec![
        ("shmem_available".to_string(), shmem_available as i64),
        ("total_subscriptions_created".to_string(), total_subs as i64),
        ("total_events_sent".to_string(), total_events as i64),
        ("active_subscriptions".to_string(), active_subs as i64),
        ("max_slots".to_string(), MAX_SLOTS as i64),
        ("max_events_per_slot".to_string(), MAX_EVENTS_PER_SLOT as i64),
        ("dedup_total_reused".to_string(), total_deduped as i64),
        ("dedup_unique_queries".to_string(), active_queries as i64),
        ("dedup_total_clients".to_string(), total_clients as i64),
    ];
    
    TableIterator::new(stats)
}

// ============================================================================
// Table Registry API (for Shared Triggers)
// ============================================================================

/// Register a subscription slot's interest in a table
/// Returns true if this is a NEW table (trigger needs to be installed)
pub fn register_table_interest(table_name: &str, slot_index: usize) -> bool {
    if !is_shmem_available() || slot_index >= MAX_SLOTS { return false; }
    
    let mut registry = TABLE_REGISTRY.exclusive();
    
    // Check if table already exists
    if let Some(t) = registry.tables.iter_mut().find(|t| t.active && t.get_table_name() == table_name) {
        t.add_slot(slot_index);
        return !t.trigger_installed;
    }
    
    // Create new entry
    if let Some(t) = registry.tables.iter_mut().find(|t| !t.active) {
        t.active = true;
        t.set_table_name(table_name);
        t.add_slot(slot_index);
        t.trigger_installed = false;
        return true;
    }
    
    pgrx::warning!("pg_subscribe: Table registry full");
    false
}

pub fn mark_trigger_installed(table_name: &str) {
    if !is_shmem_available() { return; }
    let mut registry = TABLE_REGISTRY.exclusive();
    if let Some(t) = registry.tables.iter_mut().find(|t| t.active && t.get_table_name() == table_name) {
        t.trigger_installed = true;
    }
}

pub fn is_trigger_installed(table_name: &str) -> bool {
    if !is_shmem_available() { return false; }
    TABLE_REGISTRY.share().tables.iter()
        .find(|t| t.active && t.get_table_name() == table_name)
        .map(|t| t.trigger_installed)
        .unwrap_or(false)
}


pub fn unregister_slot_from_all_tables(slot_index: usize) -> Vec<String> {
    if !is_shmem_available() || slot_index >= MAX_SLOTS { return Vec::new(); }
    
    let mut registry = TABLE_REGISTRY.exclusive();
    registry.tables.iter_mut()
        .filter(|t| t.active && t.is_slot_interested(slot_index))
        .filter_map(|t| {
            t.remove_slot(slot_index);
            if !t.has_interested_slots() {
                let name = t.get_table_name();
                t.active = false;
                t.trigger_installed = false;
                Some(name)
            } else { None }
        })
        .collect()
}

pub fn get_interested_slots_for_table(table_name: &str) -> Vec<usize> {
    if !is_shmem_available() { return Vec::new(); }
    TABLE_REGISTRY.share().tables.iter()
        .find(|t| t.active && t.get_table_name() == table_name)
        .map(|t| t.get_interested_slots())
        .unwrap_or_default()
}


pub fn list_tracked_tables() -> Vec<(String, usize, bool)> {
    if !is_shmem_available() { return Vec::new(); }
    TABLE_REGISTRY.share().tables.iter()
        .filter(|t| t.active)
        .map(|t| (t.get_table_name(), t.get_interested_slots().len(), t.trigger_installed))
        .collect()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_shmem_event_roundtrip() {
        let event = SubscribeEvent {
            mz_timestamp: 12345,
            mz_diff: 1,
            mz_progressed: false,
            data: Some(serde_json::json!({"id": 1, "name": "test"})),
        };
        
        let shmem_event = ShmemEvent::from_subscribe_event(&event);
        let restored = shmem_event.to_subscribe_event().unwrap();
        
        assert_eq!(restored.mz_timestamp, event.mz_timestamp);
        assert_eq!(restored.mz_diff, event.mz_diff);
    }
    
    #[test]
    fn test_slot_info_subscription_id() {
        let mut info = SlotInfo::default();
        info.set_subscription_id("test-sub-123");
        assert_eq!(info.get_subscription_id(), "test-sub-123");
    }
    
    #[test]
    fn test_table_registry_slot_management() {
        let mut registry = TableRegistry::default();
        registry.set_table_name("users");
        registry.active = true;
        
        // Add slots
        registry.add_slot(0);
        registry.add_slot(5);
        registry.add_slot(10);
        
        assert!(registry.is_slot_interested(0));
        assert!(registry.is_slot_interested(5));
        assert!(registry.is_slot_interested(10));
        assert!(!registry.is_slot_interested(1));
        
        // Check interested slots list
        let slots = registry.get_interested_slots();
        assert_eq!(slots.len(), 3);
        assert!(slots.contains(&0));
        assert!(slots.contains(&5));
        assert!(slots.contains(&10));
        
        // Remove a slot
        registry.remove_slot(5);
        assert!(!registry.is_slot_interested(5));
        assert!(registry.has_interested_slots());
        
        // Remove all slots
        registry.remove_slot(0);
        registry.remove_slot(10);
        assert!(!registry.has_interested_slots());
    }
    
    #[test]
    fn test_table_registry_name() {
        let mut registry = TableRegistry::default();
        
        // Test short name
        registry.set_table_name("users");
        assert_eq!(registry.get_table_name(), "users");
        
        // Test schema-qualified name
        registry.set_table_name("public.orders");
        assert_eq!(registry.get_table_name(), "public.orders");
        
        // Test long name (should truncate)
        let long_name = "a".repeat(200);
        registry.set_table_name(&long_name);
        assert!(registry.get_table_name().len() < 200);
    }
}
