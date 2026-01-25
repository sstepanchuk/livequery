//! # pg_subscribe - PostgreSQL Reactive SUBSCRIBE Extension
//!
//! Unified reactive subscriptions for ANY SQL query.
//! Uses efficient Snapshot-Diff pattern with event-driven change detection.
//!
//! ## Usage
//! ```sql
//! -- Simple reactive subscription
//! SELECT * FROM subscribe('SELECT * FROM users WHERE active = true');
//!
//! -- With identity columns for efficient diffing
//! SELECT * FROM subscribe(
//!     'SELECT * FROM orders',
//!     identity_columns => ARRAY['id']
//! );
//! ```

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry};

mod event;
mod query_analyzer;
mod query_dedup;
mod shmem;
mod streaming;
mod trigger;
mod types;
mod unified_subscribe;

pub use event::SubscribeEvent;

pgrx::pg_module_magic!();

/// Extension initialization
#[pg_guard]
pub extern "C" fn _PG_init() {
    // Initialize proper shared memory (requires shared_preload_libraries)
    shmem::init_shmem();
    
    // Initialize query deduplication registry
    query_dedup::init_query_registry();
    
    // Register GUC variables for configuration
    register_gucs();
}

/// Register configuration parameters (GUCs)
fn register_gucs() {
    // Heartbeat interval in milliseconds
    GucRegistry::define_int_guc(
        "pg_subscribe.heartbeat_interval_ms",
        "Interval between heartbeat messages in milliseconds",
        "Default is 1000 ms",
        &streaming::HEARTBEAT_INTERVAL_MS,
        100,
        60000,
        GucContext::Userset,
        GucFlags::default(),
    );
    
    // Note: max_slots and buffer sizes are now compile-time constants in shmem.rs
    // See shmem::MAX_SLOTS, shmem::MAX_EVENTS_PER_SLOT, shmem::MAX_EVENT_PAYLOAD
}

/// Main SUBSCRIBE function - unified reactive subscriptions for ANY SQL query
/// 
/// Works with JOINs, aggregations, window functions - any valid SELECT.
/// Uses efficient Snapshot-Diff pattern with event-driven change detection.
/// 
/// # Arguments
/// * `query` - SQL SELECT query to subscribe to
/// * `identity_columns` - Optional columns for row identity (like PRIMARY KEY)
///                        Improves diff performance. If not specified, uses row hash.
/// 
/// # Examples
/// ```sql
/// -- Simple query
/// SELECT * FROM subscribe('SELECT * FROM users WHERE active = true');
/// 
/// -- With identity column for efficient diffing
/// SELECT * FROM subscribe(
///     'SELECT * FROM orders WHERE user_id = 123',
///     identity_columns => ARRAY['id']
/// );
/// 
/// -- Complex JOIN with aggregation
/// SELECT * FROM subscribe(
///     'SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name',
///     identity_columns => ARRAY['name']
/// );
/// ```
#[pg_extern]
fn subscribe(
    query: &str,
    identity_columns: default!(Option<Vec<String>>, "NULL"),
) -> TableIterator<'static, (
    name!(mz_timestamp, i64),
    name!(mz_diff, i32),
    name!(mz_progressed, bool),
    name!(data, pgrx::JsonB),
)> {
    unified_subscribe::create_unified_subscription(query, identity_columns)
}

/// Get extension statistics
#[pg_extern]
fn pg_subscribe_stats() -> TableIterator<'static, (
    name!(stat_name, String),
    name!(stat_value, i64),
)> {
    shmem::get_statistics()
}

/// List all tables with shared triggers installed
/// Shows table name, number of interested subscriptions, and trigger status
#[pg_extern]
fn pg_subscribe_tracked_tables() -> TableIterator<'static, (
    name!(table_name, String),
    name!(subscriber_count, i32),
    name!(trigger_installed, bool),
)> {
    let tables = shmem::list_tracked_tables();
    let rows: Vec<_> = tables.into_iter()
        .map(|(name, count, installed)| (name, count as i32, installed))
        .collect();
    TableIterator::new(rows)
}

/// Analyze query to check IVM compatibility
#[pg_extern]
fn pg_subscribe_analyze_query(query: &str) -> pgrx::JsonB {
    let analysis = query_analyzer::analyze_query(query);
    pgrx::JsonB(serde_json::to_value(&analysis).unwrap_or_default())
}

/// Subscribe snapshot only - returns current data without streaming
#[pg_extern]
fn subscribe_snapshot(
    query: &str,
    identity_columns: default!(Option<Vec<String>>, "NULL"),
) -> TableIterator<'static, (
    name!(mz_timestamp, i64),
    name!(mz_diff, i32),
    name!(mz_progressed, bool),
    name!(data, pgrx::JsonB),
)> {
    unified_subscribe::create_snapshot_subscription(query, identity_columns)
}

/// Prepare subscription - allocates slot and installs shared triggers
/// Returns subscription_id for use with LISTEN channel
#[pg_extern]
fn pg_subscribe_prepare(query: &str) -> String {
    let analysis = query_analyzer::analyze_query(query);
    
    if !analysis.is_valid {
        pgrx::error!("Invalid query: {}", analysis.incompatibility_reason.unwrap_or_default());
    }
    
    if analysis.referenced_tables.is_empty() {
        pgrx::error!("Query must reference at least one table");
    }
    
    let subscription_id = uuid::Uuid::new_v4().to_string();
    
    let slot_index = match shmem::allocate_slot(&subscription_id) {
        Some(idx) => idx,
        None => pgrx::error!("No available subscription slots"),
    };
    
    // Register triggers - cleanup slot on failure
    let mut failed = false;
    for table_ref in &analysis.referenced_tables {
        let table_name = table_ref.schema.as_ref()
            .map(|s| format!("{}.{}", s, table_ref.table))
            .unwrap_or_else(|| table_ref.table.clone());
        
        if let Err(e) = trigger::register_subscription_for_table(&table_name, slot_index) {
            pgrx::warning!("Failed to register trigger on {}: {}", table_name, e);
            failed = true;
        }
    }
    
    if failed {
        // Cleanup on partial failure
        let _ = trigger::cleanup_shared_triggers_for_slot(slot_index);
        shmem::release_slot(slot_index);
        pgrx::error!("Failed to register triggers for subscription");
    }
    
    subscription_id
}

/// Get query deduplication statistics
#[pg_extern]
fn pg_subscribe_dedup_stats() -> TableIterator<'static, (
    name!(stat_name, String),
    name!(stat_value, i64),
)> {
    let (total_deduped, active_queries, total_clients) = query_dedup::get_dedup_stats();
    
    let stats = vec![
        ("total_deduplicated".to_string(), total_deduped as i64),
        ("active_unique_queries".to_string(), active_queries as i64),
        ("total_clients_sharing".to_string(), total_clients as i64),
    ];
    
    TableIterator::new(stats)
}

/// List all deduplicated queries
/// Shows query template, slot index, and number of clients sharing
#[pg_extern]
fn pg_subscribe_deduplicated_queries() -> TableIterator<'static, (
    name!(query_template, String),
    name!(slot_index, i32),
    name!(client_count, i32),
)> {
    let queries = query_dedup::list_deduplicated_queries();
    let rows: Vec<_> = queries.into_iter()
        .map(|(query, slot, count)| (query, slot as i32, count as i32))
        .collect();
    TableIterator::new(rows)
}

/// Normalize a query (for debugging/testing deduplication)
#[pg_extern]
fn pg_subscribe_normalize_query(query: &str) -> String {
    query_dedup::normalize_query(query)
}

/// Compute query hash (for debugging/testing deduplication)
#[pg_extern]
fn pg_subscribe_query_hash(query: &str) -> i64 {
    query_dedup::compute_query_hash(query) as i64
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_subscribe_simple() {
        // Create test table
        Spi::run("CREATE TABLE test_orders (id SERIAL PRIMARY KEY, user_id INT, amount NUMERIC)").unwrap();
        Spi::run("INSERT INTO test_orders (user_id, amount) VALUES (1, 100), (1, 200), (2, 50)").unwrap();
        
        // Test query analysis
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pg_subscribe_analyze_query('SELECT user_id, count(*) FROM test_orders GROUP BY user_id')"
        ).unwrap();
        
        assert!(result.is_some());
    }

    #[pg_test]
    fn test_analyze_complex_query() {
        let analysis = query_analyzer::analyze_query(
            "SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name"
        );
        
        assert!(analysis.has_join);
        assert!(analysis.has_aggregation);
    }
    
    #[pg_test]
    fn test_numeric_types() {
        Spi::run("CREATE TABLE test_numeric (
            id SERIAL PRIMARY KEY,
            small_int SMALLINT,
            regular_int INT,
            big_int BIGINT,
            amount NUMERIC(10,2),
            price FLOAT4,
            total FLOAT8
        )").unwrap();
        Spi::run("INSERT INTO test_numeric (small_int, regular_int, big_int, amount, price, total) 
                  VALUES (1, 100, 1000000000, 123.45, 99.99, 12345.6789)").unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT * FROM test_numeric') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        // Check that amount is present (Numeric type)
        assert!(json.get("amount").is_some());
    }
    
    #[pg_test]
    fn test_timestamp_types() {
        Spi::run("CREATE TABLE test_timestamps (
            id SERIAL PRIMARY KEY,
            created_date DATE,
            created_time TIME,
            created_at TIMESTAMP,
            updated_at TIMESTAMPTZ
        )").unwrap();
        Spi::run("INSERT INTO test_timestamps (created_date, created_time, created_at, updated_at) 
                  VALUES ('2024-01-15', '10:30:00', '2024-01-15 10:30:00', '2024-01-15 10:30:00+00')").unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT * FROM test_timestamps') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        assert!(json.get("created_date").is_some());
        assert!(json.get("created_at").is_some());
    }
    
    #[pg_test]
    fn test_uuid_type() {
        Spi::run("CREATE TABLE test_uuid (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT
        )").unwrap();
        Spi::run("INSERT INTO test_uuid (name) VALUES ('test')").unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT * FROM test_uuid') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        // UUID should be serialized as string
        assert!(json.get("id").is_some());
        assert!(json["id"].is_string());
    }
    
    #[pg_test]
    fn test_json_types() {
        Spi::run("CREATE TABLE test_json (
            id SERIAL PRIMARY KEY,
            data_json JSON,
            data_jsonb JSONB
        )").unwrap();
        Spi::run(r#"INSERT INTO test_json (data_json, data_jsonb) 
                    VALUES ('{"key": "value"}', '{"nested": {"array": [1, 2, 3]}}')"#).unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT * FROM test_json') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        // JSON should be properly nested
        assert!(json.get("data_jsonb").is_some());
    }
    
    #[pg_test]
    fn test_array_types() {
        Spi::run("CREATE TABLE test_arrays (
            id SERIAL PRIMARY KEY,
            int_arr INT[],
            text_arr TEXT[],
            bool_arr BOOLEAN[]
        )").unwrap();
        Spi::run("INSERT INTO test_arrays (int_arr, text_arr, bool_arr) 
                  VALUES (ARRAY[1,2,3], ARRAY['a','b','c'], ARRAY[true, false])").unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT * FROM test_arrays') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        // Arrays should be JSON arrays
        assert!(json.get("int_arr").is_some());
    }
    
    #[pg_test]
    fn test_column_names() {
        Spi::run("CREATE TABLE test_columns (
            user_id INT,
            user_name TEXT,
            user_email TEXT
        )").unwrap();
        Spi::run("INSERT INTO test_columns VALUES (1, 'Alice', 'alice@test.com')").unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT user_id, user_name, user_email FROM test_columns') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        // Check real column names are used, not col_1, col_2
        assert!(json.get("user_id").is_some(), "Should have user_id column");
        assert!(json.get("user_name").is_some(), "Should have user_name column");
        assert!(json.get("user_email").is_some(), "Should have user_email column");
        // Make sure old format is not present
        assert!(json.get("col_1").is_none(), "Should NOT have col_1");
    }
    
    #[pg_test]
    fn test_null_values() {
        Spi::run("CREATE TABLE test_nulls (
            id INT,
            nullable_text TEXT,
            nullable_int INT
        )").unwrap();
        Spi::run("INSERT INTO test_nulls VALUES (1, NULL, NULL)").unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT * FROM test_nulls') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        assert!(json.get("nullable_text").is_some());
        assert!(json["nullable_text"].is_null());
    }
    
    #[pg_test]
    fn test_boolean_type() {
        Spi::run("CREATE TABLE test_bool (id INT, active BOOLEAN)").unwrap();
        Spi::run("INSERT INTO test_bool VALUES (1, true), (2, false)").unwrap();
        
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT data FROM subscribe('SELECT * FROM test_bool WHERE id = 1') LIMIT 1"
        ).unwrap();
        
        assert!(result.is_some());
        let json = result.unwrap().0;
        assert_eq!(json["active"], true);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // Test setup
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'pg_subscribe'",
        ]
    }
}
