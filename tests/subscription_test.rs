//! Tests for subscription management - New Architecture
//! Uses client-provided subscription_id with shared query optimization

use livequery_server::core::event::{self, SubscriptionMode};
use livequery_server::core::query;
use livequery_server::core::row::RowData;
use livequery_server::core::subscription::{Snapshot, SubscriptionManager};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use std::hash::Hasher;

/// Helper to create RowData from JSON for tests
fn row(j: serde_json::Value) -> RowData {
    RowData::from_value(&j)
}

/// Helper to create typed cols
fn cols(names: &[&str]) -> Option<Arc<[Arc<str>]>> {
    Some(Arc::from(names.iter().map(|s| Arc::<str>::from(*s)).collect::<Vec<_>>().into_boxed_slice()))
}

// === Snapshot Tests ===

#[test]
fn test_snapshot_init() {
    let mut s = Snapshot::new();
    let c = cols(&["id"]);
    let rows = vec![row(json!({"id": 1, "name": "Alice"})), row(json!({"id": 2, "name": "Bob"}))];
    let ev = s.init_rows(rows, &c);
    assert_eq!(ev.len(), 2);
    assert!(ev.iter().all(|e| e.mz_diff == 1));
}

#[test]
fn test_snapshot_diff_insert() {
    let mut s = Snapshot::new();
    let c = cols(&["id"]);
    s.init_rows(vec![row(json!({"id": 1, "name": "Alice"}))], &c);
    let ev = s.diff_rows(vec![row(json!({"id": 1, "name": "Alice"})), row(json!({"id": 2, "name": "Bob"}))], &c);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].mz_diff, 1);
}

#[test]
fn test_snapshot_diff_delete() {
    let mut s = Snapshot::new();
    let c = cols(&["id"]);
    s.init_rows(vec![row(json!({"id": 1, "name": "Alice"})), row(json!({"id": 2, "name": "Bob"}))], &c);
    let ev = s.diff_rows(vec![row(json!({"id": 1, "name": "Alice"}))], &c);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].mz_diff, -1);
}

#[test]
fn test_snapshot_diff_update() {
    let mut s = Snapshot::new();
    let c = cols(&["id"]);
    s.init_rows(vec![row(json!({"id": 1, "name": "Alice"}))], &c);
    let ev = s.diff_rows(vec![row(json!({"id": 1, "name": "Alice Updated"}))], &c);
    assert_eq!(ev.len(), 2);
    assert!(ev.iter().any(|e| e.mz_diff == -1));
    assert!(ev.iter().any(|e| e.mz_diff == 1));
}

#[test]
fn test_snapshot_no_changes() {
    let mut s = Snapshot::new();
    let c = cols(&["id"]);
    s.init_rows(vec![row(json!({"id": 1, "name": "Alice"}))], &c);
    let ev = s.diff_rows(vec![row(json!({"id": 1, "name": "Alice"}))], &c);
    assert_eq!(ev.len(), 0);
}

// === Subscription Manager Tests (New Architecture) ===

#[test]
fn test_subscribe_new() {
    let m = SubscriptionManager::new(1000);
    // New architecture: subscription_id is first param
    let r = m.subscribe("sub-1", "SELECT * FROM users", Some(vec!["id".into()]), SubscriptionMode::Events).unwrap();
    assert!(r.is_new_query);
    assert_eq!(r.subscription_id.as_ref(), "sub-1");
    assert!(m.get_sub("sub-1").is_some());
}

#[test]
fn test_shared_query() {
    let m = SubscriptionManager::new(1000);
    let r1 = m.subscribe("sub-1", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    let r2 = m.subscribe("sub-2", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    assert!(r1.is_new_query);
    assert!(!r2.is_new_query); // Same query, not new
    assert_eq!(r1.query_id, r2.query_id); // Same query_id
    assert_ne!(r1.subscription_id, r2.subscription_id); // Different sub_ids
    assert_eq!(m.stats(), (2, 1)); // 2 subs, 1 query
}

#[test]
fn test_unsubscribe_new() {
    let m = SubscriptionManager::new(1000);
    m.subscribe("sub-1", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    m.subscribe("sub-2", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    assert_eq!(m.stats(), (2, 1));
    
    assert!(m.unsubscribe("sub-1"));
    assert_eq!(m.stats(), (1, 1)); // Query still exists
    
    assert!(m.unsubscribe("sub-2"));
    assert_eq!(m.stats(), (0, 0)); // Query removed
}

#[test]
fn test_heartbeat_new() {
    let m = SubscriptionManager::new(1000);
    m.subscribe("sub-1", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    assert!(m.heartbeat("sub-1"));
    assert!(!m.heartbeat("nope")); // Non-existent sub
}

#[test]
fn test_reject_non_select() {
    let m = SubscriptionManager::new(1000);
    assert!(m.subscribe("sub-1", "INSERT INTO users (name) VALUES ('x')", None, SubscriptionMode::Events).is_err());
}

#[test]
fn test_duplicate_subscription_id() {
    let m = SubscriptionManager::new(1000);
    m.subscribe("sub-1", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    // Same subscription_id should fail
    assert!(m.subscribe("sub-1", "SELECT * FROM orders", None, SubscriptionMode::Events).is_err());
}

#[test]
fn test_cleanup_new() {
    let m = SubscriptionManager::new(1000);
    m.subscribe("sub-1", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    let rm = m.cleanup(std::time::Duration::ZERO);
    assert_eq!(rm.len(), 1);
    assert_eq!(rm[0].as_ref(), "sub-1");
    assert!(m.get_sub("sub-1").is_none());
    assert_eq!(m.stats(), (0, 0)); // Query also removed
}

#[test]
fn test_make_batch() {
    let m = SubscriptionManager::new(1000);
    let r = m.subscribe("sub-1", "SELECT * FROM users", None, SubscriptionMode::Events).unwrap();
    let q = m.get_query(&r.query_id).unwrap();
    
    let batch1 = q.make_batch(vec![event::SubscribeEvent::insert(1, json!({"id": 1}))]);
    assert!(batch1.is_some());
    assert_eq!(batch1.unwrap().seq, 1);
    
    let batch2 = q.make_batch(vec![event::SubscribeEvent::insert(2, json!({"id": 2}))]);
    assert_eq!(batch2.unwrap().seq, 2);
    
    // Empty events should return None
    let batch3 = q.make_batch(vec![]);
    assert!(batch3.is_none());
}

// === Performance Tests ===

#[test]
fn perf_snapshot_diff_100_rows() {
    let mut s = Snapshot::new();
    let c = cols(&["id"]);
    
    let rows: Vec<_> = (0..100).map(|i| row(json!({"id": i, "name": format!("User {}", i)}))).collect();
    s.init_rows(rows.clone(), &c);
    
    let mut new_rows: Vec<_> = (0..100).map(|i| row(json!({"id": i, "name": format!("User {}", i)}))).collect();
    for i in 0..10 { new_rows[i] = row(json!({"id": i, "name": format!("Updated {}", i)})); }
    
    let start = Instant::now();
    let iterations = 1000;
    for _ in 0..iterations { let _ = s.diff_rows(new_rows.clone(), &c); }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / iterations;
    
    assert!(elapsed.as_millis() < 100, "diff too slow: {:?} ({} ns/op)", elapsed, per_op);
    println!("\n[BENCH] snapshot_diff_100_rows: {} ns/op ({:.2} µs)", per_op, per_op as f64 / 1000.0);
}

#[test]
fn perf_snapshot_diff_1000_rows() {
    let mut s = Snapshot::new();
    let c = cols(&["id"]);
    
    let rows: Vec<_> = (0..1000).map(|i| row(json!({"id": i, "name": format!("User {}", i), "email": format!("user{}@test.com", i)}))).collect();
    s.init_rows(rows.clone(), &c);
    
    let mut new_rows: Vec<_> = (0..1000).map(|i| row(json!({"id": i, "name": format!("User {}", i), "email": format!("user{}@test.com", i)}))).collect();
    for i in 0..50 { new_rows[i] = row(json!({"id": i, "name": format!("Updated {}", i), "email": format!("updated{}@test.com", i)})); }
    
    let start = Instant::now();
    let iterations = 100;
    for _ in 0..iterations { let _ = s.diff_rows(new_rows.clone(), &c); }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / iterations as u128;
    
    assert!(elapsed.as_millis() < 500, "diff 1000 too slow: {:?}", elapsed);
    println!("\n[BENCH] snapshot_diff_1000_rows: {} ns/op ({:.2} µs)", per_op, per_op as f64 / 1000.0);
}

#[test]
fn perf_subscribe_shared_query() {
    let m = SubscriptionManager::new(100000);
    let q = "SELECT id, name, email FROM users WHERE status = 'active'";
    let _ = m.subscribe("sub-0", q, None, SubscriptionMode::Events).unwrap();
    
    let start = Instant::now();
    let iterations = 10000u64;
    for i in 1..=iterations { 
        let _ = m.subscribe(&format!("sub-{}", i), q, None, SubscriptionMode::Events); 
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / iterations as u128;
    
    assert!(elapsed.as_millis() < 200, "subscribe too slow: {:?} ({} ns/op)", elapsed, per_op);
    println!("\n[BENCH] subscribe_shared_query: {} ns/op ({:.2} µs)", per_op, per_op as f64 / 1000.0);
}

#[test]
fn perf_query_analyze_cached() {
    let q = "SELECT id, name, email FROM users WHERE status = 'active' AND created_at > '2024-01-01'";
    let _ = query::analyze(q);
    
    let start = Instant::now();
    let iterations = 100000u128;
    for _ in 0..iterations { let _ = query::analyze(q); }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / iterations;
    
    assert!(elapsed.as_millis() < 200, "analyze cache too slow: {:?} ({} ns/op)", elapsed, per_op);
    println!("\n[BENCH] query_analyze_cached: {} ns/op ({:.2} µs)", per_op, per_op as f64 / 1000.0);
}

#[test]
fn perf_where_filter_eval() {
    let a = query::analyze("SELECT * FROM users WHERE status = 'active' AND age > 18");
    let row = json!({"id": 1, "status": "active", "age": 25, "name": "Test"});
    
    let start = Instant::now();
    let iterations = 1000000u128;
    for _ in 0..iterations { let _ = a.filter.eval(&row); }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / iterations;
    
    assert!(elapsed.as_millis() < 500, "filter eval too slow: {:?}", elapsed);
    println!("\n[BENCH] where_filter_eval: {} ns/op", per_op);
}

#[test]
fn perf_hrow_identity() {
    let row = json!({"id": 12345, "name": "John Doe", "email": "john@example.com", "age": 30});
    
    let start = Instant::now();
    let iterations = 1000000u128;
    for _ in 0..iterations { 
        let mut h = rustc_hash::FxHasher::default();
        if let Some(v) = row.get("id") { 
            if let Some(i) = v.as_i64() { std::hash::Hash::hash(&i, &mut h); }
        }
        std::hint::black_box(h.finish());
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / iterations;
    
    println!("\n[BENCH] hrow_identity: {} ns/op", per_op);
}

#[test]
fn perf_event_batch_serialize() {
    let events: Vec<_> = (0..100).map(|i| event::SubscribeEvent::insert(i, json!({"id": i, "name": format!("User {}", i)}))).collect();
    let batch = event::EventBatch::new(1, events);
    
    let start = Instant::now();
    let iterations = 10000u128;
    for _ in 0..iterations { 
        let mut buf = Vec::with_capacity(32000);
        serde_json::to_writer(&mut buf, &batch).unwrap();
        std::hint::black_box(buf);
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() / iterations;
    
    println!("\n[BENCH] event_batch_serialize_100: {} ns/op ({:.2} µs)", per_op, per_op as f64 / 1000.0);
}
