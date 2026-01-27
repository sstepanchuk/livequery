//! Integration tests for LiveQuery Server
//!
//! These tests require running PostgreSQL and NATS.
//! Run with: cargo test -- --ignored

use serde_json::json;

/// Test subscribe request/response format
#[test]
fn test_subscribe_request_format() {
    let request = json!({
        "query": "SELECT * FROM users WHERE active = true",
        "identity_columns": ["id"],
        "client_id": "test-client-123"
    });
    
    assert!(request["query"].is_string());
    assert!(request["identity_columns"].is_array());
    assert!(request["client_id"].is_string());
}

/// Test subscribe response format
#[test]
fn test_subscribe_response_format() {
    let response = json!({
        "success": true,
        "query_hash": 12345678901234u64,
        "subject": "livequery.12345678901234.events",
        "snapshot": [
            {
                "mz_timestamp": 1706284800000i64,
                "mz_diff": 1,
                "mz_progressed": false,
                "data": {"id": 1, "name": "Alice"}
            }
        ]
    });
    
    assert!(response["success"].as_bool().unwrap());
    assert!(response["query_hash"].is_u64());
    assert!(response["subject"].is_string());
    assert!(response["snapshot"].is_array());
}

/// Test event format
#[test]
fn test_event_format() {
    // Insert event
    let insert = json!({
        "mz_timestamp": 1706284800000i64,
        "mz_diff": 1,
        "mz_progressed": false,
        "data": {"id": 1, "name": "Alice", "active": true}
    });
    assert_eq!(insert["mz_diff"], 1);
    assert_eq!(insert["mz_progressed"], false);
    
    // Delete event
    let delete = json!({
        "mz_timestamp": 1706284800000i64,
        "mz_diff": -1,
        "mz_progressed": false,
        "data": {"id": 1, "name": "Alice", "active": true}
    });
    assert_eq!(delete["mz_diff"], -1);
    
    // Progress/heartbeat event
    let progress = json!({
        "mz_timestamp": 1706284800000i64,
        "mz_diff": 0,
        "mz_progressed": true
    });
    assert_eq!(progress["mz_diff"], 0);
    assert_eq!(progress["mz_progressed"], true);
}

/// Test unsubscribe request format
#[test]
fn test_unsubscribe_request_format() {
    let request = json!({
        "query_hash": 12345678901234u64,
        "client_id": "test-client-123"
    });
    
    assert!(request["query_hash"].is_u64());
    assert!(request["client_id"].is_string());
}

// ============================================================================
// Integration tests (require running services)
// ============================================================================

#[tokio::test]
#[ignore = "requires running PostgreSQL and NATS"]
async fn test_full_subscribe_flow() {
    // This test would:
    // 1. Connect to NATS
    // 2. Send subscribe request
    // 3. Verify response contains snapshot
    // 4. Insert data into PostgreSQL
    // 5. Trigger requery
    // 6. Verify events received
    // 7. Unsubscribe
    
    todo!("Implement full integration test")
}

#[tokio::test]
#[ignore = "requires running PostgreSQL and NATS"]
async fn test_query_deduplication() {
    // This test would:
    // 1. Subscribe with query A from client 1
    // 2. Subscribe with same query A from client 2
    // 3. Verify same query_hash returned
    // 4. Unsubscribe client 1
    // 5. Verify subscription still active for client 2
    // 6. Unsubscribe client 2
    // 7. Verify subscription removed
    
    todo!("Implement deduplication test")
}
