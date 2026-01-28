//! Tests for SQL query parsing

use livequery_server::core::query::analyze;

#[test]
fn test_simple_select() {
    let a = analyze("SELECT * FROM users");
    assert!(a.is_valid);
    assert_eq!(a.tables, vec!["users"]);
}

#[test]
fn test_schema_qualified() {
    let a = analyze("SELECT * FROM public.users");
    assert!(a.is_valid);
    assert_eq!(a.tables, vec!["users"]); // Only table name, no schema
}

#[test]
fn test_inner_join() {
    let a = analyze("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
    assert!(a.is_valid);
    assert_eq!(a.tables.len(), 2);
}

#[test]
fn test_left_join() {
    let a = analyze("SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id");
    assert!(a.is_valid);
    assert_eq!(a.tables.len(), 2);
}

#[test]
fn test_multiple_joins() {
    let a = analyze(
        "SELECT * FROM users u 
         JOIN orders o ON u.id = o.user_id 
         JOIN products p ON o.product_id = p.id",
    );
    assert!(a.is_valid);
    assert_eq!(a.tables.len(), 3);
}

#[test]
fn test_subquery() {
    let a = analyze("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
    assert!(a.is_valid);
    assert!(!a.tables.is_empty());
    assert!(a.tables.contains(&"users".to_string()));
}

#[test]
fn test_cte() {
    let a = analyze(
        "WITH active_users AS (SELECT * FROM users WHERE active = true)
         SELECT * FROM active_users",
    );
    assert!(a.is_valid);
}

#[test]
fn test_aggregation() {
    let a = analyze("SELECT user_id, COUNT(*), SUM(amount) FROM orders GROUP BY user_id");
    assert!(a.is_valid);
    assert_eq!(a.tables, vec!["orders"]);
}

#[test]
fn test_invalid_query() {
    let a = analyze("THIS IS NOT SQL");
    assert!(!a.is_valid);
    assert!(a.error.is_some());
}

#[test]
fn test_non_select() {
    let a = analyze("INSERT INTO users (name) VALUES ('test')");
    assert!(!a.is_valid);
    assert!(a.error.unwrap().contains("SELECT"));
}
