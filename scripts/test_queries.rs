//! Test script for LiveQuery Server
//! Run with: cargo run --bin test_queries

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
struct SubscribeRequest {
    subscription_id: String,
    query: String,
    identity_columns: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct SubscribeResponse {
    success: bool,
    #[allow(dead_code)]
    query_hash: Option<u64>,
    #[allow(dead_code)]
    subject: Option<String>,
    error: Option<String>,
    snapshot: Vec<serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    println!("Connecting to NATS at {}...", nats_url);
    let client = async_nats::connect(&nats_url).await?;
    println!("Connected!\n");

    // Test queries from simple to complex
    let test_cases = vec![
        // 1. Simple SELECT
        (
            "Simple SELECT",
            "SELECT * FROM users WHERE active = true",
            Some(vec!["id".to_string()]),
        ),
        // 2. SELECT with ORDER BY and LIMIT
        (
            "ORDER BY + LIMIT",
            "SELECT * FROM products ORDER BY price DESC LIMIT 5",
            Some(vec!["id".to_string()]),
        ),
        // 3. INNER JOIN
        (
            "INNER JOIN",
            "SELECT u.name, o.id as order_id, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id",
            Some(vec!["order_id".to_string()]),
        ),
        // 4. LEFT JOIN
        (
            "LEFT JOIN",
            "SELECT p.name, c.name as category FROM products p LEFT JOIN categories c ON p.category_id = c.id",
            Some(vec!["id".to_string()]),
        ),
        // 5. Multiple JOINs
        (
            "Multiple JOINs",
            r#"
            SELECT u.name as customer, p.name as product, oi.quantity, oi.price
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.id
            JOIN users u ON o.user_id = u.id
            JOIN products p ON oi.product_id = p.id
        "#,
            None,
        ),
        // 6. GROUP BY with aggregates
        (
            "GROUP BY + COUNT",
            "SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent FROM orders GROUP BY user_id",
            Some(vec!["user_id".to_string()]),
        ),
        // 7. GROUP BY with HAVING
        (
            "GROUP BY + HAVING",
            "SELECT category_id, COUNT(*) as cnt, AVG(price) as avg_price FROM products GROUP BY category_id HAVING COUNT(*) > 1",
            Some(vec!["category_id".to_string()]),
        ),
        // 8. Subquery in WHERE
        (
            "Subquery WHERE",
            "SELECT * FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders WHERE status = 'completed')",
            Some(vec!["id".to_string()]),
        ),
        // 9. Subquery in FROM (derived table)
        (
            "Derived Table",
            r#"
            SELECT category_name, total_products, total_stock
            FROM (
                SELECT c.name as category_name, COUNT(p.id) as total_products, SUM(p.stock) as total_stock
                FROM categories c
                LEFT JOIN products p ON c.id = p.category_id
                GROUP BY c.id, c.name
            ) as category_stats
            WHERE total_products > 0
        "#,
            None,
        ),
        // 10. CTE (Common Table Expression)
        (
            "CTE",
            r#"
            WITH active_products AS (
                SELECT * FROM products WHERE active = true AND stock > 0
            ),
            product_reviews AS (
                SELECT product_id, AVG(rating) as avg_rating, COUNT(*) as review_count
                FROM reviews
                GROUP BY product_id
            )
            SELECT ap.name, ap.price, COALESCE(pr.avg_rating, 0) as rating
            FROM active_products ap
            LEFT JOIN product_reviews pr ON ap.id = pr.product_id
        "#,
            None,
        ),
        // 11. Window function
        (
            "Window Function",
            r#"
            SELECT
                name,
                price,
                category_id,
                ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY price DESC) as price_rank
            FROM products
            WHERE active = true
        "#,
            None,
        ),
        // 12. Complex nested query
        (
            "Complex Nested",
            r#"
            SELECT
                u.name,
                u.email,
                order_stats.order_count,
                order_stats.total_spent,
                CASE
                    WHEN order_stats.total_spent > 1000 THEN 'VIP'
                    WHEN order_stats.total_spent > 100 THEN 'Regular'
                    ELSE 'New'
                END as customer_tier
            FROM users u
            LEFT JOIN (
                SELECT user_id, COUNT(*) as order_count, COALESCE(SUM(total), 0) as total_spent
                FROM orders
                WHERE status IN ('completed', 'shipped')
                GROUP BY user_id
            ) order_stats ON u.id = order_stats.user_id
            WHERE u.active = true
            ORDER BY order_stats.total_spent DESC NULLS LAST
        "#,
            Some(vec!["email".to_string()]),
        ),
        // 13. JSONB operations
        (
            "JSONB Query",
            "SELECT id, name, metadata->>'department' as department FROM users WHERE metadata->>'department' IS NOT NULL",
            Some(vec!["id".to_string()]),
        ),
        // 14. Array operations
        (
            "Array Query",
            "SELECT name, tags FROM products WHERE 'apple' = ANY(tags)",
            Some(vec!["id".to_string()]),
        ),
        // 15. DISTINCT
        (
            "DISTINCT",
            "SELECT DISTINCT status FROM orders ORDER BY status",
            None,
        ),
        // 16. UNION
        (
            "UNION",
            r#"
            SELECT name, 'product' as type FROM products WHERE active = true
            UNION ALL
            SELECT name, 'category' as type FROM categories
        "#,
            None,
        ),
    ];

    let mut passed = 0;
    let mut failed = 0;

    for (i, (name, query, identity_cols)) in test_cases.iter().enumerate() {
        print!("Test {}: {} ... ", i + 1, name);

        let request = SubscribeRequest {
            subscription_id: format!("test-{}", i),
            query: query.to_string(),
            identity_columns: identity_cols.clone(),
        };

        let payload = serde_json::to_vec(&request)?;

        let subject = format!("livequery.{}.subscribe", request.subscription_id);
        match client.request(subject, payload.into()).await {
            Ok(response) => match serde_json::from_slice::<SubscribeResponse>(&response.payload) {
                Ok(resp) => {
                    if resp.success {
                        println!("✅ OK ({} rows)", resp.snapshot.len());
                        passed += 1;
                    } else {
                        println!("❌ FAILED: {}", resp.error.unwrap_or_default());
                        failed += 1;
                    }
                }
                Err(e) => {
                    println!("❌ PARSE ERROR: {}", e);
                    failed += 1;
                }
            },
            Err(e) => {
                println!("❌ NATS ERROR: {}", e);
                failed += 1;
            }
        }
    }

    println!("\n========================================");
    println!("Results: {} passed, {} failed", passed, failed);
    println!("========================================");

    Ok(())
}
