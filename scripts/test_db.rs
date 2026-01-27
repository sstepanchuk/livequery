//! Simple DB connection test
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/livequery".to_string());
    
    println!("Connecting to: {}", url);
    
    match tokio_postgres::connect(&url, NoTls).await {
        Ok((client, conn)) => {
            tokio::spawn(async move { let _ = conn.await; });
            println!("Connected!");
            
            match client.query("SELECT 1 as test", &[]).await {
                Ok(rows) => {
                    println!("Query OK, {} rows", rows.len());
                    for row in rows {
                        let val: i32 = row.get(0);
                        println!("Result: {}", val);
                    }
                }
                Err(e) => println!("Query failed: {:?}", e),
            }
        }
        Err(e) => {
            println!("Connection failed: {:?}", e);
        }
    }
}
