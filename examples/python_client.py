#!/usr/bin/env python3
"""
Example Python client for pg_subscribe extension.

This demonstrates how to connect to PostgreSQL and consume
streaming subscription events.
"""

import json
import psycopg2
from psycopg2 import sql
import signal
import sys

def signal_handler(sig, frame):
    print("\nInterrupted, closing connection...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def subscribe_to_query(dsn: str, query: str):
    """
    Subscribe to a SQL query and print streaming events.
    
    Args:
        dsn: PostgreSQL connection string
        query: The SELECT query to subscribe to
    """
    conn = psycopg2.connect(dsn)
    conn.autocommit = True  # Important for streaming
    
    cur = conn.cursor()
    
    # Escape the query for use in subscribe()
    escaped_query = query.replace("'", "''")
    subscribe_sql = f"SELECT * FROM subscribe('{escaped_query}')"
    
    print(f"Subscribing to: {query}")
    print("-" * 60)
    
    cur.execute(subscribe_sql)
    
    try:
        for row in cur:
            mz_timestamp, mz_diff, mz_progressed, data = row
            
            if mz_progressed:
                # This is a heartbeat/progress message
                print(f"[{mz_timestamp}] ♥ heartbeat")
            elif mz_diff > 0:
                # This is an INSERT event
                print(f"[{mz_timestamp}] + INSERT: {json.dumps(data, indent=2)}")
            else:
                # This is a DELETE event
                print(f"[{mz_timestamp}] - DELETE: {json.dumps(data, indent=2)}")
    
    except KeyboardInterrupt:
        print("\nSubscription cancelled")
    finally:
        cur.close()
        conn.close()

def main():
    # Connection parameters
    dsn = "dbname=postgres user=postgres host=localhost"
    
    # Example queries
    examples = [
        # Simple table subscription
        "SELECT * FROM users",
        
        # With filter
        "SELECT * FROM orders WHERE status = 'pending'",
        
        # With aggregation
        "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
        
        # With JOIN
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
    ]
    
    # Subscribe to the first example by default
    query = sys.argv[1] if len(sys.argv) > 1 else examples[0]
    
    subscribe_to_query(dsn, query)

if __name__ == "__main__":
    main()
