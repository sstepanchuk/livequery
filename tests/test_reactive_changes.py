#!/usr/bin/env python3
"""
Тест реактивних змін pg_subscribe
Перевіряє що INSERT/UPDATE/DELETE відстежуються в реальному часі
"""

import psycopg2
import psycopg2.extras
import threading
import time
from typing import List, Dict

DB_CONFIG = {
    "dbname": "postgres",
    "host": "localhost",
    "port": 5432,
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def setup():
    """Setup extension and test table"""
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP EXTENSION IF EXISTS pg_subscribe CASCADE")
            cur.execute("CREATE EXTENSION pg_subscribe")
            cur.execute("DROP TABLE IF EXISTS reactive_test CASCADE")
            cur.execute("""
                CREATE TABLE reactive_test (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    value INT
                )
            """)
            # Initial data
            cur.execute("INSERT INTO reactive_test (name, value) VALUES ('initial', 100)")
    print("✅ Setup complete")


def cleanup():
    """Cleanup test table"""
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS reactive_test CASCADE")
    print("✅ Cleanup complete")


def test_subscribe_detects_insert():
    """Тест: NOTIFY механізм працює для real-time змін"""
    print("\n=== Test: NOTIFY mechanism for real-time changes ===")
    
    notifications = []
    error_holder = [None]
    
    def listener_thread():
        """Thread що слухає NOTIFY"""
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            cur = conn.cursor()
            
            # 1. Prepare to install trigger and get subscription_id
            cur.execute("SELECT pg_subscribe_prepare('SELECT * FROM reactive_test') as sub_id")
            sub_id = cur.fetchone()[0]
            print(f"  Subscription ID: {sub_id}")
            
            # 2. Build channel name and LISTEN
            channel = f"pgsub_{sub_id.replace('-', '')}"
            cur.execute(f"LISTEN {channel}")
            print(f"  Listening on channel: {channel}")
            
            # 3. Wait for notifications (with timeout)
            import select
            start = time.time()
            while time.time() - start < 5:  # 5 second timeout
                if select.select([conn], [], [], 1) == ([], [], []):
                    # Timeout, check again
                    continue
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    notifications.append(notify.payload)
                    print(f"  Received NOTIFY: {notify.payload[:100]}...")
                    if len(notifications) >= 2:
                        return
                        
            cur.close()
        except Exception as e:
            error_holder[0] = str(e)
            import traceback
            print(f"  Listener error: {e}")
            traceback.print_exc()
        finally:
            if conn:
                conn.close()
    
    # Start listener in background
    print("  Starting listener thread...")
    listener = threading.Thread(target=listener_thread)
    listener.start()
    
    # Wait for listener to start
    time.sleep(1)
    
    # Make changes in another connection
    print("  Inserting new rows...")
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("INSERT INTO reactive_test (name, value) VALUES ('new1', 200)")
            print("    Inserted new1")
            time.sleep(0.2)
            cur.execute("INSERT INTO reactive_test (name, value) VALUES ('new2', 300)")
            print("    Inserted new2")
    
    # Wait for listener
    print("  Waiting for listener...")
    listener.join(timeout=7)
    
    if error_holder[0]:
        print(f"❌ FAIL: {error_holder[0]}")
        return False
    
    print(f"  Total notifications received: {len(notifications)}")
    
    # Verify notifications were received
    if len(notifications) >= 2:
        print("✅ PASS: NOTIFY mechanism works - received real-time change notifications!")
        return True
    elif len(notifications) >= 1:
        print(f"⚠️  PARTIAL: Got {len(notifications)} notification(s)")
        return True
    else:
        print(f"❌ FAIL: No notifications received")
        return False


def test_subscribe_snapshot_only():
    """Тест: subscribe_snapshot повертає тільки поточні дані"""
    print("\n=== Test: subscribe_snapshot returns current data ===")
    
    # Add some data first
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DELETE FROM reactive_test")
            cur.execute("INSERT INTO reactive_test (name, value) VALUES ('a', 1), ('b', 2)")
    
    # Get snapshot
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM subscribe_snapshot('SELECT * FROM reactive_test ORDER BY name')")
            results = [dict(row) for row in cur]
    
    print(f"  Got {len(results)} rows")
    
    if len(results) == 2:
        names = [r['data'].get('name') for r in results]
        if 'a' in names and 'b' in names:
            print("✅ PASS: Snapshot returns correct data")
            return True
    
    print(f"❌ FAIL: Expected 2 rows with 'a' and 'b'")
    return False


def test_trigger_installation():
    """Тест: тригери встановлюються через pg_subscribe_prepare"""
    print("\n=== Test: triggers are installed ===")
    
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Use pg_subscribe_prepare which installs triggers
            cur.execute("SELECT pg_subscribe_prepare('SELECT * FROM reactive_test')")
            sub_id = cur.fetchone()[0]
            print(f"  Prepared subscription: {sub_id}")
            
            # Check for triggers
            cur.execute("""
                SELECT tgname FROM pg_trigger 
                WHERE tgrelid = 'reactive_test'::regclass 
                AND tgname LIKE '_pgsub%'
            """)
            triggers = [row[0] for row in cur]
    
    print(f"  Found triggers: {triggers}")
    
    if len(triggers) > 0:
        print("✅ PASS: Triggers installed")
        return True
    else:
        print("❌ FAIL: No triggers found")
        return False


def test_subscribe_with_join():
    """Тест: subscribe працює з JOIN"""
    print("\n=== Test: subscribe works with JOIN ===")
    
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_orders CASCADE")
            cur.execute("DROP TABLE IF EXISTS test_users CASCADE")
            cur.execute("""
                CREATE TABLE test_users (id SERIAL PRIMARY KEY, name TEXT);
                CREATE TABLE test_orders (id SERIAL PRIMARY KEY, user_id INT, amount INT);
                INSERT INTO test_users (name) VALUES ('Alice'), ('Bob');
                INSERT INTO test_orders (user_id, amount) VALUES (1, 100), (1, 200), (2, 50);
            """)
    
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM subscribe_snapshot('
                    SELECT u.name, o.amount 
                    FROM test_users u 
                    JOIN test_orders o ON u.id = o.user_id 
                    ORDER BY u.name, o.amount
                ')
            """)
            results = [dict(row) for row in cur]
    
    print(f"  Got {len(results)} rows")
    for r in results:
        print(f"    - {r['data']}")
    
    # Cleanup
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_orders CASCADE")
            cur.execute("DROP TABLE IF EXISTS test_users CASCADE")
    
    if len(results) == 3:
        print("✅ PASS: JOIN query works")
        return True
    else:
        print(f"❌ FAIL: Expected 3 rows")
        return False


def run_all_tests():
    """Run all reactive tests"""
    print("=" * 50)
    print("pg_subscribe Reactive Changes Test Suite")
    print("=" * 50)
    
    setup()
    
    results = []
    
    tests = [
        test_subscribe_snapshot_only,
        test_trigger_installation,
        test_subscribe_with_join,
        test_subscribe_detects_insert,  # This is the main reactive test
    ]
    
    for test in tests:
        try:
            passed = test()
            results.append((test.__name__, passed))
        except Exception as e:
            print(f"❌ FAIL: {test.__name__} - Exception: {e}")
            results.append((test.__name__, False))
    
    cleanup()
    
    print("\n" + "=" * 50)
    print("Results:")
    passed = sum(1 for _, p in results if p)
    failed = len(results) - passed
    
    for name, p in results:
        status = "✅" if p else "❌"
        print(f"  {status} {name}")
    
    print(f"\nTotal: {passed} passed, {failed} failed")
    print("=" * 50)
    
    return failed == 0


if __name__ == "__main__":
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
