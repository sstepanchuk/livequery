#!/usr/bin/env python3
"""
pg_subscribe Modern Test Suite
Тестування нових фіч: дедуплікація, складні запити, нові API функції
"""

import psycopg2
import psycopg2.extras
import threading
import time
import json
from contextlib import contextmanager
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# Конфігурація
DB_CONFIG = {
    "dbname": "postgres",
    "host": "localhost",
    "port": 5432,
}

TEST_TIMEOUT = 10


@dataclass
class TestResult:
    name: str
    passed: bool = False
    error: Optional[str] = None
    duration: float = 0
    details: Dict[str, Any] = None

    def __post_init__(self):
        if self.details is None:
            self.details = {}

    def __str__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        result = f"{status} {self.name} ({self.duration:.3f}s)"
        if self.error:
            result += f"\n    Error: {self.error}"
        return result


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def setup_extension():
    """Встановити розширення"""
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP EXTENSION IF EXISTS pg_subscribe CASCADE")
            cur.execute("CREATE EXTENSION pg_subscribe")


def setup_test_data():
    """Створити тестові дані"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS orders CASCADE;
                DROP TABLE IF EXISTS users CASCADE;
                DROP TABLE IF EXISTS products CASCADE;
                DROP TABLE IF EXISTS categories CASCADE;
                
                CREATE TABLE categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                );
                
                CREATE TABLE products (
                    id SERIAL PRIMARY KEY,
                    category_id INT REFERENCES categories(id),
                    name TEXT NOT NULL,
                    price NUMERIC(10,2)
                );
                
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT,
                    active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    user_id INT REFERENCES users(id),
                    product_id INT REFERENCES products(id),
                    quantity INT DEFAULT 1,
                    total NUMERIC(10,2),
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                
                INSERT INTO categories (name) VALUES ('Electronics'), ('Books'), ('Clothing');
                
                INSERT INTO products (category_id, name, price) VALUES
                    (1, 'Laptop', 999.99),
                    (1, 'Phone', 599.99),
                    (2, 'SQL Book', 49.99),
                    (3, 'T-Shirt', 19.99);
                
                INSERT INTO users (name, email, active) VALUES
                    ('Alice', 'alice@test.com', true),
                    ('Bob', 'bob@test.com', true),
                    ('Charlie', 'charlie@test.com', false),
                    ('Diana', 'diana@test.com', true);
                
                INSERT INTO orders (user_id, product_id, quantity, total, status) VALUES
                    (1, 1, 1, 999.99, 'completed'),
                    (1, 3, 2, 99.98, 'completed'),
                    (2, 2, 1, 599.99, 'pending'),
                    (2, 4, 3, 59.97, 'completed'),
                    (4, 1, 1, 999.99, 'pending');
            """)
        conn.commit()


def cleanup_test_data():
    """Видалити тестові дані"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS orders CASCADE;
                DROP TABLE IF EXISTS users CASCADE;
                DROP TABLE IF EXISTS products CASCADE;
                DROP TABLE IF EXISTS categories CASCADE;
            """)
        conn.commit()


def run_query(query: str, limit: int = 100) -> List[Dict]:
    """Виконати subscribe_snapshot запит"""
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            escaped = query.replace("'", "''")
            cur.execute(f"SELECT * FROM subscribe_snapshot('{escaped}') LIMIT {limit}")
            return [dict(row) for row in cur.fetchall()]


def get_stats() -> Dict[str, int]:
    """Отримати статистику"""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM pg_subscribe_stats()")
            return {row['stat_name']: row['stat_value'] for row in cur.fetchall()}


def get_dedup_stats() -> Dict[str, int]:
    """Отримати статистику дедуплікації"""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM pg_subscribe_dedup_stats()")
            return {row['stat_name']: row['stat_value'] for row in cur.fetchall()}


# ============================================================================
# ТЕСТИ: Query Normalization & Hashing (PostgreSQL native)
# ============================================================================

def test_query_normalization():
    """Тест нормалізації запитів через PostgreSQL"""
    result = TestResult("Query Normalization (PostgreSQL native)")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Тест нормалізації whitespace
                cur.execute("SELECT pg_subscribe_normalize_query('SELECT  *  FROM  users')")
                normalized = cur.fetchone()[0]
                
                assert normalized == "SELECT * FROM users", f"Got: {normalized}"
                
                # Тест з переносами рядків
                cur.execute("SELECT pg_subscribe_normalize_query('SELECT *\n\tFROM users\n\tWHERE id = 1')")
                normalized = cur.fetchone()[0]
                
                assert "SELECT * FROM users WHERE id = 1" == normalized, f"Got: {normalized}"
                
                result.passed = True
                result.details = {"normalized": normalized}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_query_hash_consistency():
    """Тест консистентності хешування (PostgreSQL md5)"""
    result = TestResult("Query Hash Consistency (md5)")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Однакові запити з різним whitespace повинні мати однаковий хеш
                cur.execute("SELECT pg_subscribe_query_hash('SELECT * FROM users')")
                hash1 = cur.fetchone()[0]
                
                cur.execute("SELECT pg_subscribe_query_hash('SELECT  *  FROM  users')")
                hash2 = cur.fetchone()[0]
                
                cur.execute("SELECT pg_subscribe_query_hash('SELECT *\nFROM\nusers')")
                hash3 = cur.fetchone()[0]
                
                assert hash1 == hash2 == hash3, f"Hashes should match: {hash1}, {hash2}, {hash3}"
                
                # Різні запити - різні хеші
                cur.execute("SELECT pg_subscribe_query_hash('SELECT * FROM orders')")
                hash_different = cur.fetchone()[0]
                
                assert hash1 != hash_different, "Different queries should have different hashes"
                
                result.passed = True
                result.details = {"hash_users": hash1, "hash_orders": hash_different}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


# ============================================================================
# ТЕСТИ: Complex Queries
# ============================================================================

def test_complex_join():
    """Тест складного JOIN з 3+ таблиць"""
    result = TestResult("Complex JOIN (3 tables)")
    start = time.time()
    
    try:
        query = """
            SELECT u.name as user_name, p.name as product_name, 
                   c.name as category, o.total
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN products p ON o.product_id = p.id
            JOIN categories c ON p.category_id = c.id
            ORDER BY o.total DESC
        """
        rows = run_query(query)
        
        assert len(rows) == 5, f"Expected 5 orders, got {len(rows)}"
        
        # Перевірка структури
        for row in rows:
            data = row['data']
            assert 'user_name' in data
            assert 'product_name' in data
            assert 'category' in data
            assert 'total' in data
        
        result.passed = True
        result.details = {"row_count": len(rows), "sample": rows[0]['data']}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_window_function():
    """Тест Window Functions"""
    result = TestResult("Window Function (ROW_NUMBER, RANK)")
    start = time.time()
    
    try:
        query = """
            SELECT user_id, total,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total DESC) as row_num,
                   RANK() OVER (ORDER BY total DESC) as global_rank
            FROM orders
        """
        rows = run_query(query)
        
        assert len(rows) == 5, f"Expected 5 rows, got {len(rows)}"
        
        # Перевірка що window functions працюють
        for row in rows:
            data = row['data']
            assert 'row_num' in data
            assert 'global_rank' in data
            assert int(data['row_num']) >= 1
        
        result.passed = True
        result.details = {"row_count": len(rows), "sample": rows[0]['data']}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_cte_query():
    """Тест Common Table Expression (CTE / WITH)"""
    result = TestResult("CTE (WITH clause)")
    start = time.time()
    
    try:
        query = """
            WITH user_totals AS (
                SELECT user_id, SUM(total) as total_spent, COUNT(*) as order_count
                FROM orders
                GROUP BY user_id
            ),
            top_users AS (
                SELECT * FROM user_totals WHERE total_spent > 500
            )
            SELECT u.name, t.total_spent, t.order_count
            FROM top_users t
            JOIN users u ON t.user_id = u.id
            ORDER BY t.total_spent DESC
        """
        rows = run_query(query)
        
        # Alice: 1099.97, Diana: 999.99 - обидві > 500
        assert len(rows) >= 2, f"Expected at least 2 top users, got {len(rows)}"
        
        result.passed = True
        result.details = {"row_count": len(rows), "results": [r['data'] for r in rows]}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_subquery():
    """Тест Subquery"""
    result = TestResult("Subquery (nested SELECT)")
    start = time.time()
    
    try:
        query = """
            SELECT u.name, u.email
            FROM users u
            WHERE u.id IN (
                SELECT DISTINCT user_id FROM orders WHERE status = 'completed'
            )
            ORDER BY u.name
        """
        rows = run_query(query)
        
        # Alice та Bob мають completed orders
        assert len(rows) == 2, f"Expected 2 users with completed orders, got {len(rows)}"
        
        names = [r['data']['name'] for r in rows]
        assert 'Alice' in names
        assert 'Bob' in names
        
        result.passed = True
        result.details = {"users_with_completed_orders": names}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_aggregation_with_having():
    """Тест Aggregation з HAVING"""
    result = TestResult("Aggregation with HAVING")
    start = time.time()
    
    try:
        query = """
            SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent
            FROM orders
            GROUP BY user_id
            HAVING COUNT(*) > 1
            ORDER BY total_spent DESC
        """
        rows = run_query(query)
        
        # Alice (2 orders) та Bob (2 orders)
        assert len(rows) == 2, f"Expected 2 users with >1 orders, got {len(rows)}"
        
        for row in rows:
            assert int(row['data']['order_count']) > 1
        
        result.passed = True
        result.details = {"row_count": len(rows), "results": [r['data'] for r in rows]}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_distinct_query():
    """Тест DISTINCT"""
    result = TestResult("DISTINCT query")
    start = time.time()
    
    try:
        query = """
            SELECT DISTINCT status FROM orders ORDER BY status
        """
        rows = run_query(query)
        
        statuses = [r['data']['status'] for r in rows]
        assert 'completed' in statuses
        assert 'pending' in statuses
        assert len(statuses) == 2, f"Expected 2 distinct statuses, got {len(statuses)}"
        
        result.passed = True
        result.details = {"distinct_statuses": statuses}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


# ============================================================================
# ТЕСТИ: Deduplication
# ============================================================================

def test_dedup_stats_available():
    """Тест що функції дедуплікації доступні"""
    result = TestResult("Dedup Stats Available")
    start = time.time()
    
    try:
        stats = get_dedup_stats()
        
        assert 'total_deduplicated' in stats
        assert 'active_unique_queries' in stats
        assert 'total_clients_sharing' in stats
        
        result.passed = True
        result.details = stats
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_dedup_list_queries():
    """Тест списку дедуплікованих запитів"""
    result = TestResult("Dedup List Queries")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM pg_subscribe_deduplicated_queries()")
                queries = cur.fetchall()
                
                result.details = {"query_count": len(queries), "queries": [dict(q) for q in queries]}
                result.passed = True
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_main_stats_include_dedup():
    """Тест що головна статистика включає dedup дані"""
    result = TestResult("Main Stats Include Dedup")
    start = time.time()
    
    try:
        stats = get_stats()
        
        # Перевірка базових статистик
        assert 'max_slots' in stats
        assert stats['max_slots'] == 64, f"Expected 64 slots, got {stats['max_slots']}"
        
        # Перевірка dedup статистик в головній функції
        assert 'dedup_total_reused' in stats
        assert 'dedup_unique_queries' in stats
        assert 'dedup_total_clients' in stats
        
        result.passed = True
        result.details = stats
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


# ============================================================================
# ТЕСТИ: Query Analyzer
# ============================================================================

def test_analyzer_detects_window_functions():
    """Тест що analyzer виявляє window functions"""
    result = TestResult("Analyzer Detects Window Functions")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM orders"
                cur.execute(f"SELECT pg_subscribe_analyze_query('{query}')")
                analysis = cur.fetchone()['pg_subscribe_analyze_query']
                
                # Note: field is has_window_functions (plural)
                assert analysis['has_window_functions'] == True
                
                result.passed = True
                result.details = analysis
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_analyzer_detects_cte():
    """Тест що analyzer виявляє CTE"""
    result = TestResult("Analyzer Detects CTE")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # CTE query - analyzer checks if uppercase query starts with "WITH "
                query = "WITH t AS (SELECT * FROM users) SELECT * FROM t"
                cur.execute("SELECT pg_subscribe_analyze_query(%s)", (query,))
                analysis = cur.fetchone()['pg_subscribe_analyze_query']
                
                result.details = analysis
                assert analysis['has_cte'] == True, f"has_cte should be True, got: {analysis.get('has_cte')}"
                
                result.passed = True
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_analyzer_extracts_tables():
    """Тест що analyzer витягує таблиці"""
    result = TestResult("Analyzer Extracts Tables")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id"
                cur.execute(f"SELECT pg_subscribe_analyze_query('{query}')")
                analysis = cur.fetchone()['pg_subscribe_analyze_query']
                
                tables = [t['table'] for t in analysis['referenced_tables']]
                assert 'users' in tables
                assert 'orders' in tables
                
                result.passed = True
                result.details = {"tables": tables}
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


# ============================================================================
# ТЕСТИ: Data Types Serialization
# ============================================================================

def test_all_data_types():
    """Тест серіалізації ВСІХ типів PostgreSQL"""
    result = TestResult("All PostgreSQL Data Types")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Створити таблицю з усіма типами
                cur.execute("""
                    DROP TABLE IF EXISTS test_all_types CASCADE;
                    CREATE TABLE test_all_types (
                        -- Integers
                        id SERIAL PRIMARY KEY,
                        small_int SMALLINT,
                        regular_int INT,
                        big_int BIGINT,
                        
                        -- Floats
                        float4_val FLOAT4,
                        float8_val FLOAT8,
                        numeric_val NUMERIC(15,4),
                        
                        -- Text
                        text_val TEXT,
                        varchar_val VARCHAR(100),
                        char_val CHAR(10),
                        
                        -- Boolean
                        bool_val BOOLEAN,
                        
                        -- Date/Time
                        date_val DATE,
                        time_val TIME,
                        timestamp_val TIMESTAMP,
                        timestamptz_val TIMESTAMPTZ,
                        interval_val INTERVAL,
                        
                        -- JSON
                        json_val JSON,
                        jsonb_val JSONB,
                        
                        -- UUID
                        uuid_val UUID,
                        
                        -- Arrays
                        int_arr INT[],
                        text_arr TEXT[],
                        bool_arr BOOLEAN[]
                    );
                    
                    INSERT INTO test_all_types (
                        small_int, regular_int, big_int,
                        float4_val, float8_val, numeric_val,
                        text_val, varchar_val, char_val,
                        bool_val,
                        date_val, time_val, timestamp_val, timestamptz_val, interval_val,
                        json_val, jsonb_val,
                        uuid_val,
                        int_arr, text_arr, bool_arr
                    ) VALUES (
                        32767, 2147483647, 9223372036854775807,
                        3.14, 3.14159265358979, 12345678901.1234,
                        'Hello World', 'VARCHAR test', 'CHAR      ',
                        true,
                        '2024-01-15', '14:30:45', '2024-01-15 14:30:45', '2024-01-15 14:30:45+02', '1 year 2 months 3 days',
                        '{"key": "value", "number": 42}', '{"nested": {"array": [1, 2, 3]}, "flag": true}',
                        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                        ARRAY[1, 2, 3, 4, 5], ARRAY['a', 'b', 'c'], ARRAY[true, false, true]
                    );
                """)
            conn.commit()
        
        # Виконати subscribe та перевірити типи
        rows = run_query("SELECT * FROM test_all_types")
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        
        data = rows[0]['data']
        
        # Перевірка кожного типу
        type_checks = {
            # Integers
            'small_int': (32767, int),
            'regular_int': (2147483647, int),
            'big_int': (9223372036854775807, int),
            
            # Floats/Numeric - можуть бути string через precision
            'float4_val': (None, (int, float)),  # приблизно 3.14
            'float8_val': (None, (int, float)),  # приблизно 3.14159
            'numeric_val': ('12345678901.1234', str),  # NUMERIC як string для precision
            
            # Text
            'text_val': ('Hello World', str),
            'varchar_val': ('VARCHAR test', str),
            
            # Boolean
            'bool_val': (True, bool),
            
            # Date/Time - всі як strings
            'date_val': (None, str),
            'time_val': (None, str),
            'timestamp_val': (None, str),
            'timestamptz_val': (None, str),
            'interval_val': (None, str),
            
            # JSON - as parsed objects
            'json_val': (None, dict),
            'jsonb_val': (None, dict),
            
            # UUID - як string
            'uuid_val': ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', str),
            
            # Arrays - як lists
            'int_arr': ([1, 2, 3, 4, 5], list),
            'text_arr': (['a', 'b', 'c'], list),
            'bool_arr': ([True, False, True], list),
        }
        
        errors = []
        for field, (expected_val, expected_type) in type_checks.items():
            if field not in data:
                errors.append(f"Missing field: {field}")
                continue
            
            val = data[field]
            
            # Type check
            if not isinstance(val, expected_type):
                errors.append(f"{field}: expected type {expected_type}, got {type(val).__name__} = {val}")
            
            # Value check if specified
            if expected_val is not None and val != expected_val:
                errors.append(f"{field}: expected {expected_val}, got {val}")
        
        if errors:
            result.error = "; ".join(errors[:5])  # Show first 5 errors
            result.details = {"data": data, "errors": errors}
        else:
            result.passed = True
            result.details = {
                "all_fields_present": True,
                "sample_values": {
                    "big_int": data.get('big_int'),
                    "numeric_val": data.get('numeric_val'),
                    "uuid_val": data.get('uuid_val'),
                    "jsonb_val": data.get('jsonb_val'),
                    "int_arr": data.get('int_arr'),
                    "timestamp_val": data.get('timestamp_val'),
                }
            }
        
        # Cleanup
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_all_types CASCADE")
            conn.commit()
            
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_null_values():
    """Тест обробки NULL значень"""
    result = TestResult("NULL Values Handling")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS test_nulls CASCADE;
                    CREATE TABLE test_nulls (
                        id INT,
                        text_val TEXT,
                        int_val INT,
                        bool_val BOOLEAN,
                        json_val JSONB
                    );
                    INSERT INTO test_nulls VALUES (1, NULL, NULL, NULL, NULL);
                """)
            conn.commit()
        
        rows = run_query("SELECT * FROM test_nulls")
        assert len(rows) == 1
        
        data = rows[0]['data']
        
        # Всі NULL значення повинні бути None/null
        assert data['text_val'] is None, f"text_val should be null, got {data['text_val']}"
        assert data['int_val'] is None, f"int_val should be null, got {data['int_val']}"
        assert data['bool_val'] is None, f"bool_val should be null, got {data['bool_val']}"
        assert data['json_val'] is None, f"json_val should be null, got {data['json_val']}"
        
        result.passed = True
        result.details = {"null_fields_correct": True, "data": data}
        
        # Cleanup
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_nulls CASCADE")
            conn.commit()
            
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


# ============================================================================
# ТЕСТИ: Reactive Subscribe (INSERT/UPDATE/DELETE)
# ============================================================================

def test_reactive_subscribe_insert():
    """Тест реактивного subscribe - INSERT детектується"""
    result = TestResult("Reactive Subscribe - INSERT")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS test_reactive CASCADE;
                    CREATE TABLE test_reactive (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        value INT
                    );
                    INSERT INTO test_reactive (name, value) VALUES ('initial', 100);
                """)
            conn.commit()
        
        # Отримати початковий snapshot
        rows_before = run_query("SELECT * FROM test_reactive")
        assert len(rows_before) == 1, f"Expected 1 row before, got {len(rows_before)}"
        
        # INSERT нового рядка
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO test_reactive (name, value) VALUES ('new_row', 200)")
            conn.commit()
        
        # Перевірити що новий рядок є
        rows_after = run_query("SELECT * FROM test_reactive ORDER BY id")
        assert len(rows_after) == 2, f"Expected 2 rows after INSERT, got {len(rows_after)}"
        
        names = [r['data']['name'] for r in rows_after]
        assert 'initial' in names
        assert 'new_row' in names
        
        result.passed = True
        result.details = {
            "rows_before": 1,
            "rows_after": 2,
            "inserted_row": rows_after[1]['data']
        }
        
        # Cleanup
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_reactive CASCADE")
            conn.commit()
            
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_reactive_subscribe_update():
    """Тест реактивного subscribe - UPDATE детектується"""
    result = TestResult("Reactive Subscribe - UPDATE")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS test_reactive CASCADE;
                    CREATE TABLE test_reactive (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        value INT
                    );
                    INSERT INTO test_reactive (name, value) VALUES ('row1', 100), ('row2', 200);
                """)
            conn.commit()
        
        # Отримати початковий snapshot
        rows_before = run_query("SELECT * FROM test_reactive WHERE name = 'row1'")
        assert len(rows_before) == 1
        assert rows_before[0]['data']['value'] == 100
        
        # UPDATE рядка
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE test_reactive SET value = 999 WHERE name = 'row1'")
            conn.commit()
        
        # Перевірити що значення оновилось
        rows_after = run_query("SELECT * FROM test_reactive WHERE name = 'row1'")
        assert len(rows_after) == 1
        assert rows_after[0]['data']['value'] == 999, f"Expected value 999, got {rows_after[0]['data']['value']}"
        
        result.passed = True
        result.details = {
            "value_before": 100,
            "value_after": 999
        }
        
        # Cleanup
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_reactive CASCADE")
            conn.commit()
            
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_reactive_subscribe_delete():
    """Тест реактивного subscribe - DELETE детектується"""
    result = TestResult("Reactive Subscribe - DELETE")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS test_reactive CASCADE;
                    CREATE TABLE test_reactive (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        value INT
                    );
                    INSERT INTO test_reactive (name, value) VALUES ('row1', 100), ('row2', 200), ('row3', 300);
                """)
            conn.commit()
        
        # Отримати початковий snapshot
        rows_before = run_query("SELECT * FROM test_reactive")
        assert len(rows_before) == 3
        
        # DELETE рядка
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM test_reactive WHERE name = 'row2'")
            conn.commit()
        
        # Перевірити що рядок видалено
        rows_after = run_query("SELECT * FROM test_reactive ORDER BY id")
        assert len(rows_after) == 2, f"Expected 2 rows after DELETE, got {len(rows_after)}"
        
        names = [r['data']['name'] for r in rows_after]
        assert 'row1' in names
        assert 'row3' in names
        assert 'row2' not in names
        
        result.passed = True
        result.details = {
            "rows_before": 3,
            "rows_after": 2,
            "deleted_row": "row2"
        }
        
        # Cleanup
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_reactive CASCADE")
            conn.commit()
            
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


def test_subscribe_with_filter():
    """Тест subscribe з WHERE фільтром"""
    result = TestResult("Subscribe with WHERE filter")
    start = time.time()
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DROP TABLE IF EXISTS test_filter CASCADE;
                    CREATE TABLE test_filter (
                        id SERIAL PRIMARY KEY,
                        category TEXT,
                        value INT
                    );
                    INSERT INTO test_filter (category, value) VALUES 
                        ('A', 10), ('B', 20), ('A', 30), ('B', 40), ('A', 50);
                """)
            conn.commit()
        
        # Підписка тільки на категорію A
        rows = run_query("SELECT * FROM test_filter WHERE category = 'A' ORDER BY value")
        assert len(rows) == 3, f"Expected 3 rows with category A, got {len(rows)}"
        
        values = sorted([r['data']['value'] for r in rows])
        assert values == [10, 30, 50], f"Expected [10, 30, 50], got {values}"
        
        result.passed = True
        result.details = {"filtered_count": len(rows), "values": values}
        
        # Cleanup
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS test_filter CASCADE")
            conn.commit()
            
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


# ============================================================================
# ТЕСТИ: Tracked Tables
# ============================================================================

def test_tracked_tables():
    """Тест списку таблиць з тригерами"""
    result = TestResult("Tracked Tables")
    start = time.time()
    
    try:
        # Спочатку зробимо підписку щоб зареєструвати таблицю
        run_query("SELECT * FROM users LIMIT 1")
        
        with get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM pg_subscribe_tracked_tables()")
                tables = cur.fetchall()
                
                result.details = {"table_count": len(tables), "tables": [dict(t) for t in tables]}
                result.passed = True
    except Exception as e:
        result.error = str(e)
    
    result.duration = time.time() - start
    return result


# ============================================================================
# RUN ALL TESTS
# ============================================================================

def run_all_tests():
    """Запустити всі тести"""
    print("=" * 70)
    print("pg_subscribe Modern Test Suite")
    print("=" * 70)
    print()
    
    # Setup
    print("Setting up...")
    try:
        setup_extension()
        setup_test_data()
        print("Setup complete.\n")
    except Exception as e:
        print(f"Setup failed: {e}")
        return False
    
    # Групи тестів
    test_groups = [
        ("Query Normalization & Hashing (PostgreSQL native)", [
            test_query_normalization,
            test_query_hash_consistency,
        ]),
        ("Complex Queries", [
            test_complex_join,
            test_window_function,
            test_cte_query,
            test_subquery,
            test_aggregation_with_having,
            test_distinct_query,
        ]),
        ("Deduplication", [
            test_dedup_stats_available,
            test_dedup_list_queries,
            test_main_stats_include_dedup,
        ]),
        ("Query Analyzer", [
            test_analyzer_detects_window_functions,
            test_analyzer_detects_cte,
            test_analyzer_extracts_tables,
        ]),
        ("Tracked Tables", [
            test_tracked_tables,
        ]),
        ("Data Types Serialization", [
            test_all_data_types,
            test_null_values,
        ]),
        ("Reactive Subscribe (INSERT/UPDATE/DELETE)", [
            test_reactive_subscribe_insert,
            test_reactive_subscribe_update,
            test_reactive_subscribe_delete,
            test_subscribe_with_filter,
        ]),
    ]
    
    all_results = []
    total_passed = 0
    total_failed = 0
    
    for group_name, tests in test_groups:
        print(f"\n--- {group_name} ---")
        for test_func in tests:
            print(f"  {test_func.__name__}...", end=" ", flush=True)
            result = test_func()
            all_results.append(result)
            
            if result.passed:
                total_passed += 1
                print("✅")
            else:
                total_failed += 1
                print("❌")
                if result.error:
                    print(f"      Error: {result.error}")
    
    # Cleanup
    print("\n\nCleaning up...")
    try:
        cleanup_test_data()
        print("Cleanup complete.")
    except Exception as e:
        print(f"Cleanup failed: {e}")
    
    # Summary
    print()
    print("=" * 70)
    print(f"Results: {total_passed} passed, {total_failed} failed")
    print("=" * 70)
    
    if total_failed > 0:
        print("\n❌ Failed tests:")
        for result in all_results:
            if not result.passed:
                print(f"  - {result.name}: {result.error}")
    
    return total_failed == 0


if __name__ == "__main__":
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
