-- pg_subscribe Integration Tests
-- Run with: psql -f tests/integration_test.sql

\echo '============================================'
\echo 'pg_subscribe Integration Tests'
\echo '============================================'

-- Setup: Create extension if not exists
DROP EXTENSION IF EXISTS pg_subscribe CASCADE;
CREATE EXTENSION pg_subscribe;

\echo ''
\echo '--- Test 1: Basic subscription snapshot ---'
DROP TABLE IF EXISTS test_users CASCADE;
CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);
INSERT INTO test_users (name, email) VALUES 
    ('Alice', 'alice@test.com'),
    ('Bob', 'bob@test.com'),
    ('Charlie', 'charlie@test.com');

SELECT 
    CASE WHEN count(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Basic snapshot should return 3 rows' AS test_name
FROM (
    SELECT * FROM subscribe('SELECT * FROM test_users') LIMIT 3
) t;

\echo ''
\echo '--- Test 2: Numeric types support ---'
DROP TABLE IF EXISTS test_numeric_types CASCADE;
CREATE TABLE test_numeric_types (
    id SERIAL PRIMARY KEY,
    small_val SMALLINT,
    int_val INT,
    big_val BIGINT,
    numeric_val NUMERIC(10,2),
    float_val FLOAT4,
    double_val FLOAT8
);
INSERT INTO test_numeric_types (small_val, int_val, big_val, numeric_val, float_val, double_val)
VALUES (1, 100, 1000000000, 12345.67, 3.14, 2.718281828);

SELECT 
    CASE WHEN data->>'numeric_val' IS NOT NULL THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Numeric type should be serialized' AS test_name
FROM subscribe('SELECT * FROM test_numeric_types') LIMIT 1;

\echo ''
\echo '--- Test 3: Timestamp types support ---'
DROP TABLE IF EXISTS test_timestamps CASCADE;
CREATE TABLE test_timestamps (
    id SERIAL PRIMARY KEY,
    date_val DATE,
    time_val TIME,
    ts_val TIMESTAMP,
    tstz_val TIMESTAMPTZ
);
INSERT INTO test_timestamps (date_val, time_val, ts_val, tstz_val)
VALUES ('2024-01-15', '10:30:00', '2024-01-15 10:30:00', '2024-01-15 10:30:00+00');

SELECT 
    CASE WHEN data->>'date_val' IS NOT NULL AND data->>'ts_val' IS NOT NULL 
         THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Date and Timestamp types should be serialized' AS test_name
FROM subscribe('SELECT * FROM test_timestamps') LIMIT 1;

\echo ''
\echo '--- Test 4: UUID type support ---'
DROP TABLE IF EXISTS test_uuid CASCADE;
CREATE TABLE test_uuid (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT
);
INSERT INTO test_uuid (name) VALUES ('uuid_test');

SELECT 
    CASE WHEN data->>'id' ~ '^[0-9a-f-]{36}$' THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'UUID should be serialized as valid UUID string' AS test_name
FROM subscribe('SELECT * FROM test_uuid') LIMIT 1;

\echo ''
\echo '--- Test 5: JSON/JSONB types support ---'
DROP TABLE IF EXISTS test_json CASCADE;
CREATE TABLE test_json (
    id SERIAL PRIMARY KEY,
    json_val JSON,
    jsonb_val JSONB
);
INSERT INTO test_json (json_val, jsonb_val)
VALUES ('{"key": "value"}', '{"nested": {"array": [1, 2, 3]}}');

SELECT 
    CASE WHEN (data->'jsonb_val'->>'nested') IS NOT NULL THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'JSONB should be properly nested in output' AS test_name
FROM subscribe('SELECT * FROM test_json') LIMIT 1;

\echo ''
\echo '--- Test 6: Boolean type support ---'
DROP TABLE IF EXISTS test_bool CASCADE;
CREATE TABLE test_bool (id INT, active BOOLEAN);
INSERT INTO test_bool VALUES (1, true), (2, false);

SELECT 
    CASE WHEN (data->>'active')::boolean = true THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Boolean should be properly serialized' AS test_name
FROM subscribe('SELECT * FROM test_bool WHERE id = 1') LIMIT 1;

\echo ''
\echo '--- Test 7: Column names (not col_1, col_2) ---'
DROP TABLE IF EXISTS test_column_names CASCADE;
CREATE TABLE test_column_names (
    user_id INT,
    user_name TEXT,
    user_email TEXT
);
INSERT INTO test_column_names VALUES (1, 'Test', 'test@test.com');

SELECT 
    CASE WHEN data ? 'user_id' AND data ? 'user_name' AND NOT (data ? 'col_1') 
         THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Real column names should be used (not col_1, col_2)' AS test_name
FROM subscribe('SELECT user_id, user_name, user_email FROM test_column_names') LIMIT 1;

\echo ''
\echo '--- Test 8: NULL values handling ---'
DROP TABLE IF EXISTS test_nulls CASCADE;
CREATE TABLE test_nulls (
    id INT,
    nullable_text TEXT,
    nullable_int INT
);
INSERT INTO test_nulls VALUES (1, NULL, NULL);

SELECT 
    CASE WHEN data->'nullable_text' = 'null'::jsonb THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'NULL values should be serialized as JSON null' AS test_name
FROM subscribe('SELECT * FROM test_nulls') LIMIT 1;

\echo ''
\echo '--- Test 9: Query analyzer - simple query ---'
SELECT 
    CASE WHEN (pg_subscribe_analyze_query('SELECT * FROM test_users'))::jsonb ? 'referenced_tables' 
         THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Query analyzer should return analysis for simple query' AS test_name;

\echo ''
\echo '--- Test 10: Query analyzer - JOIN query ---'
SELECT 
    CASE WHEN ((pg_subscribe_analyze_query(
        'SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id'
    ))::jsonb->>'has_join')::boolean = true 
         THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Query analyzer should detect JOIN' AS test_name;

\echo ''
\echo '--- Test 11: Query analyzer - aggregation query ---'
SELECT 
    CASE WHEN ((pg_subscribe_analyze_query(
        'SELECT user_id, COUNT(*), SUM(amount) FROM orders GROUP BY user_id'
    ))::jsonb->>'has_aggregation')::boolean = true 
         THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Query analyzer should detect aggregation' AS test_name;

\echo ''
\echo '--- Test 12: Statistics function ---'
SELECT 
    CASE WHEN count(*) >= 3 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Statistics should return at least 3 rows' AS test_name
FROM pg_subscribe_stats();

\echo ''
\echo '--- Test 13: Deduplication stats ---'
SELECT 
    CASE WHEN count(*) >= 3 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Dedup stats should return at least 3 rows' AS test_name
FROM pg_subscribe_dedup_stats();

\echo ''
\echo '--- Test 13b: Query normalization (PostgreSQL native) ---'
SELECT 
    CASE WHEN pg_subscribe_normalize_query('SELECT  *  FROM  users') = 'SELECT * FROM users'
         THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Query normalization should collapse whitespace' AS test_name;

\echo ''
\echo '--- Test 13c: Query hash consistency ---'
SELECT 
    CASE WHEN pg_subscribe_query_hash('SELECT * FROM users') = 
              pg_subscribe_query_hash('SELECT  *  FROM  users')
         THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Same query with different whitespace should have same hash' AS test_name;

\echo ''
\echo '--- Test 14: Array types support ---'
DROP TABLE IF EXISTS test_arrays CASCADE;
CREATE TABLE test_arrays (
    id SERIAL PRIMARY KEY,
    int_arr INT[],
    text_arr TEXT[]
);
INSERT INTO test_arrays (int_arr, text_arr) VALUES (ARRAY[1,2,3], ARRAY['a','b','c']);

SELECT 
    CASE WHEN jsonb_typeof(data->'int_arr') = 'array' THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Integer arrays should be serialized as JSON arrays' AS test_name
FROM subscribe('SELECT * FROM test_arrays') LIMIT 1;

\echo ''
\echo '--- Test 15: mz_diff values (insert = 1) ---'
SELECT 
    CASE WHEN mz_diff = 1 THEN 'PASS' ELSE 'FAIL' END AS test_result,
    'Snapshot rows should have mz_diff = 1 (insert)' AS test_name
FROM subscribe('SELECT * FROM test_users') LIMIT 1;

\echo ''
\echo '============================================'
\echo 'Integration Tests Complete'
\echo '============================================'

-- Cleanup
DROP TABLE IF EXISTS test_users CASCADE;
DROP TABLE IF EXISTS test_numeric_types CASCADE;
DROP TABLE IF EXISTS test_timestamps CASCADE;
DROP TABLE IF EXISTS test_uuid CASCADE;
DROP TABLE IF EXISTS test_json CASCADE;
DROP TABLE IF EXISTS test_bool CASCADE;
DROP TABLE IF EXISTS test_column_names CASCADE;
DROP TABLE IF EXISTS test_nulls CASCADE;
DROP TABLE IF EXISTS test_arrays CASCADE;
