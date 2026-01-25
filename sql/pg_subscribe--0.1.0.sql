-- pg_subscribe extension SQL definitions
-- Version: 0.1.0

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_subscribe" to load this file. \quit

-- Subscribe to a SQL query and receive streaming updates
-- Returns a set of rows with: mz_timestamp, mz_diff, mz_progressed, data
CREATE OR REPLACE FUNCTION subscribe(
    query text
) RETURNS TABLE (
    mz_timestamp bigint,
    mz_diff integer,
    mz_progressed boolean,
    data jsonb
)
LANGUAGE c
AS 'MODULE_PATHNAME', 'subscribe_wrapper';

COMMENT ON FUNCTION subscribe(text) IS 
'Subscribe to changes in a SQL query result. Returns streaming updates with:
- mz_timestamp: Logical timestamp of the change
- mz_diff: +1 for insert, -1 for delete
- mz_progressed: true for heartbeat/progress messages
- data: The row data as JSONB';

-- Subscribe with additional options
CREATE OR REPLACE FUNCTION subscribe_with_options(
    query text,
    snapshot boolean DEFAULT true,
    progress boolean DEFAULT true
) RETURNS TABLE (
    mz_timestamp bigint,
    mz_diff integer,
    mz_progressed boolean,
    data jsonb
)
LANGUAGE c
AS 'MODULE_PATHNAME', 'subscribe_with_options_wrapper';

COMMENT ON FUNCTION subscribe_with_options(text, boolean, boolean) IS 
'Subscribe to changes with options:
- query: The SQL SELECT query to subscribe to
- snapshot: Include initial snapshot (default: true)
- progress: Include progress/heartbeat messages (default: true)';

-- List all active subscriptions
CREATE OR REPLACE FUNCTION pg_subscribe_list_active()
RETURNS TABLE (
    subscription_id text,
    query text,
    created_at timestamptz,
    events_sent bigint,
    backend_pid integer
)
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_subscribe_list_active_wrapper';

COMMENT ON FUNCTION pg_subscribe_list_active() IS 
'List all currently active subscriptions';

-- Cancel a specific subscription
CREATE OR REPLACE FUNCTION pg_subscribe_cancel(
    subscription_id text
) RETURNS boolean
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_subscribe_cancel_wrapper';

COMMENT ON FUNCTION pg_subscribe_cancel(text) IS 
'Cancel an active subscription by its ID. Returns true if found and cancelled.';

-- Get extension statistics
CREATE OR REPLACE FUNCTION pg_subscribe_stats()
RETURNS TABLE (
    stat_name text,
    stat_value bigint
)
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_subscribe_stats_wrapper';

COMMENT ON FUNCTION pg_subscribe_stats() IS 
'Get pg_subscribe extension statistics';

-- Analyze query for IVM compatibility
CREATE OR REPLACE FUNCTION pg_subscribe_analyze_query(
    query text
) RETURNS jsonb
LANGUAGE c
AS 'MODULE_PATHNAME', 'pg_subscribe_analyze_query_wrapper';

COMMENT ON FUNCTION pg_subscribe_analyze_query(text) IS 
'Analyze a query for IVM (Incremental View Maintenance) compatibility.
Returns a JSON object with analysis results including:
- is_valid: Whether the query is valid SQL
- ivm_compatible: Whether IVM can be used
- referenced_tables: Tables in the query
- has_join, has_aggregation, has_group_by: Query features
- complexity_score: Estimated query complexity (0-100)';

-- Views for monitoring

CREATE OR REPLACE VIEW pg_subscribe_active AS
SELECT * FROM pg_subscribe_list_active();

COMMENT ON VIEW pg_subscribe_active IS 
'View of all active subscriptions';

CREATE OR REPLACE VIEW pg_subscribe_statistics AS
SELECT * FROM pg_subscribe_stats();

COMMENT ON VIEW pg_subscribe_statistics IS 
'View of extension statistics';
