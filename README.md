# pg_subscribe

**Reactive SUBSCRIBE for PostgreSQL** — Materialize-compatible change data streaming for any SQL query.

[![PostgreSQL 13-17](https://img.shields.io/badge/PostgreSQL-13--17-336791?logo=postgresql)](https://www.postgresql.org/)
[![Rust](https://img.shields.io/badge/Rust-pgrx-orange?logo=rust)](https://github.com/pgcentralfoundation/pgrx)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Features

- 🔄 **Reactive Subscriptions** — Subscribe to any SELECT query and receive real-time change events
- 🎯 **Materialize-Compatible** — Uses `mz_timestamp`, `mz_diff`, `mz_progressed` semantics
- 🔗 **Complex Query Support** — JOINs, aggregations, CTEs, window functions, subqueries
- ⚡ **Shared Triggers** — One trigger per table, fan-out to all subscribers
- 🔁 **Query Deduplication** — Identical queries share the same subscription slot
- 📊 **Full Type Support** — All PostgreSQL types serialized to JSON correctly
- 🦀 **Built with Rust** — Memory-safe implementation using pgrx

## Quick Start

```bash
# Install pgrx
cargo install cargo-pgrx
cargo pgrx init --pg16 /path/to/pg_config

# Build and install
cargo pgrx install --release

# Enable in PostgreSQL (add to postgresql.conf for shared memory)
# shared_preload_libraries = 'pg_subscribe'
```

```sql
CREATE EXTENSION pg_subscribe;

-- Subscribe to a query
SELECT * FROM subscribe('SELECT * FROM users WHERE active = true');
```

## Output Format

| Column | Type | Description |
|--------|------|-------------|
| `mz_timestamp` | `bigint` | PostgreSQL transaction timestamp |
| `mz_diff` | `int` | `+1` = INSERT, `-1` = DELETE |
| `mz_progressed` | `bool` | `true` = heartbeat, `false` = data |
| `data` | `jsonb` | Row data with proper types |

### Example Output

```
 mz_timestamp    | mz_diff | mz_progressed | data
-----------------+---------+---------------+------------------------------------
 822693589867596 |       1 | f             | {"id": 1, "name": "Alice", "active": true}
 822693589867596 |       1 | f             | {"id": 2, "name": "Bob", "active": true}
```

### Update Semantics

Updates emit two events (like Materialize):
```sql
UPDATE users SET name = 'Alice Smith' WHERE id = 1;
-- Results in:
-- mz_diff = -1, data = {"id": 1, "name": "Alice", ...}      -- DELETE old
-- mz_diff = +1, data = {"id": 1, "name": "Alice Smith", ...} -- INSERT new
```

## API Reference

### Core Functions

```sql
-- Reactive subscription (streams continuously)
SELECT * FROM subscribe(
    query TEXT,
    identity_columns TEXT[] DEFAULT NULL  -- optional: columns for efficient diffing
);

-- Snapshot only (returns current state, then completes)
SELECT * FROM subscribe_snapshot(query TEXT, identity_columns TEXT[] DEFAULT NULL);

-- Prepare subscription (returns subscription_id for LISTEN)
SELECT pg_subscribe_prepare(query TEXT);
```

### Analysis & Monitoring

```sql
-- Analyze query for IVM compatibility
SELECT pg_subscribe_analyze_query('SELECT * FROM users JOIN orders ON ...');

-- View statistics
SELECT * FROM pg_subscribe_stats();

-- View tracked tables with triggers
SELECT * FROM pg_subscribe_tracked_tables();

-- View deduplicated queries
SELECT * FROM pg_subscribe_deduplicated_queries();
```

### Utility Functions

```sql
-- Normalize query (for deduplication testing)
SELECT pg_subscribe_normalize_query('SELECT  *  FROM  users');

-- Compute query hash
SELECT pg_subscribe_query_hash('SELECT * FROM users');
```

## Supported Query Types

| Query Type | Support | Notes |
|------------|---------|-------|
| Simple SELECT | ✅ Full | |
| WHERE filters | ✅ Full | |
| ORDER BY, LIMIT | ✅ Full | |
| INNER JOIN | ✅ Full | |
| LEFT/RIGHT JOIN | ⚠️ Limited | Works via snapshot-diff |
| GROUP BY + aggregates | ✅ Full | COUNT, SUM, AVG, MIN, MAX |
| DISTINCT | ✅ Full | |
| Subqueries | ⚠️ Limited | Works via snapshot-diff |
| CTE (WITH) | ⚠️ Limited | Works via snapshot-diff |
| Window Functions | ⚠️ Limited | Works via snapshot-diff |

## Configuration

```sql
-- Heartbeat interval (default: 1000ms)
SET pg_subscribe.heartbeat_interval_ms = 500;
```

Compile-time constants (in `src/shmem.rs`):
- `MAX_SLOTS = 64` — Maximum concurrent subscriptions
- `MAX_EVENTS_PER_SLOT = 32` — Event buffer size per subscription
- `MAX_TRACKED_TABLES = 32` — Maximum tables with triggers

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Client                                  │
│              SELECT * FROM subscribe(...)                    │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   pg_subscribe                               │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐   │
│  │   Query     │  │   Snapshot   │  │  Shared Memory    │   │
│  │  Analyzer   │  │    Diff      │  │   Ring Buffers    │   │
│  └─────────────┘  └──────────────┘  └───────────────────┘   │
│         │                │                    ▲              │
│         ▼                ▼                    │              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │            Shared Triggers (1 per table)             │    │
│  │   users: _pgsub_shared_users                         │    │
│  │   orders: _pgsub_shared_orders                       │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Client Examples

### Python

```python
import psycopg2

conn = psycopg2.connect("dbname=mydb")
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("SELECT * FROM subscribe('SELECT * FROM users')")
    for mz_timestamp, mz_diff, mz_progressed, data in cur:
        if mz_progressed:
            print(f"Heartbeat at {mz_timestamp}")
        elif mz_diff == 1:
            print(f"INSERT: {data}")
        else:
            print(f"DELETE: {data}")
```

### Node.js

```javascript
const { Client } = require('pg');

const client = new Client();
await client.connect();

const res = await client.query(
  "SELECT * FROM subscribe('SELECT * FROM users')"
);

for (const row of res.rows) {
  if (row.mz_progressed) {
    console.log('Heartbeat:', row.mz_timestamp);
  } else if (row.mz_diff === 1) {
    console.log('INSERT:', row.data);
  } else {
    console.log('DELETE:', row.data);
  }
}
```

## Development

```bash
# Run tests
cargo pgrx test pg16

# Run Python integration tests
source .venv/bin/activate
python tests/test_modern.py

# Development server
cargo pgrx run pg16
```

## Requirements

- PostgreSQL 13, 14, 15, 16, or 17
- Rust 1.70+
- For shared memory features: add to `postgresql.conf`:
  ```
  shared_preload_libraries = 'pg_subscribe'
  ```

## License

MIT

## Acknowledgments

- Inspired by [Materialize](https://materialize.com/) SUBSCRIBE protocol
- Built with [pgrx](https://github.com/pgcentralfoundation/pgrx)
- SQL parsing via [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)
