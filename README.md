# LiveQuery Server

Real-time SQL query subscriptions for PostgreSQL via NATS.

[![CI](https://github.com/sstepanchuk/livequery/actions/workflows/ci.yml/badge.svg)](https://github.com/sstepanchuk/livequery/actions/workflows/ci.yml)
[![Release](https://github.com/sstepanchuk/livequery/actions/workflows/release.yml/badge.svg)](https://github.com/sstepanchuk/livequery/releases)

> ⚠️ **Alpha Version** — This project is under active development and testing. API may change. Not recommended for production use yet.

## What is it?

LiveQuery lets you subscribe to SQL query results. When data changes in PostgreSQL, clients automatically receive updates.

## Installation

### From Releases (Recommended)

Download pre-built binaries from [GitHub Releases](https://github.com/sstepanchuk/livequery/releases):

| Platform | Architecture | File |
|----------|--------------|------|
| Linux | x86_64 | `livequery-linux-x86_64.tar.gz` |
| Linux | aarch64 | `livequery-linux-aarch64.tar.gz` |
| macOS | x86_64 | `livequery-macos-x86_64.tar.gz` |
| macOS | Apple Silicon | `livequery-macos-aarch64.tar.gz` |
| Windows | x86_64 | `livequery-windows-x86_64.zip` |

```bash
# Example: Linux x86_64
curl -LO https://github.com/sstepanchuk/livequery/releases/latest/download/livequery-linux-x86_64.tar.gz
tar -xzf livequery-linux-x86_64.tar.gz
./livequery-server
```

### Docker Image

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/sstepanchuk/livequery:latest

# Run
docker run -d \
  -e DATABASE_URL=postgres://user:pass@host/db \
  -e NATS_URL=nats://nats:4222 \
  ghcr.io/sstepanchuk/livequery:latest
```

Available tags:
- `ghcr.io/sstepanchuk/livequery:latest` - Latest stable release
- `ghcr.io/sstepanchuk/livequery:0.1.2` - Specific version

### Build from Source

```bash
git clone https://github.com/sstepanchuk/livequery.git
cd livequery
cargo build --release
./target/release/livequery-server
```

## Quick Start

### Docker Compose (Recommended)

```bash
cd docker
docker-compose up -d
open http://localhost:3000
```

That's it! PostgreSQL, NATS, LiveQuery server, and test frontend are all running.

## Quick Start (Manual)

### 1. Setup PostgreSQL

```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
-- Restart PostgreSQL

SELECT pg_create_logical_replication_slot('livequery_slot', 'pgoutput');
CREATE PUBLICATION livequery_pub FOR ALL TABLES;
```

### 2. Run Server

```bash
DATABASE_URL=postgres://user:pass@localhost/mydb \
NATS_URL=nats://localhost:4222 \
cargo run --release
```

### 3. Subscribe

```javascript
const response = await nats.request('livequery.orders.subscribe', {
  query: "SELECT * FROM orders WHERE user_id = 123",
  mode: "events"  // or "snapshot"
});

nats.subscribe(response.subject, (msg) => {
  console.log('Changes:', msg.events);
});
```

## Two Modes

| Mode | Description | Use When |
|------|-------------|----------|
| **events** | Individual INSERT/UPDATE/DELETE events | Client maintains local state |
| **snapshot** | Full data on every change | Simple client (just displays data) |

### Events Mode (default)
```json
// Initial response
{ "snapshot": [{"mz_diff": 1, "data": {"id": 1, "name": "Order 1"}}] }

// On change (ts = server timestamp in ms for latency calculation)
{ "seq": 5, "ts": 1706400000000, "events": [
  {"mz_diff": -1, "data": {"id": 1, "name": "Order 1"}},
  {"mz_diff": 1,  "data": {"id": 1, "name": "Updated"}}
]}
```

### Snapshot Mode
```json
// Initial response
{ "rows": [{"id": 1, "name": "Order 1"}, {"id": 2, "name": "Order 2"}] }

// On any change - full new snapshot
{ "seq": 5, "ts": 1706400000000, "rows": [{"id": 1, "name": "Updated"}, {"id": 2, "name": "Order 2"}] }
```

### Latency Measurement
Each event batch includes `ts` (server timestamp in milliseconds). Calculate latency:
```javascript
const latency = Date.now() - batch.ts; // ms from server to client
```

## Pros

- **Simple** - Plain SQL, no new languages
- **Fast** - Rust + native PostgreSQL streaming, <10ms latency
- **Shared subscriptions** - Same queries share one snapshot
- **WHERE optimization** - Filters evaluated server-side, skip unnecessary requery
- **Two modes** - Events for complex clients, snapshot for simple ones

## Cons / Limitations

- **Requery on change** - Full SELECT on change (not incremental)
- **Memory** - Each subscription snapshot in RAM
- **PostgreSQL only** - Requires logical replication
- **Simple queries** - Complex JOINs/subqueries may be slow

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `NATS_URL` | `nats://localhost:4222` | NATS server |
| `WAL_SLOT` | `livequery_slot` | Replication slot name |
| `WAL_PUBLICATION` | `livequery_pub` | Publication name |
| `CLIENT_TIMEOUT_SECS` | `30` | Inactive client timeout |
| `MAX_SUBSCRIPTIONS` | `10000` | Max subscriptions |
| `DB_POOL_SIZE` | `16` | Connection pool size |

## NATS API

| Subject | Description |
|---------|-------------|
| `livequery.{subscription_id}.subscribe` | Subscribe to query (request-reply) |
| `livequery.{subscription_id}.unsubscribe` | Unsubscribe |
| `livequery.{subscription_id}.heartbeat` | Keep-alive |
| `livequery.{subscription_id}.events` | Receive updates (subscribe to this after registering) |
| `livequery.health` | Health check - returns server stats |

### Health Check

```bash
# Using NATS CLI
nats req livequery.health ""

# Response:
{
  "status": "healthy",
  "server_id": "lq-abc123",
  "subscriptions": 5,
  "queries": 2,
  "msgs_in": 150,
  "msgs_out": 300
}
```

## Project Structure

```
docker/
├── docker-compose.yml   # All services
├── Dockerfile           # LiveQuery server
├── init.sql             # Sample data
├── data/                # Container volumes
└── frontend/            # Test UI
```

## How It Works

```
┌─────────────────────────────────────────────────────────────┐
│  Client                                                     │
│    │                                                        │
│    ├─► NATS.request("livequery.{id}.subscribe", {query})   │
│    │                                                        │
│    ◄── Initial snapshot/events                              │
│    │                                                        │
│    ◄── Live updates on changes                              │
└─────────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│  LiveQuery Server                                           │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ Subscription │    │  Snapshot   │    │   WHERE     │     │
│  │   Manager    │    │   Cache     │    │   Filter    │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ WAL Streamer (pgoutput binary protocol)              │   │
│  │  └─► Native PostgreSQL replication stream            │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│  PostgreSQL (wal_level=logical)                             │
└─────────────────────────────────────────────────────────────┘
```

## Programmatic Configuration

For testing or embedding, use the builder pattern:

```rust
use livequery_server::core::Config;

let cfg = Config::builder()
    .db_url("postgres://user:pass@localhost/mydb")
    .nats_url("nats://localhost:4222")
    .max_subscriptions(5000)
    .client_timeout_secs(60)
    .build()?;
```

## License

MIT
