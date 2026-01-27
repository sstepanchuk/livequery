# LiveQuery Server

Real-time SQL query subscriptions for PostgreSQL via NATS.

## What is it?

LiveQuery lets you subscribe to SQL query results. When data changes in PostgreSQL, clients automatically receive updates.

## Quick Start with Docker

```bash
cd docker
docker-compose up -d

# Open test frontend
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
const subscriptionId = "client-1-orders";
const response = await nats.request(`livequery.${subscriptionId}.subscribe`, {
  subscription_id: subscriptionId,
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

## License

MIT
