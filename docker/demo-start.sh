#!/bin/sh
set -e

# Start PostgreSQL
docker-entrypoint.sh postgres -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4 -c log_min_messages=fatal &

# Wait for PostgreSQL
echo "Waiting for PostgreSQL..."
until pg_isready -U livequery -q; do sleep 1; done
echo "PostgreSQL ready"

# Start NATS
nats-server -c /etc/nats.conf &
sleep 1

# Start LiveQuery
DATABASE_URL="postgres://livequery:livequery@localhost:5432/livequery" \
NATS_URL="nats://localhost:4222" \
WAL_SLOT="livequery_slot" \
WAL_PUBLICATION="livequery_pub" \
RUST_LOG="info" \
livequery-server &

# Start SQL API
DATABASE_URL="postgres://livequery:livequery@localhost:5432/livequery" \
node /app/sql-api/server.js &

# Start Nginx (foreground)
echo "LiveQuery Demo ready at http://localhost:3000"
nginx -g "daemon off;"
