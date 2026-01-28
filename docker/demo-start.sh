#!/bin/sh
set -e

echo "Starting LiveQuery Test UI..."
echo "DATABASE_URL: $DATABASE_URL"
echo "NATS_URL: $NATS_URL"

# Start LiveQuery server
livequery-server &

# Start SQL API
node /app/sql-api/server.js &

# Start Nginx (foreground)
echo "Ready at http://localhost:3000"
nginx -g "daemon off;"
