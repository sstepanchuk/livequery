# ============================================
# Stage 1: Build dependencies (cached layer)
# ============================================
FROM rust:slim-bookworm AS deps

# Install nightly toolchain for edition2024 support
RUN rustup default nightly

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies - only rebuild when Cargo files change
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src benches scripts && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > benches/perf.rs && \
    echo "fn main() {}" > scripts/test_db.rs && \
    echo "fn main() {}" > scripts/test_queries.rs && \
    cargo build --release --bin livequery-server && \
    rm -rf src benches scripts target/release/deps/livequery*

# ============================================
# Stage 2: Build application
# ============================================
FROM deps AS builder

COPY src ./src
COPY benches ./benches
COPY scripts ./scripts

RUN cargo build --release --bin livequery-server && \
    strip target/release/livequery-server

# ============================================
# Stage 3: Minimal runtime
# ============================================
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/livequery-server /usr/local/bin/

ENV RUST_LOG=info
EXPOSE 8080

CMD ["livequery-server"]
