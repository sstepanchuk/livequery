# Makefile for pg_subscribe PostgreSQL extension

EXTENSION = pg_subscribe
PG_CONFIG ?= pg_config

# Default PostgreSQL version
PG_VERSION ?= pg16

.PHONY: all build install test clean run package help

all: build

# Build the extension
build:
	cargo pgrx package --features $(PG_VERSION)

# Build for development (debug)
build-dev:
	cargo build

# Install to PostgreSQL
install:
	cargo pgrx install --release --features $(PG_VERSION)

# Install for development
install-dev:
	cargo pgrx install --features $(PG_VERSION)

# Run tests
test:
	cargo pgrx test $(PG_VERSION)

# Run unit tests only (no PostgreSQL)
test-unit:
	cargo test --lib

# Run PostgreSQL with extension loaded
run:
	cargo pgrx run $(PG_VERSION)

# Clean build artifacts
clean:
	cargo clean
	rm -rf target/

# Format code
fmt:
	cargo fmt

# Lint code
lint:
	cargo clippy -- -D warnings

# Check code without building
check:
	cargo check

# Generate documentation
doc:
	cargo doc --no-deps --open

# Package for distribution
package:
	cargo pgrx package --features $(PG_VERSION)
	@echo "Package created in target/release/$(EXTENSION)-$(PG_VERSION)"

# Create schema dump
schema:
	cargo pgrx schema $(PG_VERSION)

# Help
help:
	@echo "pg_subscribe Makefile targets:"
	@echo ""
	@echo "  build       - Build the extension (release)"
	@echo "  build-dev   - Build for development (debug)"
	@echo "  install     - Install to PostgreSQL"
	@echo "  install-dev - Install development version"
	@echo "  test        - Run all tests"
	@echo "  test-unit   - Run unit tests only"
	@echo "  run         - Start PostgreSQL with extension"
	@echo "  clean       - Clean build artifacts"
	@echo "  fmt         - Format code"
	@echo "  lint        - Lint code with clippy"
	@echo "  check       - Check code without building"
	@echo "  doc         - Generate documentation"
	@echo "  package     - Package for distribution"
	@echo "  schema      - Generate SQL schema"
	@echo ""
	@echo "Environment variables:"
	@echo "  PG_VERSION  - PostgreSQL version (default: pg16)"
	@echo "                Options: pg13, pg14, pg15, pg16, pg17"
	@echo ""
	@echo "Examples:"
	@echo "  make install PG_VERSION=pg15"
	@echo "  make test PG_VERSION=pg16"
