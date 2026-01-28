.PHONY: build run test clean docker-up docker-down fmt lint check help

build:
	cargo build --release

run:
	cargo run

test:
	cargo test

clean:
	cargo clean

fmt:
	cargo fmt

lint:
	cargo clippy -- -D warnings

check:
	cargo check

# Docker commands
docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f livequery

docker-build:
	docker-compose build

# Development with auto-reload (requires cargo-watch)
watch:
	cargo watch -x run

setup-hooks:
	git config core.hooksPath .githooks
	@echo "✅ Git hooks configured"

help:
	@echo "LiveQuery Server Makefile"
	@echo ""
	@echo "  build        - Build release binary"
	@echo "  run          - Run server locally"
	@echo "  test         - Run tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  fmt          - Format code"
	@echo "  lint         - Lint with clippy"
	@echo "  check        - Check without building"
	@echo ""
	@echo "Docker:"
	@echo "  docker-up    - Start all services"
	@echo "  docker-down  - Stop all services"
	@echo "  docker-logs  - Follow livequery logs"
	@echo "  docker-build - Rebuild containers"
	@echo ""
	@echo "Development:"
	@echo "  watch        - Run with auto-reload"
	@echo "  setup-hooks  - Configure git pre-commit hooks"
