# Contributing to Orch8

Thank you for your interest in contributing to Orch8!

## Getting Started

### Prerequisites

- Rust 1.80+
- PostgreSQL 16 (or use Docker Compose)
- Node.js 22 (for E2E tests)
- protoc (Protocol Buffers compiler)

### Build

```bash
# Start Postgres
docker compose up -d

# Build everything
cargo build --workspace

# Run tests
cargo test --workspace

# Lint
cargo clippy --workspace -- -D warnings

# Format
cargo fmt --check
```

### E2E Tests

```bash
cargo build --bin orch8-server
for f in tests/e2e/*.test.js; do node --test "$f"; done
```

## Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Make your changes
4. Ensure all tests pass (`cargo test --workspace`)
5. Ensure clippy is clean (`cargo clippy --workspace -- -D warnings`)
6. Ensure formatting is correct (`cargo fmt --check`)
7. Commit with a descriptive message
8. Open a pull request

## Reporting Issues

Open an issue on GitHub with:
- Steps to reproduce
- Expected behavior
- Actual behavior
- Orch8 version and environment details

## Code Style

- Follow standard Rust conventions
- Use `clippy` with `-D warnings`
- Use `rustfmt` for formatting
- Add tests for new functionality
