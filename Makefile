.PHONY: lint lint-fmt lint-clippy lint-deny lint-machete lint-doc \
       lint-fix test check ci

# ---------------------------------------------------------------------------
# Individual lint targets
# ---------------------------------------------------------------------------

lint-fmt:
	cargo fmt --check

lint-clippy:
	SQLX_OFFLINE=true cargo clippy --workspace --all-targets -- -D warnings

lint-deny:
	cargo deny check

lint-machete:
	cargo machete

lint-doc:
	SQLX_OFFLINE=true RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items

# ---------------------------------------------------------------------------
# Composite targets
# ---------------------------------------------------------------------------

# Fast lint: fmt + clippy (seconds, no network)
lint: lint-fmt lint-clippy

# Full lint: everything including supply-chain and docs
lint-all: lint-fmt lint-clippy lint-deny lint-machete lint-doc

# Auto-fix what can be auto-fixed
lint-fix:
	cargo fmt
	cargo clippy --workspace --all-targets --fix --allow-dirty -- -D warnings

# ---------------------------------------------------------------------------
# Build & test
# ---------------------------------------------------------------------------

check:
	SQLX_OFFLINE=true cargo check --workspace

test:
	cargo test --workspace

# Mirror CI: the full sequence that must pass before merge
ci: lint-fmt lint-clippy lint-deny check test
