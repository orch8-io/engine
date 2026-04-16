# ── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests first for layer caching.
COPY Cargo.toml Cargo.lock ./
COPY orch8-types/Cargo.toml orch8-types/Cargo.toml
COPY orch8-storage/Cargo.toml orch8-storage/Cargo.toml
COPY orch8-engine/Cargo.toml orch8-engine/Cargo.toml
COPY orch8-api/Cargo.toml orch8-api/Cargo.toml
COPY orch8-grpc/Cargo.toml orch8-grpc/Cargo.toml
COPY orch8-server/Cargo.toml orch8-server/Cargo.toml
COPY orch8-cli/Cargo.toml orch8-cli/Cargo.toml

# Create dummy source files so cargo can resolve the workspace and cache deps.
RUN mkdir -p orch8-types/src && echo "" > orch8-types/src/lib.rs \
    && mkdir -p orch8-storage/src && echo "" > orch8-storage/src/lib.rs \
    && mkdir -p orch8-engine/src && echo "" > orch8-engine/src/lib.rs \
    && mkdir -p orch8-api/src && echo "" > orch8-api/src/lib.rs \
    && mkdir -p orch8-grpc/src && echo "" > orch8-grpc/src/lib.rs \
    && mkdir -p orch8-server/src && echo "fn main() {}" > orch8-server/src/main.rs \
    && mkdir -p orch8-cli/src && echo "fn main() {}" > orch8-cli/src/main.rs

# Proto file needed for grpc build.rs.
COPY proto/ proto/
COPY orch8-grpc/build.rs orch8-grpc/build.rs

# Build dependencies only (cached unless Cargo.toml/lock changes).
RUN cargo build --release --bin orch8-server --bin orch8 2>/dev/null || true

# Copy real source and rebuild.
COPY . .
# Touch all source files so cargo knows they changed.
RUN find orch8-*/src -name "*.rs" -exec touch {} + \
    && cargo build --release --bin orch8-server --bin orch8

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/orch8-server /usr/local/bin/orch8-server
COPY --from=builder /app/target/release/orch8 /usr/local/bin/orch8

# Default SQLite storage (zero-config start).
ENV ORCH8_DATABASE_BACKEND=sqlite \
    ORCH8_DATABASE_URL=sqlite:///data/orch8.db \
    ORCH8_HTTP_ADDR=0.0.0.0:8080 \
    ORCH8_GRPC_ADDR=0.0.0.0:50051

RUN mkdir -p /data

EXPOSE 8080 50051

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/orch8", "--url", "http://127.0.0.1:8080", "health"]

ENTRYPOINT ["/usr/local/bin/orch8-server"]
