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
COPY orch8-mobile/Cargo.toml orch8-mobile/Cargo.toml
COPY orch8-publisher/Cargo.toml orch8-publisher/Cargo.toml
COPY orch8-push/Cargo.toml orch8-push/Cargo.toml

# Create dummy source files so cargo can resolve the workspace and cache deps.
RUN mkdir -p orch8-types/src && echo "" > orch8-types/src/lib.rs \
    && mkdir -p orch8-storage/src && echo "" > orch8-storage/src/lib.rs \
    && mkdir -p orch8-engine/src && echo "" > orch8-engine/src/lib.rs \
    && mkdir -p orch8-api/src && echo "" > orch8-api/src/lib.rs \
    && mkdir -p orch8-grpc/src && echo "" > orch8-grpc/src/lib.rs \
    && mkdir -p orch8-server/src && echo "fn main() {}" > orch8-server/src/main.rs \
    && mkdir -p orch8-cli/src && echo "fn main() {}" > orch8-cli/src/main.rs \
    && mkdir -p orch8-mobile/src && echo "" > orch8-mobile/src/lib.rs \
    && mkdir -p orch8-publisher/src && echo "" > orch8-publisher/src/lib.rs \
    && mkdir -p orch8-push/src && echo "" > orch8-push/src/lib.rs

# Proto file needed for grpc build.rs.
COPY proto/ proto/
COPY orch8-grpc/build.rs orch8-grpc/build.rs

# dashboard/ is excluded via .dockerignore (not needed at runtime),
# but RustEmbed in orch8-cli requires the folder to exist at compile time.
RUN mkdir -p dashboard/dist

# Cargo profile: "release" for production, "docker-ci" in CI smoke tests
# (skips LTO + single-codegen-unit to stay within runner time limits).
ARG CARGO_PROFILE=release

# Build dependencies only (cached unless Cargo.toml/lock changes).
RUN cargo build --profile ${CARGO_PROFILE} --bin orch8-server --bin orch8 2>/dev/null || true

# Copy real source and rebuild.
COPY . .
RUN mkdir -p dashboard/dist
# Touch all source files so cargo knows they changed.
RUN find orch8-*/src -name "*.rs" -exec touch {} + \
    && cargo build --profile ${CARGO_PROFILE} --bin orch8-server --bin orch8

# Copy binaries to a fixed location so stage 2 doesn't need to know the profile.
# /app/bin avoids colliding with the orch8-server/ crate directory from COPY . .
RUN mkdir -p /app/bin \
    && cp target/${CARGO_PROFILE}/orch8-server /app/bin/orch8-server \
    && cp target/${CARGO_PROFILE}/orch8 /app/bin/orch8

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system orch8 && useradd --system --gid orch8 orch8

COPY --from=builder /app/bin/orch8-server /usr/local/bin/orch8-server
COPY --from=builder /app/bin/orch8 /usr/local/bin/orch8

# Default SQLite storage (zero-config start).
ENV ORCH8_STORAGE_BACKEND=sqlite \
    ORCH8_DATABASE_URL=sqlite:///data/orch8.db \
    ORCH8_HTTP_ADDR=0.0.0.0:8080 \
    ORCH8_GRPC_ADDR=0.0.0.0:50051

RUN mkdir -p /data && chown orch8:orch8 /data

EXPOSE 8080 50051

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/orch8", "--url", "http://127.0.0.1:8080", "health"]

USER orch8

ENTRYPOINT ["/usr/local/bin/orch8-server"]
