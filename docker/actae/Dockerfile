#  Stage 1: Build Actae binary (Rust) 
FROM --platform=$BUILDPLATFORM rust:1.88-bookworm AS actae-builder

ARG BUILDPLATFORM
ARG TARGETPLATFORM

RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Cross-compilation support for production amd64 deploys
RUN if [ "$TARGETPLATFORM" != "$BUILDPLATFORM" ]; then \
    case "$TARGETPLATFORM" in \
        linux/amd64) \
            apt-get update && apt-get install -y gcc-x86-64-linux-gnu && rm -rf /var/lib/apt/lists/* ; \
            rustup target add x86_64-unknown-linux-gnu ; \
            echo '[target.x86_64-unknown-linux-gnu]' >> $CARGO_HOME/config.toml ; \
            echo 'linker = "x86_64-linux-gnu-gcc"' >> $CARGO_HOME/config.toml ;; \
        linux/arm64) \
            apt-get update && apt-get install -y gcc-aarch64-linux-gnu && rm -rf /var/lib/apt/lists/* ; \
            rustup target add aarch64-unknown-linux-gnu ; \
            echo '[target.aarch64-unknown-linux-gnu]' >> $CARGO_HOME/config.toml ; \
            echo 'linker = "aarch64-linux-gnu-gcc"' >> $CARGO_HOME/config.toml ;; \
    esac; \
    fi

WORKDIR /app
COPY actae/Cargo.toml actae/Cargo.lock ./
ARG CACHE_BUST
COPY actae/src ./src
# build.rs emits build-time env vars (git SHA, license pubkey, PG jar path)
# and verifies the embedded PostgreSQL layout against the pinned checksums.
COPY actae/build.rs ./
COPY actae/pg-checksums.json ./
# API contract manifest is include_str!'d by src/api/*.rs, src/auth/*.rs and
# src/federation_gateway/*.rs at compile time. The include path resolves
# relative to the crate root (/app): src/api/../../../spec/... -> /spec/...
COPY spec/api-parity/v1/manifest.json /spec/api-parity/v1/manifest.json

ARG BUILD_PROFILE=release
ARG FEATURES=rustls
RUN echo "features=${FEATURES}"
RUN echo "cache_bust=${CACHE_BUST:-0}" > /dev/null && \
    if [ "$BUILD_PROFILE" = "debug" ]; then \
        if [ "$TARGETPLATFORM" != "$BUILDPLATFORM" ] && [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
            cargo build --features "${FEATURES}" --target x86_64-unknown-linux-gnu; \
            mkdir -p /app/out; \
            cp target/x86_64-unknown-linux-gnu/debug/actae /app/out/actae; \
        else \
            cargo build --features "${FEATURES}"; \
            mkdir -p /app/out; \
            cp target/debug/actae /app/out/actae; \
        fi; \
    else \
        if [ "$TARGETPLATFORM" != "$BUILDPLATFORM" ] && [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
            cargo build --release --features "${FEATURES}" --target x86_64-unknown-linux-gnu; \
            mkdir -p /app/out; \
            cp target/x86_64-unknown-linux-gnu/release/actae /app/out/actae; \
        else \
            cargo build --release --features "${FEATURES}"; \
            mkdir -p /app/out; \
            cp target/release/actae /app/out/actae; \
        fi; \
    fi

#  Stage 2: Build dashboard (Next.js static export) 
FROM node:22-alpine AS dashboard-builder
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json* ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build
# Output is in /app/out/

#  Stage 3: Final runtime image 
FROM --platform=$TARGETPLATFORM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 libpq5 ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Actae binary
COPY --from=actae-builder /app/out/actae /usr/local/bin/actae

# Copy dashboard static files
COPY --from=dashboard-builder /app/out/ /app/dashboard/

# Non-root runtime user (least privilege). HOME is writable so enrollment
# (`~/.actae/enrollment.json`) and log files work without root. The
# entrypoint copies the read-only certs mount into /app/certs and drops
# privileges with setpriv before exec'ing Actae.
RUN groupadd --system --gid 1001 actae \
    && useradd --system --uid 1001 --gid actae --home /home/actae actae \
    && mkdir -p /app/logs /home/actae \
    && chown -R actae:actae /app /home/actae

ENV HOME=/home/actae

COPY docker/actae/entrypoint.sh /usr/local/bin/actae-entrypoint
RUN chmod +x /usr/local/bin/actae-entrypoint

ENTRYPOINT ["/usr/local/bin/actae-entrypoint"]

EXPOSE 8002

ENV ACTAE_PORT=8002

HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=15s \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8002/healthz || exit 1

CMD ["actae"]
