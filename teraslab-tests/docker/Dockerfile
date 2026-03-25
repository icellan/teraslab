# TeraSlab Docker image for cluster acceptance testing.
#
# Multi-stage build: compile in rust:bookworm, run in debian:bookworm-slim.
# Runtime includes network tools for fault injection (iptables, tc netem).

FROM rust:latest AS builder
WORKDIR /build

# Cache-bust: this ARG changes when source changes, invalidating the COPY cache.
ARG CACHE_BUST=1

# Copy everything needed for compilation.
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY ui/ ui/
COPY benches/ benches/
COPY tests/ tests/

RUN cargo build --release --bin teraslab-server --bin teraslab-cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    iproute2 \
    iptables \
    curl \
    jq \
    procps \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/teraslab-server /usr/local/bin/teraslab-server
COPY --from=builder /build/target/release/teraslab-cli /usr/local/bin/teraslab-cli
RUN mkdir -p /etc/teraslab /data /blobstore

VOLUME ["/data", "/blobstore"]

# 3300 = client binary protocol
# 3301 = SWIM UDP (heartbeat / membership)
# 9100 = HTTP observability (metrics, health, status)
EXPOSE 3300 3301 9100

HEALTHCHECK --interval=2s --timeout=2s --start-period=5s --retries=5 \
    CMD curl -sf http://localhost:9100/health/live || exit 1

ENTRYPOINT ["teraslab-server"]
CMD ["--config", "/etc/teraslab/node.toml"]
