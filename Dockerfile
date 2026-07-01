# syntax=docker/dockerfile:1

FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN cargo fetch --locked

COPY src ./src
COPY signers ./signers
RUN cargo build --release --locked --bin lighter-mm

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin lighter \
    && mkdir -p /app/logs /app/signers \
    && chown -R lighter:lighter /app

COPY --from=builder /app/target/release/lighter-mm /usr/local/bin/lighter-mm
COPY --from=builder --chown=lighter:lighter /app/signers ./signers
COPY --chown=lighter:lighter config.json ./config.json

USER lighter

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/lighter-mm"]
CMD ["--symbol", "BTC", "--config", "/app/config.json", "--dry-run"]
