FROM rust:1.84-slim-bookworm AS builder
WORKDIR /build

COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/rs_blobstore* target/release/rs-blobstore*

COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/rs-blobstore /usr/local/bin/rs-blobstore

ENV STORAGE_ROOT=/data \
    BIND_ADDR=0.0.0.0:8080 \
    RUST_LOG=info

VOLUME ["/data"]
EXPOSE 8080

CMD ["/usr/local/bin/rs-blobstore"]
