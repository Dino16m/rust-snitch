# Multi-stage Dockerfile for building and running the `snitch` Rust binary
# Builder stage: compile the binary with cargo
FROM rust:1.91-slim-bullseye AS builder


WORKDIR /app

# Cache dependencies by copying manifests first
COPY Cargo.toml Cargo.lock ./

# Copy source
COPY ./src ./src

# Build release binary
RUN --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --release


# Final stage: small runtime image
FROM debian:bullseye-slim

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/snitch /app/snitch

# If your application listens on a port, adjust the EXPOSE value accordingly
EXPOSE 8000

ENTRYPOINT ["/app/snitch"]
