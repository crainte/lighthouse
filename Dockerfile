# syntax=docker/dockerfile:1

# --- Stage 1: build the Svelte dashboard ---
FROM oven/bun:1 AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/bun.lock ./
RUN bun install --frozen-lockfile
COPY frontend/ ./
RUN bun run build

# --- Stage 2: build the Rust binary (embeds the built dashboard) ---
FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# rust-embed reads frontend/dist at compile time
COPY --from=frontend /app/frontend/dist ./frontend/dist
RUN cargo build --release --bin warden

# --- Stage 3: minimal runtime ---
# distroless/cc provides glibc + libstdc++ + libgcc for the bundled DuckDB;
# rustls means no OpenSSL is needed.
FROM gcr.io/distroless/cc-debian12
WORKDIR /data
COPY --from=builder /app/target/release/warden /usr/local/bin/warden
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/warden"]
# Default: coordinator serving the dashboard on 0.0.0.0:8080.
# With no [[checks]] configured the coordinator seeds demo data for the dashboard.
#
# ICMP checks need raw sockets, which distroless does not grant by default:
#   docker run --cap-add=NET_RAW ...
# Without it, icmp checks report a permission error instead of silently passing.
CMD ["coordinator"]
