# syntax=docker/dockerfile:1

# --- Stage 1: build the Svelte dashboard ---
FROM oven/bun:1 AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/bun.lock ./
RUN --mount=type=cache,id=lighthouse-bun-cache,target=/root/.bun/install/cache \
    bun install --frozen-lockfile
COPY frontend/ ./
RUN --mount=type=cache,id=lighthouse-bun-cache,target=/root/.bun/install/cache \
    bun run build

# --- Stage 2: build the Rust binary (embeds the built dashboard) ---
FROM rust:1-bookworm AS builder
WORKDIR /app
# .dockerignore excludes .git, so build.rs has no repository to ask for the
# commit and takes it from here instead:
#   docker build --build-arg GIT_SHA=$(git rev-parse --short=12 HEAD) .
# Omitting it stamps "unknown", which builds fine but makes a deployed image
# untraceable to a commit.
ARG GIT_SHA=""
ENV GIT_SHA=$GIT_SHA
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
# rust-embed reads frontend/dist at compile time
COPY --from=frontend /app/frontend/dist ./frontend/dist
RUN --mount=type=cache,id=lighthouse-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=lighthouse-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=lighthouse-target,target=/app/target \
    cargo build --release --bin lighthouse && \
    cp /app/target/release/lighthouse /app/lighthouse

# --- Stage 3: minimal runtime ---
# distroless/cc provides glibc + libstdc++ + libgcc for the bundled DuckDB;
# rustls means no OpenSSL is needed.
FROM gcr.io/distroless/cc-debian12
WORKDIR /data
COPY --from=builder /app/lighthouse /usr/local/bin/lighthouse
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/lighthouse"]
# Default: coordinator serving the dashboard on 0.0.0.0:8080.
# With no [[checks]] configured the coordinator seeds demo data for the dashboard.
#
# ICMP checks need raw sockets, which distroless does not grant by default:
#   docker run --cap-add=NET_RAW ...
# Without it, icmp checks report a permission error instead of silently passing.
CMD ["coordinator"]