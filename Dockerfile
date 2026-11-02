# -------------------------------------------------------------------
# Stage 1: Build binary with cargo
# -------------------------------------------------------------------
FROM rust:1.88-bookworm AS builder
WORKDIR /usr/src/app

# Copy dependency specifications first to leverage Docker layer caching
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && echo "" > src/lib.rs \
    && cargo build --release \
    && rm -rf src

# Copy source code and build actual binary
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release

# -------------------------------------------------------------------
# Stage 2: Minimal runtime image
# -------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# Install CA certificates and tzdata for secure HTTPS/WSS connections to Binance
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled binary from builder
COPY --from=builder /usr/src/app/target/release/binance-grid-bot /app/binance-grid-bot

# Ensure data directory for SQLite persistence
RUN mkdir -p /app/data
VOLUME ["/app/data"]

# Expose web dashboard port
EXPOSE 8080

ENV RUST_LOG="binance_grid_bot=info,tower_http=info"

ENTRYPOINT ["/app/binance-grid-bot"]
CMD ["--db", "/app/data/bot.db", "--host", "0.0.0.0"]
