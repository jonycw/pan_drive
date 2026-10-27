FROM rust:1-bookworm AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app/data
COPY --from=build /src/target/release/quarkdrive-webdav /usr/local/bin/quarkdrive-webdav
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["quarkdrive-webdav"]
