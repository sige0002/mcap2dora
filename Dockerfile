FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=build /app/target/release/mcap2dora /usr/local/bin/mcap2dora
ENTRYPOINT ["mcap2dora"]
