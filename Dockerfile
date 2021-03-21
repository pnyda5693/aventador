FROM rust:1.50

WORKDIR /app

COPY . .
RUN cargo build --release

CMD ["target/release/aventador", "run"]