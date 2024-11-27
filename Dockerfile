FROM rust:latest

RUN apt-get update && apt-get install -y \
    cmake \
    protobuf-compiler \
    clang \
    pkg-config \
    libssl-dev \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/workspace

COPY . .

ENV HBASE_HOST="localhost:9090"

RUN cargo build --release

CMD ["cargo", "run", "--release"]