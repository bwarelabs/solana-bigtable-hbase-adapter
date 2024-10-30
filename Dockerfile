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

CMD ["sleep", "infinity"]
