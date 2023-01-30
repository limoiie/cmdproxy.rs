ARG RUST_VERSION

FROM rust:${RUST_VERSION:-1.65}

WORKDIR /home/projects/cmdproxy
RUN cargo init

COPY ./.cargo .cargo
COPY ./vendor vendor
COPY Cargo.toml ./
COPY Cargo.lock ./

RUN cargo build
RUN cargo clean -p cmdproxy

COPY ./src src

RUN cargo install --locked --path .

COPY ./examples examples

CMD ["cmdproxy", "--command-palette=./examples/commands-palette.yaml"]
