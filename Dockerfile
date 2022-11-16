FROM rust:1.65

WORKDIR /home/projects/cmd-proxy
RUN cargo init

COPY ./.cargo .cargo
COPY ./vendor vendor
COPY Cargo.toml ./
COPY Cargo.lock ./

RUN cargo build
RUN cargo clean -p cmd-proxy

COPY ./src src

RUN cargo install --locked --path .

CMD ["cmd-proxy", "-h"]
