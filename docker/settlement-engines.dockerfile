# Build Settlement Engines into standalone binary
FROM clux/muslrust:stable as rust
ARG CARGO_BUILD_OPTION=""
ARG RUST_BIN_DIR_NAME="debug"

RUN echo "Building profile: ${CARGO_BUILD_OPTION}, output dir: ${RUST_BIN_DIR_NAME}"

WORKDIR /usr/src
COPY ./Cargo.toml /usr/src/Cargo.toml
COPY ./Cargo.lock /usr/src/Cargo.lock
COPY ./src /usr/src/src

RUN cargo build ${CARGO_BUILD_OPTION} --features "ethereum" --package interledger-settlement-engines --bin interledger-settlement-engines

# Deploy compiled binary to another container
FROM alpine
ARG CARGO_BUILD_OPTION=""
ARG RUST_BIN_DIR_NAME="debug"

# Expose ports for HTTP
# - 3000: HTTP - settlement
EXPOSE 3000

# Install SSL certs
RUN apk --no-cache add ca-certificates

# Copy binary
COPY --from=rust \
    /usr/src/target/x86_64-unknown-linux-musl/${RUST_BIN_DIR_NAME}/interledger-settlement-engines \
    /usr/local/bin/interledger-settlement-engines

WORKDIR /opt/app

# ENV RUST_BACKTRACE=1
ENV RUST_LOG=interledger=debug

ENTRYPOINT [ "/usr/local/bin/interledger-settlement-engines" ]
CMD [ "ethereum-ledger" ]
