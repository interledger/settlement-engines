# Interledger Settlement Engines

<p align="center">
  <img src="docs/interledger-rs.svg" width="700" alt="Interledger.rs">
</p>


> Interledger Settlement Engines implementation in Rust :money_with_wings:. 

### Compliant with the [RFC](https://interledger.org/rfcs/0038-settlement-engines/)

## Currently Supported Engines

- Ethereum L1 payments (`ethereum-engine`)

## Installation and Usage

To run the settlement engines components you can use the following instructions,
depending on the engine you want to use:

### Using Docker

#### Prerequisites

- Docker

#### Install

```bash #
docker pull interledgerrs/ethereum-engine
`````

### Building From Source

#### Prerequisites

- Git
- [Rust](https://www.rust-lang.org/tools/install) - latest stable version

#### Install

```bash #
# 1. Clone the repository and change the working directory
git clone https://github.com/interledger-rs/settlement-engines

# 2. Build the engine of your choice (add `--release` to compile the release version, which is slower to compile but faster to run)
cargo build --bin <engine-name> 

This will build an engine with a redis backend. In the future, you will be able to use engines with more backends than just redis.

The currently supported engines are `ethereum-engine`, which performs settlement on Ethereum (Layer 1) without payment channel support.


# 3. Run tests
cargo test --all --all-features
```

#### Run

```bash 
cargo run --bin <engine-name> 
```

Append the `--help` flag to see available options.

## Contributing

Contributions are very welcome and if you're interested in getting involved, see [CONTRIBUTING.md](docs/CONTRIBUTING.md). We're more than happy to answer questions and mentor you in making your first contributions to Interledger.rs (even if you've never written in Rust before)!