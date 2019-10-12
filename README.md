# Interledger Settlement Engines

<p align="center">
  <img src="docs/interledger-rs.svg" width="700" alt="Interledger.rs">
</p>


> Interledger Settlement Engines implementation in Rust :money_with_wings:. 

### Compliant with the latest [RFC](https://github.com/interledger/rfcs/pull/536/)

## Currently Supported Engines

- Ethereum L1 payments

## Installation and Usage

To run the settlement engines components you can use the following instructions:

### Using Docker

#### Prerequisites

- Docker

#### Install

```bash #
docker pull interledgerrs/settlement-engines
`````

### Building From Source

#### Prerequisites

- Git
- [Rust](https://www.rust-lang.org/tools/install) - latest stable version

#### Install

```bash #
# 1. Clone the repository and change the working directory
git clone https://github.com/interledger-rs/settlement-engines && cd settlement-engines

# 2. Build the Ethereum engine (add `--release` to compile the release version, which is slower to compile but faster to run)
cargo build --features "ethereum" 

# 3. Run tests
cargo test --features "ethereum"
```

#### Run

```bash 
cargo run --features "ethereum" -- # Put CLI args after the "--"
```

Append the `--help` flag to see available options.

## Contributing

Contributions are very welcome and if you're interested in getting involved, see [CONTRIBUTING.md](docs/CONTRIBUTING.md). We're more than happy to answer questions and mentor you in making your first contributions to Interledger.rs (even if you've never written in Rust before)!
