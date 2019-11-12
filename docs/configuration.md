## How to Specify Configurations

Interledger.rs binaries such as `ilp-node` and `interledger-settlement-engines` accept configuration options in the following ways:

### Environment variables

```bash #
# Passing as environment variables
# {parameter name (typically in capital)}={value}
# note that the parameter names MUST begin with a prefix of "ILP_" e.g. ILP_PRIVATE_KEY
ILP_PRIVATE_KEY=0x380eb0f3d505f087e438eca80bc4df9a7faa24f868e69fc0440261a0fc0567dc \
ILP_OTHER_PARAMETER=other_value \
interledger-settlement-engines ethereum-ledger
```

### Standard In (stdin)

```bash #
# Passing from STDIN in JSON, TOML, YAML format.
some_command | interledger-settlement-engines ethereum-ledger
```

### Configuration files

```bash #
# Passing by a configuration file in JSON, TOML, YAML format.
# The first argument after subcommands such as `ethereum-ledger` is the path to the configuration file.
interledger-settlement-engines ethereum-ledger config.yml
```

### Command line arguments

```bash #
# Passing by command line arguments.
# --{parameter name} {value}
interledger-settlement-engines ethereum-ledger --private_key super-secret
```

Note that configurations are applied in the following order of priority:
1. Environment Variables
1. Stdin
1. Configuration files
1. Command line arguments.

## Configuration Parameters

The configuration parameters are explained in the following format.

- name of the config
    - format
    - example
    - explanation

---

### Required

- private_key
    - 32 bytes HEX
    - `0x380eb0f3d505f087e438eca80bc4df9a7faa24f868e69fc0440261a0fc0567dc`
    - An Ethereum private key for settlement account.

### Optional

- settlement_api_bind_address
    - Socket Address (`address:port`)
    - `127.0.0.1:3000`
    - A pair of an IP address and a port to listen for connections from interledger nodes. The address provides the Settlement Engine API.
- ethereum_url
    - URL
    - `http://127.0.0.1:8545`
    - An Ethereum node endpoint URL. For example, the address of `ganache`.
- token_address
    - Contract Address
    - `0xdAC17F958D2ee523a2206206994597C13D831ec7`
    - An address of the ERC20 token to be used for settlement (defaults to sending ETH if no token address is provided).
- connector_url
    - URL
    - `http://127.0.0.1:7771`
    - The Settlement Engine API endpoint URL of a connector.
- redis_url
    - URL
    - `redis://127.0.0.1:6379`, `unix:/tmp/redis.sock`
    - A URL of redis that the settlement engine connects to in order to store its data.
- chain_id
    - Non-negative Integer
    - `1`
    - The chain id so that the signer calculates the v value of the sig appropriately. Defaults to 1 which means the mainnet. There are some other options such as: 3(Ropsten: PoW testnet), 4(Rinkeby: PoA testnet), etc.
- confirmations
    - Non-negative Integer
    - `6`
    - The number of confirmations the engine will wait for a transaction's inclusion before it notifies the node of its success.
- asset_scale
    - Non-negative Integer
    - `18`
    - The asset scale you want to use for your payments. This should be changed when you use some tokens that use its own scales other than `18`.
- poll_frequency
    - Non-negative Integer
    - `5000`
    - The frequency in milliseconds at which the engine will check the blockchain about the confirmation status of a tx.
- watch_incoming
    - Boolean
    - `false`
    - Launch a blockchain watcher that listens for incoming transactions and notifies the connector upon sufficient confirmations.

