use ethers::prelude::*;
use gumdrop::Options;

use log::info;
use once_cell::sync::Lazy;
use std::convert::TryFrom;

use ilp_settlement_ethereum::{
    ethereum::EthClient,
    run::{run_ethereum_engine, EngineOpts},
};

static mut CLIENT: Lazy<HttpClient> = Lazy::new(|| {
    // empty private key
    let wallet = "0000000000000000000000000000000000000000000000000000000000000000"
        .parse::<Wallet>()
        .unwrap();
    let provider = Provider::<Http>::try_from("http://localhost:8545").unwrap();
    wallet.connect(provider)
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let opts = EngineOpts::parse_args_default_or_exit();

    let eth_client = unsafe {
        CLIENT.with_provider(Provider::<Http>::try_from(opts.ethereum_url.as_str())?);
        CLIENT.with_signer(opts.private_key.parse::<Wallet>()?);
        EthClient::new(opts.token_address, &CLIENT)
    };

    let addr = opts.settlement_api_bind_address;

    run_ethereum_engine(
        opts.store_url,
        eth_client,
        opts.connector_url,
        opts.settlement_api_bind_address,
        opts.poll_frequency,
        opts.asset_scale,
        opts.chain_id,
        opts.confirmations,
        opts.watch_incoming,
    )
    .await
    .expect("could not start eth engine");

    info!("Ethereum Settlement Engine listening on: {}", addr);

    futures::future::pending().await
}
