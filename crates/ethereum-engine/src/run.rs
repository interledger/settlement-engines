use ethers::prelude::*;
use gumdrop::Options;

use crate::{
    backends::redis::EthereumLedgerRedisStoreBuilder, ethereum::EthClient,
    EthereumLedgerSettlementEngine,
};
use interledger_settlement::core::engines_api::create_settlement_engine_filter;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::{collections::HashMap, marker::PhantomData, sync::Arc, time::Duration};
use url::Url;

use redis_crate::IntoConnectionInfo;

#[derive(Debug, Options)]
pub struct EngineOpts {
    #[options(help = "The store's URL", default = "http://localhost:7777")]
    pub store_url: Url,

    #[options(help = "The connector's URL", default = "http://localhost:7777")]
    pub connector_url: Url,

    #[options(help = "The api's URL", default = "http://localhost:3000")]
    pub settlement_api_bind_address: SocketAddr,

    #[options(help = "The ethereum URL", default = "http://localhost:8545")]
    pub ethereum_url: Url,

    #[options(help = "The private key being used")]
    pub private_key: String,

    #[options(help = "The token address (if any)")]
    pub token_address: Option<Address>,

    #[options(help = "The number of decimals", default = "18")]
    pub asset_scale: u8,

    #[options(help = "The chain id", default = "1")]
    pub chain_id: u8,

    #[options(help = "The num confs", default = "6")]
    pub confirmations: u8,

    #[options(help = "How frequently to poll the chain (in ms)", default = "5000")]
    pub poll_frequency: u64,

    #[options(help = "should we watch incoming transfers?")]
    pub watch_incoming: bool,
}

#[allow(clippy::too_many_arguments)]
pub async fn run_ethereum_engine<T: IntoConnectionInfo>(
    db: T,
    eth_client: EthClient<'static>, // TODO: Can we get rid of this static lifetime?
    connector_url: Url,
    api_address: SocketAddr,
    frequency: u64,
    asset_scale: u8,
    chain_id: u8,
    confirmations: u8,
    watch_incoming: bool,
) -> anyhow::Result<()> {
    let store = EthereumLedgerRedisStoreBuilder::new(db.into_connection_info()?)
        .connect()
        .await
        .expect("could not connect to store");

    let freq = Duration::from_millis(frequency);
    let engine = EthereumLedgerSettlementEngine {
        store: store.clone(),
        eth_client,
        asset_scale,
        chain_id,
        confirmations,
        connector_url,
        challenges: Arc::new(RwLock::new(HashMap::new())),
        account_type: PhantomData,
    };

    if watch_incoming {
        engine.spawn(freq);
    }

    let api = create_settlement_engine_filter(engine, store);
    tokio::spawn(warp::serve(api).bind(api_address));
    Ok(())
}
