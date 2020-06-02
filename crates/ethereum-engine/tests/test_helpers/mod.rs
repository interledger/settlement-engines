use futures::TryFutureExt;
use interledger::{packet::Address, service::Account as AccountTrait, store::account::Account};
use uuid::Uuid;

#[cfg(feature = "redis")]
pub mod redis_helpers;

#[cfg(feature = "redis")]
use redis_crate::ConnectionInfo;

use hex;
use interledger::stream::StreamDelivery;
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    net::SocketAddr,
    process::Command,
    str,
    thread::sleep,
    time::Duration,
};

use rand::{thread_rng, Rng};

use ilp_settlement_ethereum::{ethereum::EthClient, run::run_ethereum_engine};

use ethers::prelude::*;
use std::convert::TryFrom;

#[allow(unused)]
pub fn random_secret() -> String {
    let mut bytes: [u8; 32] = [0; 32];
    thread_rng().fill(&mut bytes);
    hex::encode(bytes)
}

#[derive(Deserialize)]
pub struct DeliveryData {
    pub delivered_amount: u64,
}

#[derive(serde::Deserialize, Debug, PartialEq)]
pub struct BalanceData {
    pub balance: f64,
    pub asset_code: String,
}

#[allow(unused)]
pub static ALICE: Lazy<HttpClient> = Lazy::new(|| {
    // empty private key
    let wallet = "380eb0f3d505f087e438eca80bc4df9a7faa24f868e69fc0440261a0fc0567dc"
        .parse::<Wallet>()
        .unwrap();
    let provider = Provider::<Http>::try_from("http://localhost:8545").unwrap();
    wallet.connect(provider)
});

#[allow(unused)]
pub static BOB: Lazy<HttpClient> = Lazy::new(|| {
    // empty private key
    let wallet = "cc96601bc52293b53c4736a12af9130abf347669b3813f9ec4cafdf6991b087e"
        .parse::<Wallet>()
        .unwrap();
    let provider = Provider::<Http>::try_from("http://localhost:8545").unwrap();
    wallet.connect(provider)
});

#[allow(unused)]
pub fn start_ganache() -> std::process::Child {
    let mut ganache = Command::new("ganache-cli");
    let ganache = ganache.stdout(std::process::Stdio::null()).arg("-m").arg(
        "abstract vacuum mammal awkward pudding scene penalty purchase dinner depart evoke puzzle",
    );
    let ganache_pid = ganache.spawn().expect("couldnt start ganache-cli");
    // wait a couple of seconds for ganache to boot up
    sleep(Duration::from_secs(2));
    ganache_pid
}

#[allow(unused)]
pub fn start_xrp_engine(
    connector_url: &str,
    redis_port: u16,
    engine_port: u16,
) -> std::process::Child {
    let mut engine = Command::new("ilp-settlement-xrp");
    engine
        .env("DEBUG", "settlement*")
        .env("CONNECTOR_URL", connector_url)
        .env(
            "REDIS_URI",
            &format!("redis://127.0.0.1:{}", redis_port.to_string()),
        )
        .env("ENGINE_PORT", engine_port.to_string());
    engine
        // .stderr(std::process::Stdio::null())
        // .stdout(std::process::Stdio::null())
        .spawn()
        .expect("couldnt start xrp engine")
}

#[allow(unused)]
#[cfg(feature = "redis")]
pub async fn start_eth_engine(
    db: ConnectionInfo,
    http_address: SocketAddr,
    eth_client: EthClient<'static>,
    settlement_port: u16,
) -> anyhow::Result<()> {
    run_ethereum_engine(
        db,
        eth_client,
        format!("http://127.0.0.1:{}", settlement_port)
            .parse()
            .unwrap(),
        http_address,
        1000,
        18,
        1,
        0,
        true,
    )
    .await
}

#[allow(unused)]
pub async fn create_account_on_node<T: Serialize>(
    api_port: u16,
    data: T,
    auth: &str,
) -> Result<Account, ()> {
    let client = reqwest::Client::new();
    let res = client
        .post(&format!("http://localhost:{}/accounts", api_port))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", auth))
        .json(&data)
        .send()
        .map_err(|_| ())
        .await?;

    let res = res.error_for_status().map_err(|_| ())?;

    Ok(res.json::<Account>().map_err(|_| ()).await.unwrap())
}

#[allow(unused)]
pub async fn create_account_on_engine<T: Serialize>(
    engine_port: u16,
    account_id: T,
) -> Result<String, ()> {
    let client = reqwest::Client::new();
    let res = client
        .post(&format!("http://localhost:{}/accounts", engine_port))
        .header("Content-Type", "application/json")
        .json(&json!({ "id": account_id }))
        .send()
        .map_err(|_| ())
        .await?;

    let res = res.error_for_status().map_err(|_| ())?;
    let data = res.bytes().map_err(|_| ()).await?;
    Ok(str::from_utf8(&data).unwrap().to_string())
}

#[allow(unused)]
pub async fn set_node_settlement_engines<T: Serialize>(
    api_port: u16,
    data: T,
    auth: &str,
) -> Result<String, ()> {
    let client = reqwest::Client::new();
    let ret = client
        .put(&format!("http://localhost:{}/settlement/engines", api_port))
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", auth))
        .json(&data)
        .send()
        .map_err(|_| ())
        .await?;

    let res = ret.error_for_status().map_err(|_| ())?;
    let data = res.bytes().map_err(|_| ()).await?;
    Ok(str::from_utf8(&data).unwrap().to_string())
}

#[allow(unused)]
pub async fn send_money_to_username<T: Display + Debug>(
    from_port: u16,
    to_port: u16,
    amount: u64,
    to_username: T,
    from_username: &str,
    from_auth: &str,
) -> Result<StreamDelivery, ()> {
    let client = reqwest::Client::new();
    let res = client
        .post(&format!(
            "http://localhost:{}/accounts/{}/payments",
            from_port, from_username
        ))
        .header("Authorization", format!("Bearer {}", from_auth))
        .json(&json!({
            "receiver": format!("http://localhost:{}/accounts/{}/spsp", to_port, to_username),
            "source_amount": amount,
        }))
        .send()
        .map_err(|_| ())
        .await?;

    let res = res.error_for_status().map_err(|_| ())?;
    Ok(res.json::<StreamDelivery>().await.unwrap())
}

#[allow(unused)]
pub async fn get_all_accounts(node_port: u16, admin_token: &str) -> Result<Vec<Account>, ()> {
    let client = reqwest::Client::new();
    let res = client
        .get(&format!("http://localhost:{}/accounts", node_port))
        .header("Authorization", format!("Bearer {}", admin_token))
        .send()
        .map_err(|_| ())
        .await?;

    let res = res.error_for_status().map_err(|_| ())?;
    let body = res.bytes().map_err(|_| ()).await?;
    let ret: Vec<Account> = serde_json::from_slice(&body).unwrap();
    Ok(ret)
}

#[allow(unused)]
pub fn accounts_to_ids(accounts: Vec<Account>) -> HashMap<Address, Uuid> {
    let mut map = HashMap::new();
    for a in accounts {
        map.insert(a.ilp_address().clone(), a.id());
    }
    map
}

#[allow(unused)]
pub async fn get_balance<T: Display>(
    account_id: T,
    node_port: u16,
    admin_token: &str,
) -> Result<BalanceData, ()> {
    let client = reqwest::Client::new();
    let res = client
        .get(&format!(
            "http://localhost:{}/accounts/{}/balance",
            node_port, account_id
        ))
        .header("Authorization", format!("Bearer {}", admin_token))
        .send()
        .map_err(|_| ())
        .await?;

    let res = res.error_for_status().map_err(|_| ())?;
    let body = res.bytes().map_err(|_| ()).await?;
    let ret: BalanceData = serde_json::from_slice(&body).unwrap();
    Ok(ret)
}

#[derive(Deserialize, Debug)]
struct FaucetResponse {
    pub account: XrpCredentials,
    pub balance: u64,
}

#[derive(Deserialize, Debug)]
pub struct XrpCredentials {
    pub address: String,
    pub secret: String,
}
