use async_trait::async_trait;
use bytes::Bytes;
use std::cmp::Ordering;
use std::process::Command;

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use hyper::StatusCode;
use std::str::FromStr;

use web3::types::{Address, H256, U256};

use ilp_settlement_ethereum::engine::{
    EthereumLedgerSettlementEngine, EthereumLedgerSettlementEngineBuilder,
};
use ilp_settlement_ethereum::utils::types::{
    Addresses, EthereumAccount, EthereumLedgerTxSigner, EthereumStore,
};
use interledger_settlement::core::{
    idempotency::{IdempotentData, IdempotentStore},
    scale_with_precision_loss,
    types::{Convert, ConvertDetails, LeftoversStore},
};

#[derive(Debug, Clone)]
pub struct TestAccount {
    pub id: String,
    pub address: Address,
    pub token_address: Address,
    pub no_details: bool,
}

impl EthereumAccount for TestAccount {
    type AccountId = String;

    fn id(&self) -> String {
        self.id.clone()
    }

    fn token_address(&self) -> Option<Address> {
        if self.no_details {
            return None;
        }
        Some(self.token_address)
    }
    fn own_address(&self) -> Address {
        self.address
    }
}

// Test Store
#[derive(Clone)]
pub struct TestStore {
    pub accounts: Arc<Vec<TestAccount>>,
    pub should_fail: bool,
    pub addresses: Arc<RwLock<HashMap<String, Addresses>>>,
    pub address_to_id: Arc<RwLock<HashMap<Addresses, String>>>,
    #[allow(clippy::all)]
    pub cache: Arc<RwLock<HashMap<String, IdempotentData>>>,
    pub last_observed_block: Arc<RwLock<U256>>,
    pub saved_hashes: Arc<RwLock<HashMap<H256, bool>>>,
    pub cache_hits: Arc<RwLock<u64>>,
    pub uncredited_settlement_amount: Arc<RwLock<HashMap<String, (BigUint, u8)>>>,
}

use num_bigint::BigUint;

#[async_trait]
impl LeftoversStore for TestStore {
    type AccountId = String;
    type AssetType = BigUint;

    async fn save_uncredited_settlement_amount(
        &self,
        account_id: Self::AccountId,
        uncredited_settlement_amount: (Self::AssetType, u8),
    ) -> Result<(), ()> {
        let mut guard = self.uncredited_settlement_amount.write();
        if let Some(leftovers) = (*guard).get_mut(&account_id) {
            match leftovers.1.cmp(&uncredited_settlement_amount.1) {
                Ordering::Greater => {
                    // the current leftovers maintain the scale so we just need to
                    // upscale the provided leftovers to the existing leftovers' scale
                    let scaled = uncredited_settlement_amount
                        .0
                        .normalize_scale(ConvertDetails {
                            from: uncredited_settlement_amount.1,
                            to: leftovers.1,
                        })
                        .unwrap();
                    *leftovers = (leftovers.0.clone() + scaled, leftovers.1);
                }
                Ordering::Equal => {
                    *leftovers = (
                        leftovers.0.clone() + uncredited_settlement_amount.0,
                        leftovers.1,
                    );
                }
                _ => {
                    // if the scale of the provided leftovers is bigger than
                    // existing scale then we update the scale of the leftovers'
                    // scale
                    let scaled = leftovers
                        .0
                        .normalize_scale(ConvertDetails {
                            from: leftovers.1,
                            to: uncredited_settlement_amount.1,
                        })
                        .unwrap();
                    *leftovers = (
                        uncredited_settlement_amount.0 + scaled,
                        uncredited_settlement_amount.1,
                    );
                }
            }
        } else {
            (*guard).insert(account_id, uncredited_settlement_amount);
        }
        Ok(())
    }

    async fn load_uncredited_settlement_amount(
        &self,
        account_id: Self::AccountId,
        local_scale: u8,
    ) -> Result<Self::AssetType, ()> {
        let mut guard = self.uncredited_settlement_amount.write();
        if let Some(l) = guard.get_mut(&account_id) {
            let ret = l.clone();
            let (scaled_leftover_amount, leftover_precision_loss) =
                scale_with_precision_loss(ret.0, local_scale, ret.1);
            // save the new leftovers
            *l = (leftover_precision_loss, std::cmp::max(local_scale, ret.1));
            Ok(scaled_leftover_amount)
        } else {
            Ok(BigUint::from(0u32))
        }
    }

    async fn get_uncredited_settlement_amount(
        &self,
        account_id: Self::AccountId,
    ) -> Result<(Self::AssetType, u8), ()> {
        let leftovers = self.uncredited_settlement_amount.read();
        Ok(if let Some(a) = leftovers.get(&account_id) {
            a.clone()
        } else {
            (BigUint::from(0u32), 1)
        })
    }

    async fn clear_uncredited_settlement_amount(
        &self,
        _account_id: Self::AccountId,
    ) -> Result<(), ()> {
        unreachable!()
    }
}

#[async_trait]
impl EthereumStore for TestStore {
    type Account = TestAccount;

    async fn save_account_addresses(&self, data: HashMap<String, Addresses>) -> Result<(), ()> {
        let mut guard = self.addresses.write();
        let mut guard2 = self.address_to_id.write();
        for (acc, d) in data {
            (*guard).insert(acc.clone(), d);
            (*guard2).insert(d, acc);
        }
        Ok(())
    }

    async fn delete_accounts(&self, _account_ids: Vec<String>) -> Result<(), ()> {
        Ok(())
    }

    async fn load_account_addresses(&self, account_ids: Vec<String>) -> Result<Vec<Addresses>, ()> {
        let mut v = Vec::with_capacity(account_ids.len());
        let addresses = self.addresses.read();
        for acc in &account_ids {
            if let Some(d) = addresses.get(acc) {
                v.push(Addresses {
                    own_address: d.own_address,
                    token_address: d.token_address,
                });
            } else {
                // if the account is not found, error out
                return Err(());
            }
        }
        Ok(v)
    }

    async fn save_recently_observed_block(
        &self,
        _net_version: String,
        block: U256,
    ) -> Result<(), ()> {
        let mut guard = self.last_observed_block.write();
        *guard = block;
        Ok(())
    }

    async fn load_recently_observed_block(&self, _net_version: String) -> Result<Option<U256>, ()> {
        let b = *self.last_observed_block.read();
        Ok(Some(b))
    }

    async fn load_account_id_from_address(&self, eth_address: Addresses) -> Result<String, ()> {
        let addresses = self.address_to_id.read();
        let d = if let Some(d) = addresses.get(&eth_address) {
            d.clone()
        } else {
            return Err(());
        };

        Ok(d)
    }

    async fn check_if_tx_processed(&self, tx_hash: H256) -> Result<bool, ()> {
        let hashes = self.saved_hashes.read();
        // if hash exists then return error
        if hashes.get(&tx_hash).is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn mark_tx_processed(&self, tx_hash: H256) -> Result<(), ()> {
        let mut hashes = self.saved_hashes.write();
        (*hashes).insert(tx_hash, true);
        Ok(())
    }
}

#[async_trait]
impl IdempotentStore for TestStore {
    async fn load_idempotent_data(
        &self,
        idempotency_key: String,
    ) -> Result<Option<IdempotentData>, ()> {
        let cache = self.cache.read();
        if let Some(data) = cache.get(&idempotency_key) {
            let mut guard = self.cache_hits.write();
            *guard += 1; // used to test how many times this branch gets executed
            Ok(Some(data.clone()))
        } else {
            Ok(None)
        }
    }

    async fn save_idempotent_data(
        &self,
        idempotency_key: String,
        input_hash: [u8; 32],
        status_code: StatusCode,
        data: Bytes,
    ) -> Result<(), ()> {
        let mut cache = self.cache.write();
        cache.insert(
            idempotency_key,
            IdempotentData::new(status_code, data, input_hash),
        );
        Ok(())
    }
}

impl TestStore {
    pub fn new(accs: Vec<TestAccount>, should_fail: bool, initialize: bool) -> Self {
        let mut addresses = HashMap::new();
        let mut address_to_id = HashMap::new();
        if initialize {
            for account in &accs {
                let token_address = if !account.no_details {
                    Some(account.token_address)
                } else {
                    None
                };
                let addrs = Addresses {
                    own_address: account.address,
                    token_address,
                };
                addresses.insert(account.id.clone(), addrs);
                address_to_id.insert(addrs, account.id.clone());
            }
        }

        TestStore {
            accounts: Arc::new(accs),
            should_fail,
            addresses: Arc::new(RwLock::new(addresses)),
            address_to_id: Arc::new(RwLock::new(address_to_id)),
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_hits: Arc::new(RwLock::new(0)),
            last_observed_block: Arc::new(RwLock::new(U256::from(0))),
            saved_hashes: Arc::new(RwLock::new(HashMap::new())),
            uncredited_settlement_amount: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// Test Service

impl TestAccount {
    pub fn new(id: String, address: &str, token_address: &str) -> Self {
        Self {
            id,
            address: Address::from_str(address).unwrap(),
            token_address: Address::from_str(token_address).unwrap(),
            no_details: false,
        }
    }
}

// Helper to create a new engine
pub async fn test_engine<Si, S, A>(
    store: S,
    key: Si,
    confs: u8,
    connector_url: &str,
    ganache_port: u16,
    token_address: Option<Address>,
    watch_incoming: bool,
) -> EthereumLedgerSettlementEngine<S, Si, A>
where
    Si: EthereumLedgerTxSigner + Clone + Send + Sync + 'static,
    S: EthereumStore<Account = A>
        + LeftoversStore<AccountId = String, AssetType = BigUint>
        + IdempotentStore
        + Clone
        + Send
        + Sync
        + 'static,
    A: EthereumAccount<AccountId = String> + Clone + Send + Sync + 'static,
{
    EthereumLedgerSettlementEngineBuilder::new(store, key)
        .connector_url(connector_url)
        .ethereum_endpoint(&format!("http://localhost:{}", ganache_port))
        .token_address(token_address)
        .confirmations(confs)
        .watch_incoming(watch_incoming)
        .poll_frequency(1000)
        .connect()
        .await
}

pub fn test_store(
    account: TestAccount,
    store_fails: bool,
    account_has_engine: bool,
    initialize: bool,
) -> TestStore {
    let mut acc = account;
    acc.no_details = !account_has_engine;
    TestStore::new(vec![acc], store_fails, initialize)
}

pub fn start_ganache(port: u16) -> std::process::Child {
    let mut ganache = Command::new("ganache-cli");
    let ganache = ganache
        .stdout(std::process::Stdio::null())
        .arg("-m").arg("abstract vacuum mammal awkward pudding scene penalty purchase dinner depart evoke puzzle")
        .arg("-p").arg(port.to_string());
    let ganache_pid = ganache.spawn().expect("couldnt start ganache-cli");
    // wait a couple of seconds for ganache to boot up
    let sleep_time = std::time::Duration::from_secs(5);
    std::thread::sleep(sleep_time);
    ganache_pid
}
