use crate::utils::types::{Addresses as EthereumAddresses, EthereumAccount, EthereumStore};

use futures::future::TryFutureExt;

use bytes::Bytes;
use ethers::core::types::{Address as EthAddress, H256, U64};

use http::StatusCode;
use std::collections::HashMap;

use async_trait::async_trait;
use num_bigint::BigUint;
use redis_crate::{self as redis, aio::MultiplexedConnection, cmd, AsyncCommands, ConnectionInfo};

use log::{error, trace};
use serde::Serialize;

use interledger_settlement::core::{
    backends_common::redis::{EngineRedisStore, EngineRedisStoreBuilder},
    idempotency::{IdempotentData, IdempotentStore},
    types::LeftoversStore,
};

use interledger_errors::{IdempotentStoreError, LeftoversStoreError};

// Key for the latest observed block and balance. The data is stored in order to
// avoid double crediting transactions which have already been processed, and in
// order to resume watching from the last observed point.
static RECENTLY_OBSERVED_BLOCK_KEY: &str = "recently_observed_block";
static SAVED_TRANSACTIONS_KEY: &str = "transactions";
static SETTLEMENT_ENGINES_KEY: &str = "settlement";
static LEDGER_KEY: &str = "ledger";
static ETHEREUM_KEY: &str = "eth";

#[derive(Clone, Debug, Serialize)]
pub struct Account {
    pub(crate) id: String,
    pub(crate) own_address: EthAddress,
    pub(crate) token_address: Option<EthAddress>,
}

fn ethereum_transactions_key(tx_hash: H256) -> String {
    format!(
        "{}:{}:{}:{}",
        ETHEREUM_KEY, LEDGER_KEY, SAVED_TRANSACTIONS_KEY, tx_hash,
    )
}

fn ethereum_ledger_key(account_id: &str) -> String {
    format!(
        "{}:{}:{}:{}",
        ETHEREUM_KEY, LEDGER_KEY, SETTLEMENT_ENGINES_KEY, account_id
    )
}

impl EthereumAccount for Account {
    type AccountId = String;

    fn id(&self) -> Self::AccountId {
        self.id.clone()
    }
    fn token_address(&self) -> Option<EthAddress> {
        self.token_address
    }

    fn own_address(&self) -> EthAddress {
        self.own_address
    }
}

pub struct EthereumLedgerRedisStoreBuilder {
    redis_store_builder: EngineRedisStoreBuilder,
}

impl EthereumLedgerRedisStoreBuilder {
    pub fn new(redis_url: ConnectionInfo) -> Self {
        EthereumLedgerRedisStoreBuilder {
            redis_store_builder: EngineRedisStoreBuilder::new(redis_url),
        }
    }

    pub async fn connect(&self) -> Result<EthereumLedgerRedisStore, ()> {
        let redis_store = self.redis_store_builder.connect().await?;
        let connection = redis_store.connection.clone();
        Ok(EthereumLedgerRedisStore {
            redis_store,
            connection,
        })
    }
}

/// An Ethereum Store that uses Redis as its underlying database.
///
/// This store saves all Ethereum Ledger data for the Ethereum Settlement engine
#[derive(Clone)]
pub struct EthereumLedgerRedisStore {
    redis_store: EngineRedisStore,
    connection: MultiplexedConnection,
}

impl EthereumLedgerRedisStore {
    #[allow(unused)]
    pub fn new(redis_store: EngineRedisStore) -> Self {
        let connection = redis_store.connection.clone();
        EthereumLedgerRedisStore {
            redis_store,
            connection,
        }
    }
}

#[async_trait]
impl LeftoversStore for EthereumLedgerRedisStore {
    type AccountId = String;
    type AssetType = BigUint;

    async fn get_uncredited_settlement_amount(
        &self,
        account_id: Self::AccountId,
    ) -> Result<(Self::AssetType, u8), LeftoversStoreError> {
        self.redis_store
            .get_uncredited_settlement_amount(account_id)
            .await
    }

    async fn save_uncredited_settlement_amount(
        &self,
        account_id: Self::AccountId,
        uncredited_settlement_amount: (Self::AssetType, u8),
    ) -> Result<(), LeftoversStoreError> {
        self.redis_store
            .save_uncredited_settlement_amount(account_id, uncredited_settlement_amount)
            .await
    }

    async fn load_uncredited_settlement_amount(
        &self,
        account_id: Self::AccountId,
        local_scale: u8,
    ) -> Result<Self::AssetType, LeftoversStoreError> {
        self.redis_store
            .load_uncredited_settlement_amount(account_id, local_scale)
            .await
    }

    async fn clear_uncredited_settlement_amount(
        &self,
        account_id: Self::AccountId,
    ) -> Result<(), LeftoversStoreError> {
        self.redis_store
            .clear_uncredited_settlement_amount(account_id)
            .await
    }
}

#[async_trait]
impl IdempotentStore for EthereumLedgerRedisStore {
    async fn load_idempotent_data(
        &self,
        idempotency_key: String,
    ) -> Result<Option<IdempotentData>, IdempotentStoreError> {
        self.redis_store.load_idempotent_data(idempotency_key).await
    }

    async fn save_idempotent_data(
        &self,
        idempotency_key: String,
        input_hash: [u8; 32],
        status_code: StatusCode,
        data: Bytes,
    ) -> Result<(), IdempotentStoreError> {
        self.redis_store
            .save_idempotent_data(idempotency_key, input_hash, status_code, data)
            .await
    }
}

#[async_trait]
impl EthereumStore for EthereumLedgerRedisStore {
    type Account = Account;

    async fn load_account_addresses(
        &self,
        account_ids: Vec<String>,
    ) -> Result<Vec<EthereumAddresses>, ()> {
        let mut pipe = redis::pipe();
        let mut connection = self.connection.clone();
        for account_id in account_ids.iter() {
            pipe.hgetall(ethereum_ledger_key(&account_id));
        }
        // TODO: Can we make this just directly return us a Vec<EthereumAddresses>?
        // Have to implement the Redis traits
        let addresses: Vec<HashMap<String, Vec<u8>>> = pipe
            .query_async(&mut connection)
            .map_err(move |err| {
                error!(
                    "Error the addresses for accounts: {:?} {:?}",
                    account_ids, err
                )
            })
            .await?;

        trace!("Loaded account addresses {:?}", addresses);
        let mut ret = Vec::with_capacity(addresses.len());
        for addr in &addresses {
            let own_address = if let Some(own_address) = addr.get("own_address") {
                own_address
            } else {
                return Err(());
            };
            let mut out = [0; 20];
            out.copy_from_slice(own_address);
            let own_address = EthAddress::from(out);

            let token_address = if let Some(token_address) = addr.get("token_address") {
                token_address
            } else {
                return Err(());
            };

            let token_address = if token_address.len() == 20 {
                let mut out = [0; 20];
                out.copy_from_slice(token_address);
                Some(EthAddress::from(out))
            } else {
                None
            };
            ret.push(EthereumAddresses {
                own_address,
                token_address,
            });
        }
        Ok(ret)
    }

    async fn delete_accounts(&self, account_ids: Vec<String>) -> Result<(), ()> {
        let mut pipe = redis::pipe();
        for account_id in account_ids.iter() {
            pipe.del(ethereum_ledger_key(&account_id));
        }
        pipe.query_async(&mut self.connection.clone())
            .map_err(move |err| {
                error!(
                    "Error the addresses for accounts: {:?} {:?}",
                    account_ids, err
                )
            })
            .await?;
        Ok(())
    }

    async fn save_account_addresses(
        &self,
        data: HashMap<String, EthereumAddresses>,
    ) -> Result<(), ()> {
        let mut pipe = redis::pipe();
        for (account_id, d) in data {
            let token_address = if let Some(token_address) = d.token_address {
                token_address.as_bytes().to_owned()
            } else {
                vec![]
            };
            // TOOD: Implement ToRedis for EthereumAddresses
            let acc_id = ethereum_ledger_key(&account_id);
            let addrs = &[
                ("own_address", d.own_address.as_bytes()),
                ("token_address", &token_address),
            ];
            pipe.hset_multiple(acc_id, addrs).ignore();
            pipe.set(addrs_to_key(d), account_id).ignore();
        }
        pipe.query_async(&mut self.connection.clone())
            .map_err(move |err| error!("Error saving account data: {:?}", err))
            .await?;
        Ok(())
    }

    async fn save_recently_observed_block(
        &self,
        net_version: String,
        block: U64,
    ) -> Result<(), ()> {
        let mut connection = self.connection.clone();
        connection
            .hset(RECENTLY_OBSERVED_BLOCK_KEY, net_version, block.low_u64())
            .map_err(move |err| error!("Error saving last observed block {:?}: {:?}", block, err))
            .await?;
        Ok(())
    }

    async fn load_recently_observed_block(&self, net_version: String) -> Result<Option<U64>, ()> {
        let mut connection = self.connection.clone();
        let block: Option<u64> = connection
            .hget(RECENTLY_OBSERVED_BLOCK_KEY, net_version)
            .map_err(move |err| error!("Error loading last observed block: {:?}", err))
            .await?;
        Ok(block.map(U64::from))
    }

    async fn load_account_id_from_address(
        &self,
        eth_address: EthereumAddresses,
    ) -> Result<String, ()> {
        let mut connection = self.connection.clone();
        let account_id: Option<String> = connection
            .get(addrs_to_key(eth_address))
            .map_err(move |err| error!("Error loading account data: {:?}", err))
            .await?;
        account_id.ok_or(()) // Option<_> to Result<_, ()>
    }

    async fn check_if_tx_processed(&self, tx_hash: H256) -> Result<bool, ()> {
        let mut connection = self.connection.clone();
        connection
            .exists(ethereum_transactions_key(tx_hash))
            .map_err(move |err| error!("Error loading account data: {:?}", err))
            .await
    }

    async fn mark_tx_processed(&self, tx_hash: H256) -> Result<(), ()> {
        let mut connection = self.connection.clone();
        let marked_successfully: bool = cmd("SETNX")
            .arg(ethereum_transactions_key(tx_hash))
            .arg(true)
            .query_async(&mut connection)
            .map_err(move |err| error!("Error loading account data: {:?}", err))
            .await?;
        if marked_successfully {
            Ok(())
        } else {
            Err(())
        }
    }
}

fn addrs_to_key(address: EthereumAddresses) -> String {
    let token_address = if let Some(token_address) = address.token_address {
        token_address.to_string()
    } else {
        "null".to_string()
    };
    format!(
        "account:{}:{}",
        address.own_address.to_string(),
        token_address
    )
}

#[cfg(test)]
mod tests {
    use super::super::test_helpers::TestContext;
    use super::*;
    use std::iter::FromIterator;
    use std::str::FromStr;

    async fn test_store() -> (EthereumLedgerRedisStore, TestContext) {
        let context = TestContext::new();
        let redis_store = EngineRedisStoreBuilder::new(context.get_client_connection_info())
            .connect()
            .await
            .unwrap();
        (EthereumLedgerRedisStore::new(redis_store), context)
    }

    #[tokio::test]
    async fn saves_and_loads_ethereum_addreses_properly() {
        let (store, _context) = test_store().await;
        let account_ids = vec!["1".to_string(), "2".to_string()];
        let account_addresses = vec![
            EthereumAddresses {
                own_address: EthAddress::from_str("3cdb3d9e1b74692bb1e3bb5fc81938151ca64b02")
                    .unwrap(),
                token_address: Some(
                    EthAddress::from_str("c92be489639a9c61f517bd3b955840fa19bc9b7c").unwrap(),
                ),
            },
            EthereumAddresses {
                own_address: EthAddress::from_str("2fcd07047c209c46a767f8338cb0b14955826826")
                    .unwrap(),
                token_address: None,
            },
        ];
        let input = HashMap::from_iter(vec![
            (account_ids[0].clone(), account_addresses[0]),
            (account_ids[1].clone(), account_addresses[1]),
        ]);
        store.save_account_addresses(input).await.unwrap();
        let data = store
            .load_account_addresses(account_ids.clone())
            .await
            .unwrap();
        assert_eq!(data[0], account_addresses[0]);
        assert_eq!(data[1], account_addresses[1]);
        let acc_id = store
            .load_account_id_from_address(account_addresses[0])
            .await
            .unwrap();
        assert_eq!(acc_id, account_ids[0]);
        let acc_id = store
            .load_account_id_from_address(account_addresses[1])
            .await
            .unwrap();
        assert_eq!(acc_id, account_ids[1]);
    }

    #[tokio::test]
    async fn saves_and_loads_last_observed_data_properly() {
        let (store, _context) = test_store().await;
        let block1 = U64::from(1);
        let block2 = U64::from(2);
        store
            .save_recently_observed_block("1".to_owned(), block1)
            .await
            .unwrap();
        store
            .save_recently_observed_block("2".to_owned(), block2)
            .await
            .unwrap();
        let ret1 = store
            .load_recently_observed_block("1".to_owned())
            .await
            .unwrap();
        let ret2 = store
            .load_recently_observed_block("2".to_owned())
            .await
            .unwrap();
        assert_eq!(ret1, Some(block1));
        assert_eq!(ret2, Some(block2));
    }

    #[tokio::test]
    async fn saves_tx_hashes_properly() {
        let (store, _context) = test_store().await;
        let tx_hash =
            H256::from_str("b28675771f555adf614f1401838b9fffb43bc285387679bcbd313a8dc5bdc00e")
                .unwrap();
        store.mark_tx_processed(tx_hash).await.unwrap();
        let seen = store.check_if_tx_processed(tx_hash).await.unwrap();
        assert_eq!(seen, true);
    }
}
