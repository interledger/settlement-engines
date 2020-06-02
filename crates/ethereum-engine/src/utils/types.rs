use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    str::FromStr,
};

use ethers::core::types::{Address, H256, U64};

/// An Ethereum account is associated with an address. We additionally require
/// that an optional `token_address` is implemented. If the `token_address` of an
/// Ethereum Account is not `None`, than that account is used with the ERC20 token
/// associated with that `token_address`.
pub trait EthereumAccount {
    type AccountId: Eq
        + Hash
        + Debug
        + Display
        + Default
        + FromStr
        + Send
        + Sync
        + Clone
        + Serialize;

    fn id(&self) -> Self::AccountId;

    fn own_address(&self) -> Address;

    fn token_address(&self) -> Option<Address> {
        None
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Copy)]
pub struct Addresses {
    pub own_address: Address,
    pub token_address: Option<Address>,
}

/// Trait used to store Ethereum account addresses, as well as any data related
/// to the connector notifier service such as the most recently observed block
/// and account balance
#[async_trait]
pub trait EthereumStore {
    type Account: EthereumAccount;

    /// Saves the Ethereum address associated with this account
    /// called when creating an account on the API.
    async fn save_account_addresses(
        &self,
        data: HashMap<<Self::Account as EthereumAccount>::AccountId, Addresses>,
    ) -> Result<(), ()>;

    /// Loads the Ethereum address associated with this account
    async fn load_account_addresses(
        &self,
        account_ids: Vec<<Self::Account as EthereumAccount>::AccountId>,
    ) -> Result<Vec<Addresses>, ()>;

    /// Deletes the Ethereum address associated with this account
    /// called when deleting an account on the API.
    async fn delete_accounts(
        &self,
        account_ids: Vec<<Self::Account as EthereumAccount>::AccountId>,
    ) -> Result<(), ()>;

    /// Saves the latest block number, up to which all
    /// transactions have been communicated to the connector
    async fn save_recently_observed_block(&self, net_version: String, block: U64)
        -> Result<(), ()>;

    /// Loads the latest saved block number
    async fn load_recently_observed_block(&self, net_version: String) -> Result<Option<U64>, ()>;

    /// Retrieves the account id associated with the provided addresses pair.
    /// Note that an account with the same `own_address` but different ERC20
    /// `token_address` can exist multiple times since each occurence represents
    /// a different token.
    async fn load_account_id_from_address(
        &self,
        eth_address: Addresses,
    ) -> Result<<Self::Account as EthereumAccount>::AccountId, ()>;

    /// Returns true if the transaction has already been processed and saved in
    /// the store.
    async fn check_if_tx_processed(&self, tx_hash: H256) -> Result<bool, ()>;

    /// Saves the transaction hash in the store.
    async fn mark_tx_processed(&self, tx_hash: H256) -> Result<(), ()>;
}
