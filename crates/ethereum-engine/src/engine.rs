use super::utils::{
    types::{Addresses, EthereumAccount, EthereumLedgerTxSigner, EthereumStore},
    web3::{filter_transfer_logs, make_tx, sent_to_us, ERC20Transfer, EthAddress},
};
use futures::{compat::Future01CompatExt, TryFutureExt};

use async_trait::async_trait;
use clarity::Signature;
use log::{debug, error, trace};
use parking_lot::RwLock;
use sha3::{Digest, Keccak256 as Sha3};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;

use hyper::StatusCode;
use log::info;
use num_bigint::BigUint;
use reqwest::{Client, Response as HttpResponse};
use serde::{de::Error as DeserializeError, Deserialize, Deserializer, Serialize};
use serde_json::json;
use std::net::SocketAddr;
use std::{marker::PhantomData, str::FromStr, time::Duration};
use std::{str, u64};
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use url::Url;
use uuid::Uuid;
use web3::{
    api::Web3,
    futures::Future as Future01,
    transports::Http,
    types::Transaction,
    types::{Address, BlockNumber, CallRequest, H256, U256},
};

use interledger_http::error::*;
use interledger_settlement::core::{
    engines_api::create_settlement_engine_filter,
    scale_with_precision_loss,
    types::{ApiResponse, LeftoversStore, Quantity, SettlementEngine, CONVERSION_ERROR_TYPE},
};
use secrecy::Secret;

const MAX_RETRIES: usize = 10;
const ETH_CREATE_ACCOUNT_PREFIX: &[u8] = b"ilp-ethl-create-account-message";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentDetailsRequest {
    challenge: Vec<u8>,
}

impl PaymentDetailsRequest {
    fn new(challenge: Vec<u8>) -> Self {
        PaymentDetailsRequest { challenge }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentDetailsResponse {
    to: Addresses,
    signature: Signature,
    challenge: Option<Vec<u8>>,
}

impl PaymentDetailsResponse {
    fn new(to: Addresses, signature: Signature, challenge: Option<Vec<u8>>) -> Self {
        PaymentDetailsResponse {
            to,
            signature,
            challenge,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EthereumLedgerSettlementEngine<S, Si, A> {
    store: S,
    signer: Si,
    account_type: PhantomData<A>,

    // Configuration data
    web3: Web3<Http>,
    address: Addresses,
    chain_id: u8,
    confirmations: u8,
    poll_frequency: Duration,
    connector_url: Url,
    asset_scale: u8,
    net_version: String,
    challenges: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

pub struct EthereumLedgerSettlementEngineBuilder<'a, S, Si, A> {
    store: S,
    signer: Si,

    /// Ethereum Endpoint, default localhost:8545
    ethereum_endpoint: Option<&'a str>,
    chain_id: Option<u8>,
    confirmations: Option<u8>,
    poll_frequency: Option<Duration>,
    connector_url: Option<Url>,
    token_address: Option<Address>,
    asset_scale: Option<u8>,
    watch_incoming: bool,
    account_type: PhantomData<A>,
}

impl<'a, S, Si, A> EthereumLedgerSettlementEngineBuilder<'a, S, Si, A>
where
    S: EthereumStore<Account = A>
        + LeftoversStore<AccountId = String, AssetType = BigUint>
        + Clone
        + Send
        + Sync
        + 'static,
    Si: EthereumLedgerTxSigner + Clone + Send + Sync + 'static,
    A: EthereumAccount<AccountId = String> + Clone + Send + Sync + 'static,
{
    pub fn new(store: S, signer: Si) -> Self {
        Self {
            store,
            signer,
            ethereum_endpoint: None,
            chain_id: None,
            confirmations: None,
            poll_frequency: None,
            connector_url: None,
            token_address: None,
            asset_scale: None,
            watch_incoming: false,
            account_type: PhantomData,
        }
    }

    pub fn token_address(&mut self, token_address: Option<Address>) -> &mut Self {
        self.token_address = token_address;
        self
    }

    pub fn ethereum_endpoint(&mut self, endpoint: &'a str) -> &mut Self {
        self.ethereum_endpoint = Some(endpoint);
        self
    }

    pub fn asset_scale(&mut self, asset_scale: u8) -> &mut Self {
        self.asset_scale = Some(asset_scale);
        self
    }

    pub fn chain_id(&mut self, chain_id: u8) -> &mut Self {
        self.chain_id = Some(chain_id);
        self
    }

    pub fn confirmations(&mut self, confirmations: u8) -> &mut Self {
        self.confirmations = Some(confirmations);
        self
    }

    /// The frequency to check for new blocks for in milliseconds
    pub fn poll_frequency(&mut self, poll_frequency: u64) -> &mut Self {
        self.poll_frequency = Some(Duration::from_millis(poll_frequency));
        self
    }

    pub fn watch_incoming(&mut self, watch_incoming: bool) -> &mut Self {
        self.watch_incoming = watch_incoming;
        self
    }

    pub fn connector_url(&mut self, connector_url: &'a str) -> &mut Self {
        self.connector_url = Some(connector_url.parse().unwrap());
        self
    }

    pub async fn connect(&self) -> EthereumLedgerSettlementEngine<S, Si, A> {
        let ethereum_endpoint = if let Some(ref ethereum_endpoint) = self.ethereum_endpoint {
            &ethereum_endpoint
        } else {
            "http://localhost:8545"
        };
        let chain_id = if let Some(chain_id) = self.chain_id {
            chain_id
        } else {
            1
        };
        let connector_url = if let Some(connector_url) = self.connector_url.clone() {
            connector_url
        } else {
            "http://localhost:7771".parse().unwrap()
        };
        let confirmations = if let Some(confirmations) = self.confirmations {
            confirmations
        } else {
            6
        };
        let poll_frequency = if let Some(poll_frequency) = self.poll_frequency {
            poll_frequency
        } else {
            Duration::from_secs(5)
        };
        let asset_scale = if let Some(asset_scale) = self.asset_scale {
            asset_scale
        } else {
            18
        };

        let (eloop, transport) = Http::new(ethereum_endpoint).unwrap();
        eloop.into_remote();
        let web3 = Web3::new(transport);
        let address = Addresses {
            own_address: self.signer.address(),
            token_address: self.token_address,
        };

        let store = self.store.clone();
        let signer = self.signer.clone();
        let watch_incoming = self.watch_incoming;
        let net_version = web3
            .net()
            .version()
            .compat()
            .await
            .unwrap_or_else(|_| chain_id.to_string());
        let engine = EthereumLedgerSettlementEngine {
            web3,
            store,
            signer,
            address,
            chain_id,
            confirmations,
            poll_frequency,
            connector_url,
            asset_scale,
            net_version,
            account_type: PhantomData,
            challenges: Arc::new(RwLock::new(HashMap::new())),
        };
        if watch_incoming {
            engine.notify_connector_on_incoming_settlement();
        }
        engine
    }
}

impl<S, Si, A> EthereumLedgerSettlementEngine<S, Si, A>
where
    S: EthereumStore<Account = A>
        + LeftoversStore<AccountId = String, AssetType = BigUint>
        + Clone
        + Send
        + Sync
        + 'static,
    Si: EthereumLedgerTxSigner + Clone + Send + Sync + 'static,
    A: EthereumAccount<AccountId = String> + Clone + Send + Sync + 'static,
{
    /// Periodically spawns a job every `self.poll_frequency` that notifies the
    /// Settlement Engine's connectors about transactions which are sent to the
    /// engine's address.
    pub fn notify_connector_on_incoming_settlement(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(this.poll_frequency);
            loop {
                interval.tick().await;
                let _ = this.handle_received_transactions().await;
            }
        });
    }

    /// Routine for notifying the connector about incoming transactions.
    /// Algorithm (heavily unoptimized):
    /// 1. Fetch the last observed block number
    /// 2. Fetch the current block number from Ethereum
    /// 3. Fetch all blocks since the last observed one,
    ///    until $(current block number - confirmations), where $confirmations
    ///    is a security parameter to be safe against block reorgs.
    /// 4. For each block (in parallel):
    ///     For each transaction (in parallel):
    ///     1. Skip if it is not sent to our address or have 0 value.
    ///     2. Fetch the id that matches its sender from the store
    ///     3. Notify the connector about it by making a POST request to the connector's
    ///        /accounts/$id/settlements endpoint with transaction's value as the
    ///        body. This call is retried if it fails.
    /// 5. Save (current block number - confirmations) to be used as
    ///    last observed data for the next call of this function.
    // Optimization: This will make 1 settlement API request to
    // the connector per transaction that comes to us. Since
    // we're scanning across multiple blocks, we want to
    // optimize it so that: it gathers a mapping of
    // (accountId=>amount) across all transactions and blocks, and
    // only makes 1 transaction per accountId with the
    // appropriate amount.
    pub async fn handle_received_transactions(&self) -> Result<(), ()> {
        let confirmations = self.confirmations;
        let web3 = self.web3.clone();
        let our_address = self.address.own_address;
        let token_address = self.address.token_address;
        let net_version = self.net_version.clone();

        // get the current block number
        let current_block = web3
            .eth()
            .block_number()
            .compat()
            .map_err(move |err| error!("Could not fetch current block number {:?}", err))
            .await?;

        // get the safe number of blocks to avoid reorgs
        let to_block = current_block - confirmations;

        // If we are just starting up, fetch only the most recent block
        // Note this means we will ignore transactions that were received before
        // the first time the settlement engine was started.
        let last_observed_block = self
            .store
            .load_recently_observed_block(net_version.clone())
            .await?;
        let from_block = if let Some(last_observed_block) = last_observed_block {
            if to_block == last_observed_block {
                // We already processed the latest block
                return Ok(());
            } else {
                last_observed_block + 1
            }
        } else {
            // Check only the latest block
            to_block
        };

        trace!("Fetching txs from block {} until {}", from_block, to_block);

        // TODO: Make this work with join_all!
        if let Some(token_address) = token_address {
            // get all erc20 transactions
            let transfers: Vec<ERC20Transfer> = filter_transfer_logs(
                web3.clone(),
                token_address,
                None,
                Some(our_address),
                BlockNumber::Number(from_block.low_u64()),
                BlockNumber::Number(to_block.low_u64()),
            )
            .await?;

            for transfer in transfers {
                self.notify_erc20_transfer(transfer, token_address).await?;
            }
        } else {
            let checked_blocks = from_block.low_u64()..=to_block.low_u64();
            for block_num in checked_blocks {
                let maybe_block = self
                    .web3
                    .eth()
                    .block_with_txs(BlockNumber::Number(block_num).into())
                    .map_err(move |err| {
                        error!("Got error while getting block {}: {:?}", block_num, err)
                    })
                    .compat()
                    .await?;
                if let Some(block) = maybe_block {
                    for tx in block.transactions {
                        self.notify_eth_transfer(tx).await?;
                    }
                }
            }
        }

        trace!("Processed all transactions up to block {}", to_block);
        // now that all transactions have been processed successfully, we
        // can save `to_block` as the latest observed block
        self.store
            .save_recently_observed_block(net_version, to_block)
            .await
    }

    /// Submits an ERC20 transfer object's data to the connector
    // todo: Try combining the body of this function with `notify_eth_transfer`
    async fn notify_erc20_transfer(
        &self,
        transfer: ERC20Transfer,
        token_address: Address,
    ) -> Result<(), ()> {
        let tx_hash = transfer.tx_hash;
        let addr = Addresses {
            own_address: transfer.from,
            token_address: Some(token_address),
        };
        let amount = transfer.amount;
        let is_tx_processed = self
            .store
            .check_if_tx_processed(tx_hash)
            .map_err(move |_| error!("Error when querying store about transaction: {:?}", tx_hash))
            .await?;

        if !is_tx_processed {
            let account_id = self.store.load_account_id_from_address(addr).await?;

            debug!("Notifying connector about incoming ERC20 transaction for account {} for amount: {} (tx hash: {})", account_id, amount, tx_hash);
            self.notify_connector(account_id.to_string(), amount.to_string(), tx_hash)
                .await?;
            self.store.mark_tx_processed(tx_hash).await?;
        }

        Ok(())
    }

    /// Notifies the connector about an Ethereum transaction. Requires parsing the transaction directly
    async fn notify_eth_transfer(&self, tx: Transaction) -> Result<(), ()> {
        let our_address = self.address.own_address;
        let tx_hash = tx.hash;
        let is_tx_processed = self
            .store
            .check_if_tx_processed(tx_hash)
            .map_err(move |_| error!("Error when querying store about transaction: {:?}", tx_hash))
            .await?;

        // This is a no-op if the operation was already processed
        if !is_tx_processed {
            if let Some((from, amount)) = sent_to_us(tx, our_address) {
                trace!(
                    "Got transaction for our account from {} for amount {}",
                    from,
                    amount
                );
                if amount > U256::from(0) {
                    // if the tx was for us and had some non-0 amount, then let
                    // the connector know
                    let addr = Addresses {
                        own_address: from,
                        token_address: None,
                    };

                    let account_id = self.store.load_account_id_from_address(addr).await?;
                    self.notify_connector(account_id.to_string(), amount.to_string(), tx_hash)
                        .await?;
                    self.store.mark_tx_processed(tx_hash).await?;
                }
            }
        }
        Ok(())
    }

    async fn notify_connector(
        &self,
        account_id: String,
        amount: String,
        tx_hash: H256,
    ) -> Result<(), ()> {
        let engine_scale = self.asset_scale;
        let mut url = self.connector_url.clone();
        url.path_segments_mut()
            .expect("Invalid connector URL")
            .push("accounts")
            .push(&account_id.clone())
            .push("settlements");
        debug!(
            "Making POST to {:?} {:?} about {:?}",
            url,
            amount,
            format!("{:?}", tx_hash)
        );

        let account_id_clone = account_id.clone();
        let amount_clone = amount.clone();
        let action = move || {
            let client = Client::new();
            let account_id = account_id.clone();
            let amount = amount.clone();
            client
                .post(url.as_ref())
                .header("Idempotency-Key", format!("{:?}", tx_hash))
                .json(&json!(Quantity::new(amount.clone(), engine_scale)))
                .send()
                .map_err(move |err| {
                    error!(
                        "Error notifying Accounting System's account: {:?}, amount: {:?}: {:?}",
                        account_id, amount, err
                    );
                })
                .compat()
        };
        let ret = Retry::spawn(
                ExponentialBackoff::from_millis(10).take(MAX_RETRIES),
                action,
            )
            .compat()
            .map_err(move |_| {
                error!("Exceeded max retries when notifying connector about account {:?} for amount {:?} and transaction hash {:?}. Please check your API.", account_id_clone, amount_clone, tx_hash)
            })
            .await?;
        trace!("Accounting system responded with {:?}", ret);
        Ok(())
    }

    /// Helper function which submits an Ethereum ledger transaction to `to` for `amount`.
    /// If called with `token_address`, it makes an ERC20 transaction instead.
    /// Due to the lack of an API to create and sign the transaction
    /// automatically it has to be done manually as follows:
    /// 1. fetch the account's nonce
    /// 2. construct the raw transaction using the nonce and the provided parameters
    /// 3. Sign the transaction (along with the chain id, due to EIP-155)
    /// 4. Submit the RLP-encoded transaction to the network
    async fn settle_to(
        &self,
        to: Address,
        amount: U256,
        token_address: Option<Address>,
    ) -> Result<Option<H256>, ()> {
        if amount == U256::from(0) {
            return Ok(None);
        }
        let web3 = self.web3.clone();
        let own_address = self.address.own_address;
        let chain_id = self.chain_id;
        let signer = self.signer.clone();

        let mut tx = make_tx(to, amount, token_address);
        let value = U256::from_str(&tx.value.to_string()).unwrap();
        let estimate_gas_destination = if let Some(token_address) = token_address {
            token_address
        } else {
            to
        };

        let gas_price = web3
            .eth()
            .gas_price()
            .compat()
            .map_err(|err| error!("could not fetch gas price {:?}", err))
            .await?;
        let gas_amount = match web3
            .eth()
            .estimate_gas(
                CallRequest {
                    to: estimate_gas_destination,
                    from: None,
                    gas: None,
                    gas_price: None,
                    value: Some(value),
                    data: Some(tx.data.clone().into()),
                },
                None,
            )
            .compat()
            .await
        {
            // if the gas estimation fails, use a default amount that will never
            // fail (eth transactions take 21000 gas, and ERC20 transactions are
            // between 50-70k)
            // This call will fail on Geth nodes until
            // https://github.com/ethereum/go-ethereum/issues/2586 is fixed
            // Note: Was fixed in rust-web3 master: https://github.com/tomusdrw/rust-web3/pull/291
            Ok(amount) => amount,
            Err(_) => U256::from(100_000),
        };
        let nonce = web3
            .eth()
            .transaction_count(own_address, Some(BlockNumber::Pending))
            .compat()
            .map_err(|err| error!("could not fetch nonce {:?}", err))
            .await?;

        tx.gas_price = gas_price;
        tx.gas = gas_amount;
        tx.nonce = nonce;

        let signed_tx = signer.sign_raw_tx(tx.clone(), chain_id); // 3

        let action = move || {
            trace!("Sending tx to Ethereum: {}", hex::encode(&signed_tx));
            web3.eth() // 4
                // TODO use send_transaction_with_confirmation
                .send_raw_transaction(signed_tx.clone().into())
                .map_err(|err| {
                    error!("Error sending transaction to Ethereum ledger: {:?}", err);
                    err
                })
        };
        let tx_hash = Retry::spawn(
            ExponentialBackoff::from_millis(10).take(MAX_RETRIES),
            action,
        )
        .compat()
        .map_err(move |_err| {
            error!("Unable to submit tx to Ethereum ledger");
        })
        .await?;
        debug!("Transaction submitted. Hash: {:?}", tx_hash);
        Ok(Some(tx_hash))
    }

    /// Helper function that returns the addresses associated with an
    /// account from a given string account id
    async fn load_account(&self, account_id: String) -> Result<(String, Addresses), String> {
        let store = self.store.clone();
        let addr = self.address;
        let account_id_clone = account_id.clone();
        let addresses = store
            .load_account_addresses(vec![account_id.clone()])
            .map_err(move |_err| {
                let error_msg = format!("[{:?}] Error getting account: {}", addr, account_id_clone);
                error!("{}", error_msg);
                error_msg
            })
            .await?;
        Ok((account_id, addresses[0]))
    }
}

#[async_trait]
impl<S, Si, A> SettlementEngine for EthereumLedgerSettlementEngine<S, Si, A>
where
    S: EthereumStore<Account = A>
        + LeftoversStore<AccountId = String, AssetType = BigUint>
        + Clone
        + Send
        + Sync
        + 'static,
    Si: EthereumLedgerTxSigner + Clone + Send + Sync + 'static,
    A: EthereumAccount<AccountId = String> + Clone + Send + Sync + 'static,
{
    /// Settlement Engine's function that corresponds to the
    /// /accounts/:id/ endpoint (POST). It queries the connector's
    /// accounts/:id/messages with a "PaymentDetailsRequest" that gets forwarded
    /// to the connector's peer that matches the provided account id, which
    /// finally gets forwarded to the peer SE's receive_message endpoint which
    /// responds with its Ethereum and Token addresses. Upon
    /// receival of Ethereum and Token addresses from the peer, it saves them in
    /// the store.
    async fn create_account(&self, account_id: String) -> Result<ApiResponse, ApiError> {
        // let self_clone = self.clone();
        // let store: S = self.store.clone();
        // let signer = self.signer.clone();
        // let address = self.address;

        // We make a POST request to OUR connector's `messages`
        // endpoint. This will in turn send an outgoing
        // request to its peer connector, which will ask its
        // own engine about its settlement information. Then,
        // we store that information and use it when
        // performing settlements.
        let idempotency_uuid = Uuid::new_v4().to_hyphenated().to_string();
        let challenge = Uuid::new_v4().to_hyphenated().to_string();
        let challenge = challenge.into_bytes();
        let challenge_clone = challenge.clone();
        let client = Client::new();
        let mut url = self.connector_url.clone();

        // send a payment details request (we send them a challenge)
        url.path_segments_mut()
            .expect("Invalid connector URL")
            .push("accounts")
            .push(&account_id.to_string())
            .push("messages");
        let body =
            serde_json::to_string(&PaymentDetailsRequest::new(challenge_clone.clone())).unwrap();
        let url_clone = url.clone();
        let action = move || {
            client
                .post(url.as_ref())
                .header("Content-Type", "application/octet-stream")
                .header("Idempotency-Key", idempotency_uuid.clone())
                .body(body.clone())
                .send()
                .compat()
        };

        let resp = Retry::spawn(
            ExponentialBackoff::from_millis(10).take(MAX_RETRIES),
            action,
        )
        .compat()
        .map_err(move |err| {
            let err = format!("Couldn't notify connector {:?}", err);
            error!("{}", err);
            ApiError::internal_server_error().detail(err)
        })
        .await?;
        let payment_details = parse_body_into_payment_details(resp).await?;

        let data = prefixed_message(challenge_clone);
        let challenge_hash = Sha3::digest(&data);
        let recovered_address = payment_details.signature.recover(&challenge_hash);
        trace!("Received payment details {:?}", payment_details);
        match recovered_address {
            Ok(recovered_address) => {
                if recovered_address.as_bytes() != &payment_details.to.own_address.as_bytes()[..] {
                    let error_msg = format!(
                        "Recovered address did not match: {:?}. Expected {:?}",
                        recovered_address.to_string(),
                        payment_details.to
                    );
                    error!("{}", error_msg);
                    return Err(ApiError::internal_server_error().detail(error_msg));
                }
            }
            Err(error_msg) => {
                let error_msg = format!("Could not recover address {:?}", error_msg);
                error!("{}", error_msg);
                return Err(ApiError::internal_server_error().detail(error_msg));
            }
        };

        // ACK BACK
        if let Some(challenge) = payment_details.challenge {
            // if we were challenged, we must respond
            let data = prefixed_message(challenge);
            let signature = self.signer.sign_message(&data);
            let resp = {
                // Respond with our address, a signature,
                // and no challenge, since we already sent
                // them one earlier
                let ret = PaymentDetailsResponse::new(self.address, signature, None);
                serde_json::to_string(&ret).unwrap()
            };
            let idempotency_uuid = Uuid::new_v4().to_hyphenated().to_string();
            let client = Client::new();
            let action = move || {
                client
                    .post(url_clone.as_ref())
                    .header("Content-Type", "application/octet-stream")
                    .header("Idempotency-Key", idempotency_uuid.clone())
                    .body(resp.clone())
                    .send()
                    .compat()
                    .map_err(|err| error!("{}", err))
            };

            tokio::spawn(
                Retry::spawn(
                    ExponentialBackoff::from_millis(10).take(MAX_RETRIES),
                    action,
                )
                .compat()
                .map_err(|err| error!("{:?}", err)),
            );
        }

        let data = HashMap::from_iter(vec![(account_id, payment_details.to)]);
        self.store
            .save_account_addresses(data)
            .map_err(move |err| {
                let err_type = ApiErrorType {
                    r#type: &ProblemType::Default,
                    title: "Store connection error",
                    status: StatusCode::BAD_REQUEST,
                };
                let err = format!("Couldn't connect to store {:?}", err);
                error!("{}", err);
                ApiError::from_api_error_type(&err_type).detail(err)
            })
            .await?;
        Ok(ApiResponse::Default)
    }

    // Deletes an account from the engine
    async fn delete_account(&self, account_id: String) -> Result<ApiResponse, ApiError> {
        let store = self.store.clone();
        let account_id_clone = account_id.clone();
        // Ensure account exists
        self.load_account(account_id.clone())
            .map_err(|err| {
                let error_msg = format!("Error loading account {:?}", err);
                error!("{}", error_msg);
                ApiError::internal_server_error().detail(error_msg)
            })
            .await?;

        // if the load call succeeds, then the account exists and we must:
        // 1. delete their addresses
        // 2. clear their uncredited settlement amounts
        store
            .clear_uncredited_settlement_amount(account_id_clone.clone())
            .map_err(|err| {
                error!("Couldn't clear uncredit settlement amount {:?}", err);
                ApiError::internal_server_error()
            })
            .await?;
        store
            .delete_accounts(vec![account_id])
            .map_err(move |_| {
                let error_msg = "Couldn't delete account".to_string();
                error!("{}", error_msg);
                ApiError::internal_server_error()
            })
            .await?;

        Ok(ApiResponse::Default)
    }

    /// Settlement Engine's function that corresponds to the
    /// /accounts/:id/messages endpoint (POST).
    /// The body is a challenge issued by the peer's settlement engine which we
    /// must sign to prove ownership of our address
    async fn receive_message(
        &self,
        account_id: String,
        body: Vec<u8>,
    ) -> Result<ApiResponse, ApiError> {
        let address = self.address;
        let store = self.store.clone();
        // We are only returning our information, so
        // there is no need to return any data about the
        // provided account.
        // If we received a SYN, we respond with a signed message
        if let Ok(req) = serde_json::from_slice::<PaymentDetailsRequest>(&body) {
            // debug!(
            //     "Received account creation request. Responding with our account's details {} {:?}",
            //     account_id, address
            // );
            // Otherwise, we save the received address
            let data = prefixed_message(req.challenge);
            let signature = self.signer.sign_message(&data);
            let resp = {
                let challenge = Uuid::new_v4().to_hyphenated().to_string();
                let challenge = challenge.into_bytes();
                let mut guard = self.challenges.write();
                (*guard).insert(account_id, challenge.clone());
                // Respond with our address, a signature, and our own challenge
                let ret = PaymentDetailsResponse::new(address, signature, Some(challenge));
                serde_json::to_vec(&ret).unwrap()
            };
            Ok(ApiResponse::Data(resp.into()))
        } else if let Ok(resp) = serde_json::from_slice::<PaymentDetailsResponse>(&body) {
            debug!("Received payment details: {:?}", resp);
            let challenge = self.challenges.read().clone();
            let challenge = if let Some(challenge) = challenge.get(&account_id) {
                challenge
            } else {
                // if we did not send them a challenge, do nothing
                return Ok(ApiResponse::Default);
            };

            // if we sent them a challenge, we will verify the received
            // signature and address, and if sig verification passes, we'll
            // save them in our store
            let data = prefixed_message(challenge.to_vec());
            let challenge_hash = Sha3::digest(&data);
            match resp.signature.recover(&challenge_hash) {
                Ok(recovered_address) => {
                    if recovered_address.as_bytes() == &resp.to.own_address.as_bytes()[..] {
                        // If the signature was correct, save the provided address to the store
                        let data = HashMap::from_iter(vec![(account_id, resp.to)]);
                        store
                            .save_account_addresses(data)
                            .map_err(move |err| {
                                let error_msg = format!("Couldn't connect to store {:?}", err);
                                error!("{}", error_msg);
                                ApiError::internal_server_error() // .detail(error_msg)
                            })
                            .await?;
                        Ok(ApiResponse::Default)
                    } else {
                        // If the signature did not match, we should return an error
                        Err(ApiError::bad_request())
                    }
                }
                Err(error_msg) => {
                    error!("{}", error_msg);
                    Err(ApiError::bad_request())
                }
            }
        } else {
            // Non PaymentDetails requests/responses should be errored out
            let error_msg = "Ignoring message that was neither a PaymentDetailsRequest nor a PaymentDetailsResponse";
            error!("{}", error_msg);
            let err_type = ApiErrorType {
                r#type: &ProblemType::Default,
                title: "Invalid message type",
                status: StatusCode::BAD_REQUEST,
            };
            Err(ApiError::from_api_error_type(&err_type).detail(error_msg))
        }
    }

    /// Settlement Engine's function that corresponds to the
    /// /accounts/:id/settlements endpoint (POST). It performs an Ethereum
    /// onchain transaction to the Ethereum Address that corresponds to the
    /// provided account id, for the amount specified in the message's body. If
    /// the account is associated with an ERC20 token, it makes an ERC20 call instead.
    async fn send_money(
        &self,
        account_id: String,
        body: Quantity,
    ) -> Result<ApiResponse, ApiError> {
        let engine_scale = self.asset_scale;
        let connector_scale = body.scale;

        let amount_from_connector = match BigUint::from_str(&body.amount) {
            Ok(a) => a,
            Err(_err) => {
                let error_msg = format!("Error converting to BigUint {:?}", _err);
                error!("{:?}", error_msg);
                return Err(ApiError::from_api_error_type(&CONVERSION_ERROR_TYPE).detail(error_msg));
            }
        };
        let (amount, precision_loss) =
            scale_with_precision_loss(amount_from_connector, engine_scale, connector_scale);

        let uncredited_settlement_amount = self
            .store
            .load_uncredited_settlement_amount(account_id.clone(), engine_scale)
            .map_err(move |err| {
                let error_msg = format!("Error loading leftovers {:?}", err);
                error!("{}", error_msg);
                ApiError::internal_server_error().detail(error_msg)
            })
            .await?;
        let (account_id, addresses) = self
            .load_account(account_id)
            .map_err(move |err| {
                let error_msg = format!("Error loading account {:?}", err);
                error!("{}", error_msg);
                ApiError::internal_server_error().detail(error_msg)
            })
            .await?;

        debug!(
            "Sending settlement to account {} (Ethereum address: {}) for amount: {}{}",
            account_id,
            addresses.own_address,
            amount,
            if let Some(token_address) = addresses.token_address {
                format!(" (token address: {}", token_address)
            } else {
                "".to_string()
            }
        );

        // Typecast to web3::U256
        let total_amount = amount + uncredited_settlement_amount;
        let total_amount = match U256::from_dec_str(&total_amount.to_string()) {
            Ok(a) => a,
            Err(_err) => {
                let error_msg = format!("Error converting to U256 {:?}", _err);
                error!("{:?}", error_msg);
                return Err(ApiError::from_api_error_type(&CONVERSION_ERROR_TYPE).detail(error_msg));
            }
        };

        // Execute the settlement
        self.settle_to(addresses.own_address, total_amount, addresses.token_address)
            .map_err(move |_| {
                let error_msg = "Error connecting to the blockchain.".to_string();
                error!("{}", error_msg);
                let err_type = ApiErrorType {
                    r#type: &ProblemType::Default,
                    title: "Blockchain connection error",
                    status: StatusCode::BAD_GATEWAY,
                };
                ApiError::from_api_error_type(&err_type).detail(error_msg)
            })
            .await?;

        // Save any leftovers
        self.store
            .save_uncredited_settlement_amount(account_id, (precision_loss, connector_scale))
            .map_err(move |_| {
                let error_msg = "Couldn't save uncredited settlement amount.".to_string();
                error!("{}", error_msg);
                ApiError::internal_server_error()
            })
            .await?;

        Ok(ApiResponse::Default)
    }
}

async fn parse_body_into_payment_details(
    resp: HttpResponse,
) -> Result<PaymentDetailsResponse, ApiError> {
    let body = resp
        .bytes()
        .map_err(|err| {
            let err = format!("Couldn't retrieve body {:?}", err);
            error!("{}", err);
            ApiError::internal_server_error().detail(err)
        })
        .await?;
    serde_json::from_slice::<PaymentDetailsResponse>(&body).map_err(|err| {
        let err = format!(
            "Couldn't parse body {:?} into payment details {:?}",
            body, err
        );
        error!("{}", err);
        ApiError::internal_server_error().detail(err)
    })
}

fn prefixed_message(challenge: Vec<u8>) -> Vec<u8> {
    let mut ret = ETH_CREATE_ACCOUNT_PREFIX.to_vec();
    ret.extend(challenge);
    ret
}

// Redis helpers to run binaries
#[cfg(feature = "redis")]
pub mod redis_bin {
    use super::*;
    use crate::backends::redis::*;
    use redis_crate::ConnectionInfo;
    use redis_crate::IntoConnectionInfo;

    #[doc(hidden)]
    #[allow(clippy::all)]
    pub async fn run_ethereum_engine(opt: EthereumLedgerOpt) -> Result<(), ()> {
        // TODO make key compatible with
        // https://github.com/tendermint/signatory to have HSM sigs

        let ethereum_store = EthereumLedgerRedisStoreBuilder::new(opt.redis_connection.clone())
            .connect()
            .await?;

        let engine = EthereumLedgerSettlementEngineBuilder::new(
            ethereum_store.clone(),
            opt.private_key.clone(),
        )
        .ethereum_endpoint(&opt.ethereum_url)
        .chain_id(opt.chain_id)
        .connector_url(&opt.connector_url)
        .confirmations(opt.confirmations)
        .asset_scale(opt.asset_scale)
        .poll_frequency(opt.poll_frequency)
        .watch_incoming(opt.watch_incoming)
        .token_address(opt.token_address)
        .connect()
        .await;

        let api = create_settlement_engine_filter(engine, ethereum_store);
        tokio::spawn(warp::serve(api).bind(opt.settlement_api_bind_address));
        info!(
            "Ethereum Settlement Engine listening on: {}",
            &opt.settlement_api_bind_address
        );
        Ok(())
    }

    #[derive(Deserialize, Clone)]
    pub struct EthereumLedgerOpt {
        pub private_key: Secret<String>,
        pub settlement_api_bind_address: SocketAddr,
        pub ethereum_url: String,
        pub token_address: Option<EthAddress>,
        pub connector_url: String,
        #[serde(deserialize_with = "deserialize_redis_connection", alias = "redis_url")]
        pub redis_connection: ConnectionInfo,
        // Although the length of `chain_id` seems to be not limited on its specs,
        // u8 seems sufficient at this point.
        pub chain_id: u8,
        pub confirmations: u8,
        pub asset_scale: u8,
        pub poll_frequency: u64,
        pub watch_incoming: bool,
    }

    fn deserialize_redis_connection<'de, D>(deserializer: D) -> Result<ConnectionInfo, D::Error>
    where
        D: Deserializer<'de>,
    {
        Url::parse(&String::deserialize(deserializer)?)
            .map_err(|err| DeserializeError::custom(format!("Invalid URL: {:?}", err)))?
            .into_connection_info()
            .map_err(|err| {
                DeserializeError::custom(format!(
                    "Error converting into Redis connection info: {:?}",
                    err
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_helpers::{
        fixtures::{ALICE, BOB, MESSAGES_API},
        test_engine, test_store, TestAccount,
    };
    use lazy_static::lazy_static;
    use mockito;
    use secrecy::Secret;

    lazy_static! {
        pub static ref ALICE_PK: Secret<String> = Secret::new(String::from(
            "380eb0f3d505f087e438eca80bc4df9a7faa24f868e69fc0440261a0fc0567dc"
        ));
        pub static ref BOB_PK: Secret<String> = Secret::new(String::from(
            "cc96601bc52293b53c4736a12af9130abf347669b3813f9ec4cafdf6991b087e"
        ));
    }
    #[tokio::test]
    async fn test_create_get_account() {
        let bob: TestAccount = BOB.clone();

        let challenge = Uuid::new_v4().to_hyphenated().to_string();
        let signature = BOB_PK.clone().sign_message(&challenge.clone().into_bytes());

        let body_se_data = serde_json::to_string(&PaymentDetailsResponse {
            to: Addresses {
                own_address: bob.address,
                token_address: None,
            },
            signature,
            challenge: None,
        })
        .unwrap();

        // simulate our connector that accepts the request, forwards it to the
        // peer's connector and returns the peer's se addresses
        let m = mockito::mock("POST", MESSAGES_API.clone())
            .with_status(200)
            .with_body(body_se_data)
            .expect(1) // only 1 request is made to the connector (idempotency works properly)
            .create();
        let connector_url = mockito::server_url();

        // alice sends a create account request to her engine
        // which makes a call to bob's connector which replies with bob's
        // address and signature on the challenge
        let store = test_store(bob.clone(), false, false, false);
        let engine = test_engine(
            store.clone(),
            ALICE_PK.clone(),
            0,
            &connector_url,
            None,
            false,
        )
        .await;

        // the signed message does not match.
        // (We are not able to make Mockito capture the challenge and return a
        // signature on it.)
        let ret: ApiError = engine.create_account(bob.id).await.unwrap_err();
        assert_eq!(ret.status.as_u16(), 500);
        let error_msg = ret.detail.unwrap();
        assert!(error_msg.starts_with("Recovered address did not match:"));
        assert!(error_msg.ends_with("Expected Addresses { own_address: 0x9b925641c5ef3fd86f63bff2da55a0deeafd1263, token_address: None }"));

        m.assert();
    }

    #[tokio::test]
    async fn test_receive_message() {
        let bob: TestAccount = BOB.clone();

        let challenge = Uuid::new_v4().to_hyphenated().to_string().into_bytes();
        let signed_challenge = prefixed_message(challenge.clone());

        let signature = ALICE_PK.clone().sign_message(&signed_challenge);

        let store = test_store(ALICE.clone(), false, false, false);
        let engine = test_engine(
            store.clone(),
            ALICE_PK.clone(),
            0,
            "http://127.0.0.1:8770",
            None,
            false,
        )
        .await;

        // Alice's engine receives a challenge by Bob.
        let c = serde_json::to_vec(&PaymentDetailsRequest::new(challenge)).unwrap();
        let ret = engine.receive_message(bob.id.to_string(), c).await.unwrap();
        let alice_addrs = Addresses {
            own_address: ALICE.address,
            token_address: None,
        };
        let data = match ret {
            ApiResponse::Default => panic!("got default when we expected data"),
            ApiResponse::Data(b) => b,
        };
        let data: PaymentDetailsResponse = serde_json::from_slice(&data).unwrap();
        // The returned addresses must be Alice's
        assert_eq!(data.to, alice_addrs);
        // The returned signature must be Alice's sig.
        assert_eq!(data.signature, signature);
        // The returned challenge is sent over to Bob, who will use it to send
        // his address back
        assert!(data.challenge.is_some());

        // Alice's engine now receives Bob's addresses
        let challenge = data.challenge.unwrap();
        let signed_challenge = prefixed_message(challenge.clone());
        let signature = BOB_PK.clone().sign_message(&signed_challenge);
        let bob_addrs = Addresses {
            own_address: BOB.address,
            token_address: None,
        };
        let c =
            serde_json::to_vec(&PaymentDetailsResponse::new(bob_addrs, signature, None)).unwrap();
        let ret = engine.receive_message(bob.id.to_string(), c).await.unwrap();
        if let ApiResponse::Data(_) = ret {
            panic!("got data when we expected default ret")
        }

        // check that alice's store got updated with bob's addresses
        let addrs = store
            .load_account_addresses(vec![bob.id.to_string()])
            .await
            .unwrap();
        assert_eq!(addrs[0], bob_addrs);
    }
}
