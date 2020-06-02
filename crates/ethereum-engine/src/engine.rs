#![allow(clippy::needless_lifetimes)] // False positive on async-trait
use super::utils::types::{Addresses, EthereumAccount, EthereumStore};
use interledger_settlement::core::{
    scale_with_precision_loss,
    types::{ApiResponse, LeftoversStore, Quantity, SettlementEngine, CONVERSION_ERROR_TYPE},
};
use num_bigint::BigUint;

use futures::{compat::Future01CompatExt, TryFutureExt};
use parking_lot::RwLock;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use url::Url;

use crate::{EthClient, IncomingTransfer};
use ::http::StatusCode;
use async_trait::async_trait;
use interledger_errors::*;
use log::{debug, error, trace};
use reqwest::{Client as RequestClient, Response as HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::iter::FromIterator;
use std::str::FromStr;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use uuid::Uuid;

use ethers::prelude::*;

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
pub struct EthereumLedgerSettlementEngine<'a, S, A> {
    pub store: S,
    pub eth_client: EthClient<'a>,

    pub asset_scale: u8,
    pub chain_id: u8,
    pub confirmations: u8,
    pub connector_url: Url,
    pub challenges: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    pub account_type: PhantomData<A>,
}

impl<S, A> EthereumLedgerSettlementEngine<'static, S, A>
where
    S: EthereumStore<Account = A>
        + LeftoversStore<AccountId = String, AssetType = BigUint>
        + Clone
        + Send
        + Sync
        + 'static,
    A: EthereumAccount<AccountId = String> + Clone + Send + Sync + 'static,
{
    pub fn spawn(&self, freq: std::time::Duration) {
        let engine_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(freq);
            loop {
                interval.tick().await;
                // ignore the return value
                let _ = engine_clone.handle_received_transactions().await;
            }
        });
    }
}

impl<'a, S, A> EthereumLedgerSettlementEngine<'a, S, A>
where
    S: EthereumStore<Account = A>
        + LeftoversStore<AccountId = String, AssetType = BigUint>
        + Clone
        + Send
        + Sync,
    A: EthereumAccount<AccountId = String> + Clone + Send + Sync,
{
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
        // get the current block number
        let current_block = self
            .eth_client
            .get_block_number()
            .map_err(move |err| error!("Could not fetch current block number {:?}", err))
            .await?;

        // get the safe number of blocks to avoid reorgs
        let to_block = current_block - self.confirmations;

        // If we are just starting up, fetch only the most recent block
        // Note this means we will ignore transactions that were received before
        // the first time the settlement engine was started.
        let last_observed_block = self
            .store
            .load_recently_observed_block(self.chain_id.to_string())
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
        let (token, incoming_transfers) = self
            .eth_client
            .check_incoming(from_block, to_block)
            .await
            .map_err(|err| error!("could not get incoming txs: {}", err))?;

        for transfer in &incoming_transfers {
            self.notify_incoming(token, transfer).await?;
        }

        trace!("Processed all transactions up to block {}", to_block);
        // now that all transactions have been processed successfully, we
        // can save `to_block` as the latest observed block
        self.store
            .save_recently_observed_block(self.chain_id.to_string(), to_block)
            .await
    }

    /// Notifies the connector about an Ethereum transaction. Requires parsing the transaction directly
    pub async fn notify_incoming(
        &self,
        token_address: Option<Address>,
        transfer: &IncomingTransfer,
    ) -> Result<(), ()> {
        let tx_hash = transfer.hash;
        let is_tx_processed = self
            .store
            .check_if_tx_processed(tx_hash)
            .map_err(move |_| error!("Error when querying store about transaction: {:?}", tx_hash))
            .await?;

        // This is a no-op if the operation was already processed
        if !is_tx_processed {
            trace!(
                "Got transaction for our account from {} for amount {}",
                transfer.from,
                transfer.amount
            );

            // if the tx was for us and had some non-0 amount, then let
            // the connector know
            let addr = Addresses {
                own_address: transfer.from,
                token_address,
            };

            let account_id = self.store.load_account_id_from_address(addr).await?;
            self.notify_connector(account_id.to_string(), transfer.amount.to_string(), tx_hash)
                .await?;
            self.store.mark_tx_processed(tx_hash).await?;
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
            let client = RequestClient::new();
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

    /// Helper function that returns the addresses associated with an
    /// account from a given string account id
    pub async fn load_account(&self, account_id: String) -> Result<(String, Addresses), String> {
        let store = self.store.clone();
        let addr = self.eth_client.address();
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
impl<'a, S, A> SettlementEngine for EthereumLedgerSettlementEngine<'a, S, A>
where
    S: EthereumStore<Account = A>
        + LeftoversStore<AccountId = String, AssetType = BigUint>
        + Clone
        + Send
        + Sync,
    A: EthereumAccount<AccountId = String> + Clone + Send + Sync,
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
        // We make a POST request to OUR connector's `messages`
        // endpoint. This will in turn send an outgoing
        // request to its peer connector, which will ask its
        // own engine about its settlement information. Then,
        // we store that information and use it when
        // performing settlements.
        let idempotency_uuid = Uuid::new_v4().to_hyphenated().to_string();
        let challenge = Uuid::new_v4().to_hyphenated().to_string().into_bytes();
        let client = RequestClient::new();
        let mut url = self.connector_url.clone();
        let address = Addresses {
            own_address: self.eth_client.address(),
            token_address: self.eth_client.token.as_ref().map(|token| token.address()),
        };

        // send a payment details request (we send them a challenge)
        url.path_segments_mut()
            .expect("Invalid connector URL")
            .push("accounts")
            .push(&account_id.to_string())
            .push("messages");
        let body = serde_json::to_string(&PaymentDetailsRequest::new(challenge.clone())).unwrap();
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

        let data = prefixed_message(&challenge);
        let recovered_address = payment_details.signature.recover(data);
        trace!("Received payment details {:?}", payment_details);
        match recovered_address {
            Ok(recovered_address) => {
                if recovered_address != payment_details.to.own_address {
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
            let data = prefixed_message(&challenge);
            let signature = self.eth_client.signer_unchecked().sign_message(&data);
            let resp = {
                // Respond with our address, a signature,
                // and no challenge, since we already sent
                // them one earlier
                let ret = PaymentDetailsResponse::new(address, signature, None);
                serde_json::to_string(&ret).unwrap()
            };
            let idempotency_uuid = Uuid::new_v4().to_hyphenated().to_string();
            let client = RequestClient::new();
            let action = move || {
                client
                    .post(url_clone.as_ref())
                    .header("Content-Type", "application/octet-stream")
                    .header("Idempotency-Key", idempotency_uuid.clone())
                    .body(resp.clone())
                    .send()
                    .map_err(|err| error!("{}", err))
                    .compat()
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
        let store = self.store.clone();
        let address = Addresses {
            own_address: self.eth_client.address(),
            token_address: self.eth_client.token.as_ref().map(|token| token.address()),
        };
        // We are only returning our information, so
        // there is no need to return any data about the
        // provided account.
        // If we received a SYN, we respond with a signed message
        if let Ok(req) = serde_json::from_slice::<PaymentDetailsRequest>(&body) {
            // Otherwise, we save the received address
            let data = prefixed_message(&req.challenge);
            let signature = self.eth_client.signer_unchecked().sign_message(&data);
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
            let data = prefixed_message(challenge);
            match resp.signature.recover(data) {
                Ok(recovered_address) => {
                    if recovered_address == resp.to.own_address {
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

        // Typecast to ethers::U256
        let total_amount = amount + uncredited_settlement_amount;
        let total_amount = match U256::from_dec_str(&total_amount.to_string()) {
            Ok(a) => a,
            Err(_err) => {
                let error_msg = format!("Error converting to U256 {:?}", _err);
                error!("{:?}", error_msg);
                return Err(ApiError::from_api_error_type(&CONVERSION_ERROR_TYPE).detail(error_msg));
            }
        };

        self.eth_client
            .transfer(addresses.own_address, total_amount)
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

fn prefixed_message(challenge: &[u8]) -> Vec<u8> {
    let mut ret = ETH_CREATE_ACCOUNT_PREFIX.to_vec();
    ret.extend(challenge);
    ret
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_helpers::{
        fixtures::{ALICE, BOB, MESSAGES_API},
        test_engine, test_store, TestAccount,
    };

    use crate::ethereum::HttpClient;
    use once_cell::sync::Lazy;
    use std::convert::TryFrom;

    pub static ALICE_PK: Lazy<HttpClient> = Lazy::new(|| {
        let wallet = "380eb0f3d505f087e438eca80bc4df9a7faa24f868e69fc0440261a0fc0567dc"
            .parse::<Wallet>()
            .unwrap();
        let provider = Provider::<Http>::try_from("http://localhost:8545").unwrap();
        wallet.connect(provider)
    });

    pub static BOB_PK: Lazy<HttpClient> = Lazy::new(|| {
        let wallet = "cc96601bc52293b53c4736a12af9130abf347669b3813f9ec4cafdf6991b087e"
            .parse::<Wallet>()
            .unwrap();
        let provider = Provider::<Http>::try_from("http://localhost:8545").unwrap();
        wallet.connect(provider)
    });

    #[tokio::test]
    async fn test_create_get_account() {
        let bob: TestAccount = BOB.clone();

        let challenge = Uuid::new_v4().to_hyphenated().to_string().into_bytes();
        let signature = BOB_PK.sign_message(challenge).await.unwrap();

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
        let engine = test_engine(store.clone(), &ALICE_PK, 0, &connector_url, None, false).await;

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
        let signed_challenge = prefixed_message(&challenge);

        let signature = ALICE_PK.sign_message(signed_challenge).await.unwrap();

        let store = test_store(ALICE.clone(), false, false, false);
        let engine = test_engine(
            store.clone(),
            &ALICE_PK,
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
        let signed_challenge = prefixed_message(&challenge);
        let signature = BOB_PK.sign_message(signed_challenge).await.unwrap();
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
