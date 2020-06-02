use ::http::StatusCode;
use interledger_errors::*;
use log::error;

use ethers::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EthError {
    #[error(transparent)]
    ClientError(#[from] ClientError),

    #[error(transparent)]
    ContractError(#[from] ContractError),
}

impl From<EthError> for ApiError {
    fn from(err: EthError) -> Self {
        error!("{}", err);
        let err_type = ApiErrorType {
            r#type: &ProblemType::Default,
            title: "Blockchain connection error",
            status: StatusCode::BAD_GATEWAY,
        };
        ApiError::from_api_error_type(&err_type).detail(err.to_string())
    }
}

pub type ERC20Client<'a> = ERC20Token<'a, Http, Wallet>;
pub type HttpClient = Client<Http, Wallet>;

// Unified HTTP client for ERC20 and ETH transfers
#[derive(Clone, Debug)]
pub struct EthClient<'a> {
    pub client: &'a HttpClient,
    pub token: Option<ERC20Client<'a>>,
}

impl<'a> std::ops::Deref for EthClient<'a> {
    type Target = &'a HttpClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<'a> EthClient<'a> {
    pub fn new(addr: Option<Address>, client: &'a HttpClient) -> Self {
        let token = addr.map(|addr| ERC20Client::new(addr, client));
        Self { client, token }
    }

    pub async fn transfer<A: Into<Address>, U: Into<U256>>(
        &self,
        to: A,
        amount: U,
    ) -> Result<H256, EthError> {
        let to = to.into();
        let amount = amount.into();

        Ok(if let Some(ref token) = self.token {
            token.transfer(to, amount).send().await?
        } else {
            let tx = TransactionRequest::new().to(to).value(amount);
            self.client.send_transaction(tx, None).await?
        })
    }

    /// Returns a vector of address/amounts which sent transactions to `client.address()`
    pub async fn check_incoming<T: Into<U64>>(
        &self,
        from_block: T,
        to_block: T,
    ) -> Result<(Option<Address>, Vec<IncomingTransfer>), EthError> {
        Ok(if let Some(ref token) = self.token {
            let transfers = token
                .transfer_filter()
                .from_block(from_block.into())
                .to_block(to_block.into())
                .topic1(self.client.address())
                .query_with_hashes()
                .await?;

            let incoming = transfers
                .into_iter()
                .map(|(tx_hash, transfer)| IncomingTransfer {
                    from: transfer.from,
                    amount: transfer.value,
                    hash: tx_hash,
                })
                .collect::<Vec<_>>();

            (Some(token.address()), incoming)
        } else {
            let mut incoming = Vec::new();
            for block_num in from_block.into().as_u64()..=to_block.into().as_u64() {
                let block = self
                    .client
                    .get_block_with_txs(block_num)
                    .await
                    // we need to MapErr this due to the Deref
                    .map_err(ClientError::ProviderError)?;

                for tx in block.transactions {
                    let hash = tx.hash;
                    if let Some((from, amount)) = sent_to_us(tx, self.client.address()) {
                        if amount.as_u64() > 0 {
                            incoming.push(IncomingTransfer { from, amount, hash });
                        }
                    }
                }
            }

            (None, incoming)
        })
    }
}

#[derive(Debug, Clone)]
pub struct IncomingTransfer {
    pub from: Address,
    pub amount: U256,
    pub hash: TxHash,
}

// TODO : Extend this function to inspect the data field of a
// transaction, so that it supports contract wallets such as the Gnosis Multisig
// etc. Implementing this would require RLP decoding the `tx.data` field of the
// transaction to get the arguments of the function called. If any of them
// indicates that it's a transaction made to our account, it should return
// the address of the contract.
// There is no need to implement any ERC20 functionality here since these
// transfers can be quickly found by filtering for the `Transfer` ERC20 event.
fn sent_to_us(tx: Transaction, our_address: Address) -> Option<(Address, U256)> {
    if let Some(to) = tx.to {
        if to == our_address {
            Some((tx.from, tx.value))
        } else {
            None
        }
    } else {
        None
    }
}

// Generate bindings for the ERC20 token
abigen!(
    ERC20Token,
    r#"[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"name","type":"string"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"symbol","type":"string"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"decimals","type":"uint8"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"value","type":"uint256"}],"name":"approve","outputs":[{"name":"success","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"totalSupply","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"from","type":"address"},{"name":"to","type":"address"},{"name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"success","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"who","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"type":"function"},{"constant":false,"inputs":[{"name":"to","type":"address"},{"name":"value","type":"uint256"}],"name":"transfer","outputs":[{"name":"success","type":"bool"}],"payable":false,"type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"remaining","type":"uint256"}],"payable":false,"type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#
);
