use super::utils::*;
use mockito;
use num_bigint::BigUint;
use serde_json::json;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::Duration;

use ilp_settlement_ethereum::{
    ethereum::{ERC20Client, ERC20TOKEN_ABI},
    utils::types::{Addresses, EthereumStore},
};
use interledger_settlement::core::types::{
    ApiResponse, LeftoversStore, Quantity, SettlementEngine,
};

#[path = "../test_helpers/mod.rs"]
mod test_helpers;
use test_helpers::{ALICE, BOB};

use ethers::{prelude::*, utils::GanacheBuilder};


pub static ALICE_ACC: Lazy<TestAccount> = Lazy::new(|| TestAccount::new(
    "1".to_string(),
    "3cdb3d9e1b74692bb1e3bb5fc81938151ca64b02",
    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
));

pub static BOB_ACC: Lazy<TestAccount> = Lazy::new(|| TestAccount::new(
    "0".to_string(),
    "9b925641c5ef3fd86f63bff2da55a0deeafd1263",
    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
));

#[tokio::test]
async fn test_send_erc20() {
    let _ganache = GanacheBuilder::new().mnemonic(
        "abstract vacuum mammal awkward pudding scene penalty purchase dinner depart evoke puzzle"
    ).spawn();

    let alice = ALICE.clone();
    let bob = BOB.clone();

    use rustc_hex::FromHex;
    let bytecode = include_str!("./fixtures/erc20.code");
    let bytecode = bytecode.from_hex::<Vec<u8>>().unwrap().into();
    let factory = ContractFactory::new(&ALICE, &ERC20TOKEN_ABI, &bytecode);
    let supply = U256::from_dec_str("1000000000000000000000").unwrap();
    let contract = factory.deploy(supply).unwrap().send().await.unwrap();
    let token_address = contract.address();

    let contract = ERC20Client::new(token_address, &ALICE);

    let alice_store = test_store(ALICE_ACC.clone(), false, false, true);
    alice_store
        .save_account_addresses(HashMap::from_iter(vec![(
            "0".to_string(),
            Addresses {
                own_address: bob.address(),
                token_address: Some(token_address),
            },
        )]))
        .await
        .unwrap();

    let bob_store = test_store(BOB_ACC.clone(), false, false, true);
    bob_store
        .save_account_addresses(HashMap::from_iter(vec![(
            "42".to_string(),
            Addresses {
                own_address: alice.address(),
                token_address: Some(token_address),
            },
        )]))
        .await
        .unwrap();

    let bob_mock = mockito::mock("POST", "/accounts/42/settlements")
        .match_body(mockito::Matcher::JsonString(
            json!(Quantity::new(100_000_000_000u64, 18)).to_string(),
        ))
        .with_status(200)
        .with_body(json!(Quantity::new(100, 9)).to_string())
        .create();

    let bob_connector_url = mockito::server_url();
    let _bob_engine = test_engine(
        bob_store.clone(),
        &BOB,
        0,
        &bob_connector_url,
        Some(token_address),
        true,
    )
    .await;

    let alice_engine = test_engine(
        alice_store.clone(),
        &ALICE,
        0,
        "http://127.0.0.1:9999",
        Some(token_address),
        false, // alice sends the transaction to bob (set it up so that she doesn't listen for inc txs)
    )
    .await;

    // 100 Gwei
    let ret = alice_engine
        .send_money(BOB_ACC.id.to_string(), Quantity::new(100u64, 9))
        .await
        .unwrap();
    if let ApiResponse::Data(_) = ret {
        panic!("expected empty default ret type for send money")
    }

    // wait a few seconds so that the receiver's engine that does the polling
    tokio::time::delay_for(Duration::from_secs(3)).await;

    let alice_balance = contract.balance_of(alice.address()).call().await.unwrap();
    let bob_balance = contract.balance_of(bob.address()).call().await.unwrap();
    assert_eq!(
        alice_balance,
        U256::from_dec_str("999999999900000000000").unwrap()
    );
    assert_eq!(bob_balance, U256::from_dec_str("100000000000").unwrap()); // 100 + 9 0's for the Gwei conversion

    bob_mock.assert();
}

#[tokio::test]
async fn test_send_eth() {
    let alice = ALICE.clone();
    let bob = BOB.clone();

    let alice_store = test_store(ALICE_ACC.clone(), false, false, true);
    alice_store
        .save_account_addresses(HashMap::from_iter(vec![(
            "0".to_string(),
            Addresses {
                own_address: bob.address(),
                token_address: None,
            },
        )]))
        .await
        .unwrap();

    let bob_store = test_store(BOB_ACC.clone(), false, false, true);
    bob_store
        .save_account_addresses(HashMap::from_iter(vec![(
            "42".to_string(),
            Addresses {
                own_address: alice.address(),
                token_address: None,
            },
        )]))
        .await
        .unwrap();

       let _ganache = GanacheBuilder::new().mnemonic(
           "abstract vacuum mammal awkward pudding scene penalty purchase dinner depart evoke puzzle"
       ).spawn();

    let bob_mock = mockito::mock("POST", "/accounts/42/settlements")
        .match_body(mockito::Matcher::JsonString(
            json!(Quantity::new(100_000_000_001u64, 18)).to_string(),
        ))
        .with_status(200)
        .with_body(json!(Quantity::new(100, 9)).to_string())
        .create();

    let bob_connector_url = mockito::server_url();
    let _bob_engine = test_engine(
        bob_store.clone(),
        &BOB,
        0,
        &bob_connector_url,
        None,
        true,
    )
    .await;

    let alice_engine = test_engine(
        alice_store.clone(),
        &ALICE,
        0,
        "http://127.0.0.1:9999",
        None,
        false, // alice sends the transaction to bob (set it up so that she doesn't listen for inc txs)
    )
    .await;

    // Connector sends an amount that's smaller than what the engine can
    // process, leftovers must be stored
    let ret = alice_engine
        .send_money(BOB_ACC.id.to_string(), Quantity::new(9, 19))
        .await
        .unwrap();
    if let ApiResponse::Data(_) = ret {
        panic!("expected empty default ret type for send money")
    }

    // The leftovers must be set
    assert_eq!(
        alice_store
            .get_uncredited_settlement_amount(BOB_ACC.id.to_string())
            .await
            .unwrap(),
        (BigUint::from(9u32), 19)
    );

    // the connector sends one more request, still less than the minimum amount,
    // but this puts the leftovers over the min amount for the next call
    let ret = alice_engine
        .send_money(BOB_ACC.id.to_string(), Quantity::new(11, 20))
        .await
        .unwrap();
    if let ApiResponse::Data(_) = ret {
        panic!("expected empty default ret type for send money")
    }

    // The leftovers must be set
    assert_eq!(
        alice_store
            .get_uncredited_settlement_amount(BOB_ACC.id.to_string())
            .await
            .unwrap(),
        (BigUint::from(101u32), 20)
    );

    let ret = alice_engine
        .send_money(BOB_ACC.id.to_string(), Quantity::new(100u64, 9))
        .await
        .unwrap();
    if let ApiResponse::Data(_) = ret {
        panic!("expected empty default ret type for send money")
    }

    // the remaining leftovers are correctly set
    assert_eq!(
        alice_store
            .get_uncredited_settlement_amount(BOB_ACC.id.to_string())
            .await
            .unwrap(),
        (BigUint::from(1u32), 20)
    );

    tokio::time::delay_for(Duration::from_secs(2)).await;

    use std::convert::TryFrom;
    let provider = Provider::<Http>::try_from("http://localhost:8545").unwrap();
    let alice_balance = provider.get_balance(alice.address(), None).await.unwrap();
    let bob_balance = provider.get_balance(bob.address(), None).await.unwrap();
    let expected_alice = U256::from_dec_str("99998739899999999999").unwrap(); // 99ether - 21k gas - 100 gwei - 1 wei (only 1 tranasaction was made, despite the 2 zero-value settlement requests)
    let expected_bob = U256::from_dec_str("100000000100000000001").unwrap(); // 100 ether + 100 gwei + 1 wei
    assert_eq!(alice_balance, expected_alice);
    assert_eq!(bob_balance, expected_bob);

    bob_mock.assert();
}
