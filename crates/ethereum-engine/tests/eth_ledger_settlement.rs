#![type_length_limit = "6000000"]
#![recursion_limit = "128"]
#![allow(unused_imports)]

use env_logger;
use futures::future::join_all;
use futures::Future;
use ilp_node::InterledgerNode;
use interledger::{api::AccountDetails, packet::Address, service::Username};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{self, json};
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::runtime::Builder as RuntimeBuilder;

mod test_helpers;
use test_helpers::{
    accounts_to_ids, create_account_on_engine, create_account_on_node, get_all_accounts,
    get_balance, random_secret, send_money_to_username, start_ganache, BalanceData,
};

#[cfg(feature = "redis")]
use test_helpers::{redis_helpers::*, start_eth_engine};

/// In this test we have Alice and Bob who have peered with each other and run
/// Ethereum ledger settlement engines. Alice proceeds to make SPSP payments to
/// Bob, until she eventually reaches Bob's `settle_threshold`. Once that's
/// exceeded, her engine makes a settlement request to Bob. Alice's connector
/// immediately applies the balance change. Bob's engine listens for incoming
/// transactions, and once the transaction has sufficient confirmations it
/// lets Bob's connector know about it, so that it adjusts their credit.
#[cfg(feature = "redis")]
#[tokio::test]
async fn eth_ledger_settlement() {
    // Nodes 1 and 2 are peers, Node 2 is the parent of Node 3
    let _ = env_logger::try_init();
    let context = TestContext::new();

    let mut ganache_pid = start_ganache();

    // Each node will use its own DB within the redis instance
    let mut connection_info1 = context.get_client_connection_info();
    connection_info1.db = 1;
    let mut connection_info2 = context.get_client_connection_info();
    connection_info2.db = 2;

    let node1_http = get_open_port(Some(3010));
    let node1_settlement = get_open_port(Some(3011));
    let node1_engine = get_open_port(Some(3012));
    let node1_engine_address = SocketAddr::from(([127, 0, 0, 1], node1_engine));
    let alice_key = "380eb0f3d505f087e438eca80bc4df9a7faa24f868e69fc0440261a0fc0567dc".to_string();
    let node2_http = get_open_port(Some(3020));
    let node2_settlement = get_open_port(Some(3021));
    let node2_engine = get_open_port(Some(3022));
    let node2_engine_address = SocketAddr::from(([127, 0, 0, 1], node2_engine));
    let bob_key = "cc96601bc52293b53c4736a12af9130abf347669b3813f9ec4cafdf6991b087e".to_string();

    let node1: InterledgerNode = serde_json::from_value(json!({
        "ilp_address": "example.alice",
        "admin_auth_token": "admin",
        "database_url": connection_info_to_string(connection_info1.clone()),
        "http_bind_address": format!("127.0.0.1:{}", node1_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node1_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
    }))
    .unwrap();

    let node2: InterledgerNode = serde_json::from_value(json!({
        "admin_auth_token": "admin",
        "database_url": connection_info_to_string(connection_info2.clone()),
        "http_bind_address": format!("127.0.0.1:{}", node2_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node2_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
    }))
    .unwrap();

    let alice_on_alice = json!({
        "ilp_address": "example.alice",
        "username": "alice",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_incoming_token" : "in_alice",
        "settle_to": -10,
        "routing_relation": "NonRoutingAccount",
    });

    let bob_on_alice = json!({
        "username": "bob",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_incoming_token" : "bob_password",
        "ilp_over_http_url": format!("http://localhost:{}/accounts/alice/ilp", node2_http),
        "ilp_over_http_outgoing_token" : "alice_password",
        "min_balance": -100,
        "settle_threshold": 70,
        "settle_to": 10,
        "settlement_engine_url": format!("http://localhost:{}", node1_engine),
        "routing_relation": "Child",
    });

    start_eth_engine(
        connection_info1,
        node1_engine_address,
        alice_key,
        node1_settlement,
    )
    .await
    .unwrap();
    node1.serve().await.unwrap();
    create_account_on_node(node1_http, alice_on_alice, "admin")
        .await
        .unwrap();
    create_account_on_node(node1_http, bob_on_alice, "admin")
        .await
        .unwrap();

    let bob_on_bob = json!({
        "username": "bob",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_incoming_token" : "in_bob",
        "settle_to": -10,
        "routing_relation": "NonRoutingAccount",
    });

    let alice_on_bob = json!({
        "ilp_address": "example.alice",
        "username": "alice",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_incoming_token" : "alice_password",
        "ilp_over_http_url": format!("http://localhost:{}/accounts/bob/ilp", node1_http),
        "ilp_over_http_outgoing_token" : "bob_password",
        "min_balance": -100,
        "settle_threshold": 70,
        "settle_to": -10,
        "settlement_engine_url": format!("http://localhost:{}", node2_engine),
        "routing_relation": "Parent",
    });

    start_eth_engine(
        connection_info2,
        node2_engine_address,
        bob_key,
        node2_settlement,
    )
    .await
    .unwrap();
    node2.serve().await.unwrap();
    create_account_on_node(node2_http, bob_on_bob, "admin")
        .await
        .unwrap();
    create_account_on_node(node2_http, alice_on_bob, "admin")
        .await
        .unwrap();

    // Make 4 subsequent payments (we could also do a 71 payment
    // directly)
    let send1 = send_money_to_username(node1_http, node2_http, 10, "bob", "alice", "in_alice");
    let send2 = send_money_to_username(node1_http, node2_http, 20, "bob", "alice", "in_alice");
    let send3 = send_money_to_username(node1_http, node2_http, 39, "bob", "alice", "in_alice");
    let send4 = send_money_to_username(node1_http, node2_http, 1, "bob", "alice", "in_alice");

    let get_balances = move || {
        join_all(vec![
            get_balance("bob", node1_http, "bob_password"),
            get_balance("alice", node2_http, "alice_password"),
        ])
    };

    send1.await.unwrap();
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: 10e-9
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: -10e-9
        }
    );

    send2.await.unwrap();
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: 30e-9
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: -30e-9
        }
    );
    send3.await.unwrap();
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: 69e-9
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: -69e-9
        }
    );
    // Up to here, Alice's balance should be -69 and Bob's
    // balance should be 69. Once we make 1 more payment, we
    // exceed the settle_threshold and thus a settlement is made
    send4.await.unwrap();
    // Wait a few seconds so that the receiver's engine
    // gets the data
    delay(5000).await;
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    // Since the credit connection reached -70, and the
    // settle_to is -10, a 60 Wei transaction is made.
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: 10e-9
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: -10e-9
        }
    );
    ganache_pid.kill().unwrap();
}
