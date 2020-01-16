#![type_length_limit = "6000000"]
#![recursion_limit = "128"]

use futures::future::join_all;
use ilp_node::InterledgerNode;
use serde_json::{self, json};
use std::time::Duration;

mod test_helpers;
use test_helpers::{
    create_account_on_node, get_balance, random_secret, send_money_to_username, start_xrp_engine,
    BalanceData,
};

#[cfg(feature = "redis")]
use test_helpers::redis_helpers::*;

#[cfg(feature = "redis")]
#[tokio::test]
/// In this test we have Alice and Bob who have peered with each other and run
/// XRP ledger settlement engines. Alice proceeds to make SPSP payments to
/// Bob, until she eventually reaches Bob's `settle_threshold`. Once that's
/// exceeded, her engine makes a settlement request to Bob. Alice's connector
/// immediately applies the balance change. Bob's engine listens for incoming
/// transactions, and once the transaction has sufficient confirmations it
/// lets Bob's connector know about it, so that it adjusts their credit.
async fn xrp_ledger_settlement() {
    // Nodes 1 and 2 are peers, Node 2 is the parent of Node 3
    let _ = env_logger::try_init();
    let context = TestContext::new();

    // Each node will use its own DB within the redis instance
    let mut connection_info1 = context.get_client_connection_info();
    connection_info1.db = 1;
    let mut connection_info2 = context.get_client_connection_info();
    connection_info2.db = 2;

    let node1_http = get_open_port(Some(3010));
    let node1_settlement = get_open_port(Some(3011));
    let node1_engine = get_open_port(Some(3012));

    let node2_http = get_open_port(Some(3020));
    let node2_settlement = get_open_port(Some(3021));
    let node2_engine = get_open_port(Some(3022));

    // spawn 2 redis servers for the XRP engines
    let alice_redis_port = get_open_port(Some(6379));
    let bob_redis_port = get_open_port(Some(6380));
    let mut alice_engine_redis = RedisServer::spawn_with_port(alice_redis_port);
    let mut bob_engine_redis = RedisServer::spawn_with_port(bob_redis_port);
    let mut engine_alice =
        start_xrp_engine("http://localhost:3011", alice_redis_port, node1_engine);
    let mut engine_bob = start_xrp_engine("http://localhost:3021", bob_redis_port, node2_engine);
    std::thread::sleep(std::time::Duration::from_secs(10));

    let node1: InterledgerNode = serde_json::from_value(json!({
        "ilp_address": "example.alice",
        "admin_auth_token": "admin",
        "database_url": connection_info_to_string(connection_info1),
        "http_bind_address": format!("127.0.0.1:{}", node1_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node1_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
    }))
    .unwrap();

    let node2: InterledgerNode = serde_json::from_value(json!({
        "ilp_address": "example.bob",
        "admin_auth_token": "admin",
        "database_url": connection_info_to_string(connection_info2),
        "http_bind_address": format!("127.0.0.1:{}", node2_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node2_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
    }))
    .unwrap();

    let alice_on_alice = json!({
        "ilp_address": "example.alice",
        "username": "alice",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "in_alice",
        "settle_to": -10,
    });

    let bob_on_alice = json!({
        "ilp_address": "example.bob",
        "username": "bob",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "bob_password",
        "ilp_over_http_url": format!("http://localhost:{}/accounts/alice/ilp", node2_http),
        "ilp_over_http_outgoing_token" : "alice_password",
        "min_balance": -100,
        "settle_threshold": 70,
        "settle_to": 10,
        "settlement_engine_url": format!("http://localhost:{}", node1_engine),
    });

    node1.serve().await.unwrap();
    create_account_on_node(node1_http, alice_on_alice, "admin")
        .await
        .unwrap();
    create_account_on_node(node1_http, bob_on_alice, "admin")
        .await
        .unwrap();

    let bob_on_bob = json!({
        "ilp_address": "example.bob",
        "username": "bob",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "in_bob",
    });

    let alice_on_bob = json!({
        "ilp_address": "example.alice",
        "username": "alice",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "alice_password",
        "ilp_over_http_url": format!("http://localhost:{}/accounts/bob/ilp", node1_http),
        "ilp_over_http_outgoing_token" : "bob_password",
        "min_balance": -100,
        "settle_threshold": 70,
        "settle_to": -10,
        "settlement_engine_url": format!("http://localhost:{}", node2_engine),
    });

    node2.serve().await.unwrap();
    create_account_on_node(node2_http, bob_on_bob, "admin")
        .await
        .unwrap();
    create_account_on_node(node2_http, alice_on_bob, "admin")
        .await
        .unwrap();

    let get_balances = move || {
        join_all(vec![
            get_balance("bob", node1_http, "bob_password"),
            get_balance("alice", node2_http, "alice_password"),
        ])
    };

    // first spend
    send_money_to_username(node1_http, node2_http, 10, "bob", "alice", "in_alice")
        .await
        .unwrap();
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: 10e-6
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: -10e-6
        }
    );

    // second psend
    send_money_to_username(node1_http, node2_http, 20, "bob", "alice", "in_alice")
        .await
        .unwrap();
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: 30e-6
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: -30e-6
        }
    );

    // 3rd spend, still below settlement limit
    send_money_to_username(node1_http, node2_http, 39, "bob", "alice", "in_alice")
        .await
        .unwrap();
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: 69e-6
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: -69e-6
        }
    );

    // Up to here, Alice's balance should be -69 and Bob's
    // balance should be 69. Once we make 1 more payment, we
    // exceed the settle_threshold and thus a settlement is made
    send_money_to_username(node1_http, node2_http, 1, "bob", "alice", "in_alice")
        .await
        .unwrap();
    // Wait a few seconds so that the receiver's engine
    // gets the data and applies it (longer than the
    // Ethereum engine since we're using a public
    // testnet here)
    tokio::time::delay_for(Duration::from_secs(10)).await;
    // Since the credit connection reached -70, and the
    // settle_to is -10, a 60 Wei transaction is made.
    let ret = get_balances().await;
    let ret: Vec<_> = ret.into_iter().map(|r| r.unwrap()).collect();
    assert_eq!(
        ret[0],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: 10e-6
        }
    );
    assert_eq!(
        ret[1],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: -10e-6
        }
    );
    engine_alice.kill().unwrap();
    alice_engine_redis.kill().unwrap();
    engine_bob.kill().unwrap();
    bob_engine_redis.kill().unwrap();
}
