#![type_length_limit = "6000000"]
#![recursion_limit = "128"]
#![allow(unused_imports)]

use env_logger;
use futures::{future::join_all, Future};
use ilp_node::InterledgerNode;
use interledger::{api::AccountSettings, packet::Address, service::Username};
use serde_json::json;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::runtime::Builder as RuntimeBuilder;

mod test_helpers;
use test_helpers::{
    accounts_to_ids, create_account_on_node, get_all_accounts, get_balance, random_secret,
    send_money_to_username, set_node_settlement_engines, start_ganache, start_xrp_engine,
    BalanceData, ALICE, BOB,
};

use ilp_settlement_ethereum::ethereum::EthClient;

#[cfg(feature = "redis")]
use test_helpers::{redis_helpers::*, start_eth_engine};

#[cfg(feature = "redis")]
#[tokio::test]
async fn eth_xrp_interoperable() {
    // Nodes 1 and 2 are peers, Node 2 is the parent of Node 3
    let _ = env_logger::try_init();
    let context = TestContext::new();

    let mut ganache_pid = start_ganache();

    // Each node will use its own DB within the redis instance
    let mut connection_info1 = context.get_client_connection_info();
    connection_info1.db = 1;
    let mut connection_info2 = context.get_client_connection_info();
    connection_info2.db = 2;
    let mut connection_info3 = context.get_client_connection_info();
    connection_info3.db = 3;

    let node1_http = get_open_port(Some(3010));
    let node1_settlement = get_open_port(Some(3011));
    let node1_engine = get_open_port(Some(3012));
    let node1_engine_address = SocketAddr::from(([127, 0, 0, 1], node1_engine));

    let node2_http = get_open_port(Some(3020));
    let node2_settlement = get_open_port(Some(3021));
    let node2_engine = get_open_port(Some(3022));
    let node2_engine_address = SocketAddr::from(([127, 0, 0, 1], node2_engine));
    let node2_xrp_engine_port = get_open_port(Some(3023));

    let node3_http = get_open_port(Some(3030));
    let node3_settlement = get_open_port(Some(3031));
    let _node3_engine = get_open_port(Some(3032)); // unused engine
    let node3_xrp_engine_port = get_open_port(Some(3033));

    // spawn 2 redis servers for the XRP engines
    let node2_redis_port = get_open_port(Some(6380));
    let node3_redis_port = get_open_port(Some(6381));
    let mut node2_engine_redis = RedisServer::spawn_with_port(node2_redis_port);
    let mut node3_engine_redis = RedisServer::spawn_with_port(node3_redis_port);
    let mut node2_xrp_engine = start_xrp_engine(
        &format!("http://localhost:{}", node2_settlement),
        node2_redis_port,
        node2_xrp_engine_port,
    );
    let mut node3_xrp_engine = start_xrp_engine(
        &format!("http://localhost:{}", node3_settlement),
        node3_redis_port,
        node3_xrp_engine_port,
    );
    std::thread::sleep(std::time::Duration::from_secs(15));

    let node1_eth_key = EthClient::new(None, &ALICE);
    let node2_eth_key = EthClient::new(None, &BOB);

    start_eth_engine(
        connection_info1.clone(),
        node1_engine_address,
        node1_eth_key,
        node1_settlement,
    )
    .await
    .unwrap();
    start_eth_engine(
        connection_info2.clone(),
        node2_engine_address,
        node2_eth_key,
        node2_settlement,
    )
    .await
    .unwrap();

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
    let alice_on_alice = json!({
        "ilp_address": "example.alice",
        "username": "alice",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_url": format!("http://localhost:{}/ilp", node1_http),
        "ilp_over_http_incoming_token" : "in_alice",
    });

    let bob_on_alice = json!({
        "ilp_address": "example.bob",
        "username": "bob",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_url": format!("http://localhost:{}/accounts/alice/ilp", node2_http),
        "ilp_over_http_incoming_token" : "bob_password",
        "ilp_over_http_outgoing_token" : "alice_password",
        "min_balance": -1_000_000_000,
        "settle_threshold": 70000,
        "settle_to": 10000,
        "settlement_engine_url": format!("http://localhost:{}", node1_engine),
        "routing_relation": "Peer",
    });

    node1.serve().await.unwrap();
    create_account_on_node(node1_http, alice_on_alice, "admin")
        .await
        .unwrap();
    create_account_on_node(node1_http, bob_on_alice, "admin")
        .await
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

    // Instead of using settlement engines configured for each account,
    // Bob uses globally-configured settlement engines for each currency
    let bob_settlement_engines = json!({
        "ETH": format!("http://localhost:{}", node2_engine),
        "XRP": format!("http://localhost:{}", node2_xrp_engine_port),
    });
    let alice_on_bob = json!({
        "ilp_address": "example.alice",
        "username": "alice",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_url": format!("http://localhost:{}/accounts/bob/ilp", node1_http),
        "ilp_over_http_incoming_token" : "alice_password",
        "ilp_over_http_outgoing_token" : "bob_password",
        "min_balance": -100_000,
        "routing_relation": "Peer",
    });
    let charlie_on_bob = json!({
        "username": "charlie",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "charlie_password",
        "ilp_over_http_outgoing_token": "bob_password",
        "ilp_over_http_url": format!("http://localhost:{}/accounts/bob/ilp", node3_http),
        "min_balance": -100,
        "settle_threshold": 70000,
        "settle_to": 5000,
        "routing_relation": "Child",
    });

    node2.serve().await.unwrap();
    create_account_on_node(node2_http, alice_on_bob, "admin")
        .await
        .unwrap();
    create_account_on_node(node2_http, charlie_on_bob, "admin")
        .await
        .unwrap();
    // Setting the settlement engines after the accounts are created should
    // still trigger the call to create their accounts on the settlement engines
    set_node_settlement_engines(node2_http, bob_settlement_engines, "admin")
        .await
        .unwrap();

    let charlie_on_charlie = json!({
        "username": "charlie",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "in_charlie",
        "ilp_over_http_url": format!("http://localhost:{}/ilp", node3_http),
    });
    let bob_on_charlie = json!({
        "ilp_address": "example.bob",
        "username": "bob",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "bob_password",
        "ilp_over_http_outgoing_token": "charlie_password",
        "ilp_over_http_url": format!("http://localhost:{}/accounts/charlie/ilp", node2_http),
        "min_balance": -100_000,
        "routing_relation": "Parent",
        "settlement_engine_url": format!("http://localhost:{}", node3_xrp_engine_port),
    });

    let node3: InterledgerNode = serde_json::from_value(json!({
        "admin_auth_token": "admin",
        "database_url": connection_info_to_string(connection_info3),
        "http_bind_address": format!("127.0.0.1:{}", node3_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node3_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
    }))
    .unwrap();

    node3.serve().await.unwrap();
    create_account_on_node(node3_http, bob_on_charlie, "admin")
        .await
        .unwrap();
    create_account_on_node(node3_http, charlie_on_charlie, "admin")
        .await
        .unwrap();

    // Set the exchange rates
    let client = reqwest::Client::new();
    client
        .put(&format!("http://localhost:{}/rates", node2_http))
        .header("Authorization", "Bearer admin")
        // 1 XRP = $1, 1 ETH = $1000
        .json(&json!({"XRP": 1, "ETH": 1000}))
        .send()
        .await
        .unwrap();

    send_money_to_username(
        node1_http, node3_http, 69000, "charlie", "alice", "in_alice",
    )
    .await
    .unwrap();
    // Pay 1k Gwei --> 1 drop
    // This will trigger a 60 Gwei settlement from Alice to Bob.
    send_money_to_username(node1_http, node3_http, 1000, "charlie", "alice", "in_alice")
        .await
        .unwrap();

    // wait for the settlements
    delay(10000).await;

    // Get the balances
    let balances = futures::future::join_all(vec![
        get_balance("alice", node1_http, "in_alice"),
        get_balance("bob", node1_http, "bob_password"),
        get_balance("alice", node2_http, "alice_password"),
        get_balance("charlie", node2_http, "charlie_password"),
        get_balance("charlie", node3_http, "in_charlie"),
        get_balance("bob", node3_http, "bob_password"),
    ])
    .await;
    let balances: Vec<_> = balances.into_iter().map(|b| b.unwrap()).collect();

    assert_eq!(
        balances[0],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: -70000e-9
        }
    );
    // Since Alice has configured Bob's
    // `settle_threshold` and `settle_to` to be
    // 70k and 10k respectively, once she
    // exceeded the 70k threshold, she made a 60k
    // Gwei settlement to Bob so that their debt
    // settles down to 10k.
    // From her perspective, Bob's account has a
    // positive 10k balance since she owes him money.
    assert_eq!(
        balances[1],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: 10000e-9
        }
    );
    // From Bob's perspective, Alice's account
    // has a negative sign since he is owed money.
    assert_eq!(
        balances[2],
        BalanceData {
            asset_code: "ETH".to_owned(),
            balance: -10000e-9
        }
    );
    // As Bob forwards money to Charlie, he also
    // eventually exceeds the `settle_threshold`
    // which incidentally is set to 70k. As a
    // result, he must make a XRP ledger
    // settlement of 65k Drops to get his debt
    // back to the `settle_to` value of charlie,
    // which is 5k (70k - 5k = 65k).
    assert_eq!(
        balances[3],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: 5000e-6
        }
    );
    // Charlie's balance indicates that he's
    // received 70k drops (the total amount Alice sent him)
    assert_eq!(
        balances[4],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: 70000e-6
        }
    );
    // And he sees is owed 5k by Bob.
    assert_eq!(
        balances[5],
        BalanceData {
            asset_code: "XRP".to_owned(),
            balance: -5000e-6
        }
    );

    node2_engine_redis.kill().unwrap();
    node3_engine_redis.kill().unwrap();
    node2_xrp_engine.kill().unwrap();
    node3_xrp_engine.kill().unwrap();
    ganache_pid.kill().unwrap();
}
