#![recursion_limit = "128"]
#![allow(unused_imports)]

use env_logger;
use futures::stream::Stream;
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
};
use web3::{
    transports::Http,
    types::{Address as EthAddress, TransactionRequest, U256},
    Web3,
};

#[cfg(feature = "redis")]
use test_helpers::{redis_helpers::*, start_eth_engine};

// Alice, Charlie and Charlie' (added same username to show that the system has no issue with overlapping usernames) are users of the ILP network
// They each are connected to a separate connector. The 3 connectors are connected in a Peer-Peer / Parent-Child relationship.
// As a result, Alice corresponds to example.op1.alice, Bob to example.op2.bob, and Charlie to example.op2.op3.charlie
// Alice is required to prepay from her connector in order to make any payment. She queries the new /deposit endpoint and obtains
// the connector's settlement engine's address, to which she sends 200 XRP to. She then proceeds to distribute 150 XRP to
// Charlie and 25 XRP to Bob. Bob and Charlie are then able to withdraw the funds via the new /withdrawals endpoint.
// We assume a rate of 1:10:100 for XRP:ETH:BTC for simplicity in the calculations.
// Remarks:
// 1. Op1, Op2 and Op3 are considered to be large banks. They operate in BTC and do not run any settlement system as they
// settle on a monthly basis, enforced via some legal contract.
// 2. Bob and Charlie receive ETH and are able to withdrawl real ETH, assuming Op2 and Op3's settlement engines are solvent. This
// assumes capital which must be made available by the connectors to their users.
// 3. Alice, Bob and Charlie do not need to run any connector/engine software! From their point of view, they're just performing 3 actions:
// a) top up, b) withdraw, c) transfer.
#[cfg(feature = "redis")]
#[test]
fn dex() {
    // Nodes 1 and 2 are peers, Node 2 is the parent of Node 3
    let _ = env_logger::try_init();
    let context = TestContext::new();

    let rates = move |port, token| {
        let client = reqwest::r#async::Client::new();
        client
            .put(&format!("http://localhost:{}/rates", port))
            .header("Authorization", format!("Bearer {}", token))
            .json(&json!({"BTC": 100, "ETH": 10, "XRP": 1}))
            .send()
            .map_err(|err| panic!(err))
            .and_then(|res| {
                res.error_for_status()
                    .expect("Error setting exchange rates");
                Ok(())
            })
    };

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
    // let node2_xrp_engine_port = get_open_port(Some(3023));

    let node3_http = get_open_port(Some(3030));
    let node3_settlement = get_open_port(Some(3031));
    let node3_engine = get_open_port(Some(3032));
    let node3_engine_address = SocketAddr::from(([127, 0, 0, 1], node3_engine));
    // let node3_xrp_engine_port = get_open_port(Some(3033));

    // spawn an XRP engine on Node 2, so that it's able to send settlements to the XRP account
    // and credit it for potential incoming settlements
    // let node2_redis_port = get_open_port(Some(6380));
    // let mut node2_engine_redis = RedisServer::spawn_with_port(node2_redis_port);
    // let mut node2_xrp_engine = start_xrp_engine(
    //     &format!("http://localhost:{}", node2_settlement),
    //     node2_redis_port,
    //     node2_xrp_engine_port,
    // );
    // std::thread::sleep(std::time::Duration::from_secs(15));

    let alice_addr = "0x3cdb3d9e1b74692bb1e3bb5fc81938151ca64b02";
    // let charlie_xrp_addr = "rNV1sAC7pYNGCEw9wfRvPUKrGg1G6t1QZH";
    let node3_charlie_addr = "0xafa1a814a87783c642266c37e1dfbadfb0c393e4";

    let node1_eth_key =
        "380eb0f3d505f087e438eca80bc4df9a7faa24f868e69fc0440261a0fc0567dc".to_string();
    let node2_eth_key =
        "cc96601bc52293b53c4736a12af9130abf347669b3813f9ec4cafdf6991b087e".to_string();
    let node3_eth_key =
        "6c0350691e1f5e4faaeeb6045053b3f6ff6ff9d287fd01df3c9a0aec42a1f83d".to_string();
    let node1_eth_engine_fut = start_eth_engine(
        connection_info1.clone(),
        node1_engine_address,
        node1_eth_key,
        node1_settlement,
    );
    let node2_eth_engine_fut = start_eth_engine(
        connection_info2.clone(),
        node2_engine_address,
        node2_eth_key,
        node2_settlement,
    );

    let node3_eth_engine_fut = start_eth_engine(
        connection_info3.clone(),
        node3_engine_address,
        node3_eth_key,
        node3_settlement,
    );

    let mut runtime = RuntimeBuilder::new()
        .panic_handler(|err| std::panic::resume_unwind(err))
        .build()
        .unwrap();

    let node1: InterledgerNode = serde_json::from_value(json!({
        "ilp_address": "example.op1",
        "admin_auth_token": "admin",
        "redis_connection": connection_info_to_string(connection_info1),
        "http_bind_address": format!("127.0.0.1:{}", node1_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node1_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
    }))
    .unwrap();
    let alice_on_op1 = json!({
        "username": "alice",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_incoming_token" : "in_alice",
        "routing_relation": "Child",
        "min_balance" : 0, // We do not allow Alice to go negative. She has to prepay.
        "settlement_engine_url": format!("http://localhost:{}", node1_engine),
        "settlement_extra" : alice_addr,
    });
    // the Bitcoiners use satoshis as a unit
    let op2_on_op1 = json!({
        "ilp_address": "example.op2",
        "username": "op2",
        "asset_code": "BTC",
        "asset_scale": 8,
        "ilp_over_http_url": format!("http://localhost:{}/ilp", node2_http),
        "ilp_over_http_incoming_token" : "op2_password",
        "ilp_over_http_outgoing_token" : "op1:op1_password",
        "routing_relation": "Peer",
    });

    let op1_fut = create_account_on_node(node1_http, op2_on_op1, "admin")
        .and_then(move |_| create_account_on_node(node1_http, alice_on_op1, "admin"))
        .and_then(move |_| rates(node1_http, "admin"));

    runtime.spawn(
        node1_eth_engine_fut
            .and_then(move |_| node1.serve())
            .and_then(move |_| op1_fut)
            .and_then(move |_| Ok(())),
    );

    let node2: InterledgerNode = serde_json::from_value(json!({
        "ilp_address": "example.op2",
        "admin_auth_token": "admin",
        "redis_connection": connection_info_to_string(connection_info2.clone()),
        "http_bind_address": format!("127.0.0.1:{}", node2_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node2_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
        "exchange_rate_spread": 0.01, // operator 2 takes 1% of volume as fees.
    }))
    .unwrap();

    // Instead of using settlement engines configured for each account,
    // Op2 uses globally-configured settlement engines for each currency
    let op2_settlement_engines = json!({
        "ETH": format!("http://localhost:{}", node2_engine),
        // "XRP": format!("http://localhost:{}", node2_xrp_engine_port),
    });
    let op1_on_op2 = json!({
        "ilp_address": "example.op1",
        "username": "op1",
        "asset_code": "BTC",
        "asset_scale": 8,
        "ilp_over_http_url": format!("http://localhost:{}/ilp", node1_http),
        "ilp_over_http_incoming_token" : "op1_password",
        "ilp_over_http_outgoing_token" : "op2:op2_password",
        "routing_relation": "Peer",
    });
    let op3_on_op2 = json!({
        "username": "op3",
        "asset_code": "BTC",
        "asset_scale": 8,
        "ilp_over_http_url": format!("http://localhost:{}/ilp", node3_http),
        "ilp_over_http_incoming_token" : "op3_password",
        "ilp_over_http_outgoing_token" : "op2:op2_password",
        "routing_relation": "Child",
    });
    let charlie_on_op2 = json!({
        "username": "charlie",
        "asset_code": "XRP",
        "asset_scale": 6,
        "ilp_over_http_incoming_token" : "charlie_password",
        "routing_relation": "Child",
        // "settlement_extra" : charlie_xrp_addr,
    });

    // op2 acts as a bridge node and has no child accounts
    let op2_fut = create_account_on_node(node2_http, op1_on_op2, "admin")
        .join(create_account_on_node(node2_http, op3_on_op2, "admin"))
        .join(create_account_on_node(node2_http, charlie_on_op2, "admin"))
        .join(rates(node2_http, "admin"))
        // Setting the settlement engines after the accounts are created should
        // still trigger the call to create their accounts on the settlement engines
        .and_then(move |_| {
            set_node_settlement_engines(node2_http, op2_settlement_engines, "admin")
        });

    runtime.spawn(
        node2_eth_engine_fut
            .and_then(move |_| node2.serve())
            .and_then(move |_| op2_fut)
            .and_then(move |_| Ok(())),
    );

    let node3: InterledgerNode = serde_json::from_value(json!({
        "admin_auth_token": "admin",
        "redis_connection": connection_info_to_string(connection_info3),
        "http_bind_address": format!("127.0.0.1:{}", node3_http),
        "settlement_api_bind_address": format!("127.0.0.1:{}", node3_settlement),
        "secret_seed": random_secret(),
        "route_broadcast_interval": 200,
    }))
    .unwrap();
    let op2_on_op3 = json!({
        "ilp_address": "example.op2",
        "username": "op2",
        "asset_code": "BTC",
        "asset_scale": 8,
        "ilp_over_http_url": format!("http://localhost:{}/ilp", node2_http),
        "ilp_over_http_incoming_token" : "op2_password",
        "ilp_over_http_outgoing_token" : "op3:op3_password",
        "routing_relation": "Parent",
    });
    let charlie_on_op3 = json!({
        "username": "charlie",
        "asset_code": "ETH",
        "asset_scale": 9,
        "ilp_over_http_incoming_token" : "charlie_password",
        "routing_relation": "Child",
        "settlement_engine_url": format!("http://localhost:{}", node3_engine),
        "settlement_extra" : node3_charlie_addr,
    });
    let op3_fut = create_account_on_node(node3_http, op2_on_op3, "admin")
        .and_then(move |_| delay(2000)) // give it some time to update its ilp address for its followup children
        .and_then(move |_| create_account_on_node(node3_http, charlie_on_op3, "admin"))
        .join(rates(node3_http, "admin"));
    runtime.spawn(
        node3_eth_engine_fut
            .and_then(move |_| node3.serve())
            .and_then(move |_| op3_fut)
            .and_then(move |_| Ok(())),
    );

    let get_balances = move || {
        join_all(vec![
            get_balance("alice", node1_http, "in_alice"),
            get_balance("op2", node1_http, "op2_password"),
            get_balance("charlie", node2_http, "charlie_password"),
            get_balance("op1", node2_http, "op1_password"),
            get_balance("op3", node2_http, "op3_password"),
            get_balance("op2", node3_http, "op2_password"),
            get_balance("charlie", node3_http, "charlie_password"),
        ])
    };

    // Alice tops up her balance. IRL this could be Alice being shown a QR code
    let topup = move |account, port, auth, value: String| {
        // get the address to deposit
        let client = reqwest::r#async::Client::new();
        client
            // TODO: This currently fetches it from the engine directly, make sure we can fetch it from the node as well
            .get(&format!(
                "http://localhost:{}/accounts/{}/deposit",
                port, account
            ))
            .header("Authorization", format!("Bearer {}", auth))
            .send()
            .and_then(move |res| res.into_body().concat2())
            .map_err(|err| {
                eprintln!("Error fetching info from node: {:?}", err);
            })
            .and_then(move |data| {
                let addresses: serde_json::Value = serde_json::from_slice(&data).unwrap();
                let address = match addresses.get("own_address").unwrap() {
                    serde_json::Value::String(s) => s,
                    _ => panic!("did not receive address. should never happen"),
                };
                let address = EthAddress::from_str(&address[2..]).unwrap();
                let (eloop, transport) =
                    web3::transports::Http::new("http://localhost:8545").unwrap();
                eloop.into_remote();

                delay(2000).map_err(|_| ()).and_then(move |_| {
                    // broadcast a tx
                    let web3 = web3::Web3::new(transport);
                    web3.eth()
                        .send_transaction(TransactionRequest {
                            gas_price: None,
                            value: Some(U256::from_dec_str(&value).unwrap()),
                            from: EthAddress::from_str(&alice_addr[2..]).unwrap(),
                            to: Some(address),
                            gas: None,
                            data: None,
                            nonce: None,
                            condition: None,
                        })
                        .map_err(|_| ())
                        .and_then(move |_| Ok(()))
                })
            })
    };

    let withdraw = move |port, account, auth, amount| {
        let client = reqwest::r#async::Client::new();
        client
            .post(&format!(
                "http://localhost:{}/accounts/{}/withdrawals",
                port, account,
            ))
            .json(&json!({ "amount": amount }))
            .header("Authorization", format!("Bearer {}", auth))
            .header("Content-type", "application/json")
            .send()
            .and_then(move |res| res.into_body().concat2())
            .map_err(|err| {
                eprintln!("Error submitting withdrawal request: {:?}", err);
            })
            .and_then(move |_| Ok(()))
    };

    runtime
        .block_on(
            delay(1000)
                .map_err(|err| panic!(err))
                // 1e17 Wei (0.1 Ether top-up balance)
                .and_then(move |_| {
                    // TODO: There's a bug in the authorization API
                    topup("alice", 3010, "admin", "100000000000000000".to_string())
                })
                .and_then(move |_| delay(3000).map_err(|err| panic!(err)))
                // Send 0.005 ETH to Charlie on Node2 (example.op2.charlie)
                // Can we specify an ILP Address directly? Having to specify the IP/username kinda defeats the purpose.
                .and_then(move |_| {
                    send_money_to_username(
                        node1_http, node2_http, 5_000_000, "charlie", "alice", "in_alice",
                    )
                })
                // Send 0.09 ETH to Charlie on Node3 (example.op3.charlie)
                .and_then(move |_| {
                    send_money_to_username(
                        node1_http, node3_http, 90_000_000, "charlie", "alice", "in_alice",
                    )
                })
                .and_then(move |_| get_balances())
                .and_then(move |balances| {
                    // Alice still has 0.005 ETH left
                    assert_eq!(balances[0], 5_000_000);
                    // Operator 1 owes operator 2 950k sats (900 went to the next node, 50 went to this node)
                    assert_eq!(balances[1], 950_000);
                    // Charlie2 has 49.5k drops (op2 took a cut)
                    assert_eq!(balances[2], 49_500);
                    // Operator 1 owes operator 2 950k sats (symmetric to balances[1] but on node2 instead of node1)
                    assert_eq!(balances[3], -950_000);
                    // Operator 2 owes operator 3 900k sats (on node 2 so it's negative)
                    assert_eq!(balances[4], 891_000);
                    // Operator 2 owes operator 3 891k sats (on node 3 so it's negative)
                    assert_eq!(balances[5], -891_000);
                    // Charlie3 has 0.891 ETH
                    assert_eq!(balances[6], 89_100_000);
                    Ok(())
                })
                // example.op3.charlie wants to withdraw some ETH
                .and_then(move |_| withdraw(node3_http, "charlie", "admin", 85_000_000))
                .and_then(move |_| delay(2000).map_err(|err| panic!(err)))
                .and_then(move |_| get_balances())
                .and_then(move |balances| {
                    assert_eq!(balances[0], 5_000_000);
                    assert_eq!(balances[1], 950_000);
                    assert_eq!(balances[2], 49_500); // remember, this is xrp drops
                    assert_eq!(balances[3], -950_000);
                    assert_eq!(balances[4], 891_000);
                    assert_eq!(balances[5], -891_000);
                    // Charlie's balance has been updated due to the withdrawal
                    assert_eq!(balances[6], 4_100_000);
                    let (eloop, transport) = Http::new("http://localhost:8545").unwrap();
                    eloop.into_remote();
                    let web3 = Web3::new(transport);
                    web3.eth()
                        .balance(
                            EthAddress::from_str(&node3_charlie_addr[2..]).unwrap(),
                            None,
                        )
                        .map_err(|_| ())
                        .and_then(move |balance| {
                            // 100 ETH from Ganache + 0.085 ETH. Connector's engine paid the gas.
                            assert_eq!(balance.to_string(), "100085000000000000000");
                            ganache_pid.kill().unwrap();
                            Ok(())
                        })
                }),
            // When the XRP Engine supports it: example.op2.charlie wants to withdraw some XRP
            // .and_then(move |_| withdraw(node2_http, "charlie", "admin", 50_000))
        )
        .unwrap();
}
