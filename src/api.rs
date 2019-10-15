use crate::stores::{IdempotentEngineData, IdempotentEngineStore};
use crate::{ApiResponse, SettlementEngine};
use bytes::{buf::FromBuf, Bytes};
use futures::{
    future::{err, ok, Either},
    Future,
};
use hyper::{Response, StatusCode};
use interledger_settlement::Quantity;
use log::error;
use ring::digest::{digest, SHA256};
use tokio::executor::spawn;

use warp::{self, Filter};

/// # Settlement Engine API
///
/// Tower_Web service which exposes settlement related endpoints as described in RFC536,
/// See [forum discussion](https://forum.interledger.org/t/settlement-architecture/545) for more context.
/// All endpoints are idempotent.
pub fn create_settlement_engine_filter<E, S>(
    engine: E,
    store: S,
) -> warp::filters::BoxedFilter<(impl warp::Reply,)>
where
    E: SettlementEngine + Clone + Send + Sync + 'static,
    S: IdempotentEngineStore + Clone + Send + Sync + 'static,
{
    let with_store = warp::any().map(move || store.clone()).boxed();
    let with_engine = warp::any().map(move || engine.clone()).boxed();
    let idempotency = warp::header::optional::<String>("idempotency-key");
    let account_id = warp::path("accounts").and(warp::path::param2::<String>()); // account_id

    // POST /accounts/ (optional idempotency-key header)
    // Body is a Vec<u8> object
    let accounts = warp::post2()
        .and(warp::path("accounts"))
        .and(warp::path::end())
        .and(idempotency)
        .and(warp::body::concat())
        .and(with_engine.clone())
        .and(with_store.clone())
        .and_then(
            move |idempotency_key: Option<String>,
                  body: warp::body::FullBody,
                  engine: E,
                  store: S| {
                // TODO: Hacky way to get a string from the body, what's a better way?
                let account_id = String::from_utf8_lossy(&Vec::from_buf(body)).to_string();
                let input = format!("{}", account_id);
                let input_hash = get_hash_of(input.as_ref());

                // Wrap do_send_outgoing_message in a closure to be invoked by
                // the idempotency wrapper
                let create_account_fn = move || engine.create_account(account_id);
                make_idempotent_call(store, create_account_fn, input_hash, idempotency_key)
                    // TODO Replace with error case with response
                    .map_err(move |(_status_code, error_msg)| warp::reject::custom(error_msg))
                    .and_then(move |(status_code, message)| {
                        Ok(Response::builder()
                            .status(status_code)
                            .body(message)
                            .unwrap())
                    })
            },
        );

    // POST /accounts/:account_id/settlements (optional idempotency-key header)
    // Body is a Quantity object
    let settlement_endpoint = account_id.and(warp::path("settlements"));
    let settlements = warp::post2()
        .and(settlement_endpoint)
        .and(warp::path::end())
        .and(idempotency)
        .and(warp::body::json())
        .and(with_engine.clone())
        .and(with_store.clone())
        .and_then(
            move |id: String,
                  idempotency_key: Option<String>,
                  quantity: Quantity,
                  engine: E,
                  store: S| {
                let input = format!("{}{:?}", id, quantity);
                let input_hash = get_hash_of(input.as_ref());
                let send_money_fn = move || engine.send_money(id, quantity);
                make_idempotent_call(store, send_money_fn, input_hash, idempotency_key)
                    // TODO Replace with error case with response
                    .map_err(move |(_status_code, error_msg)| warp::reject::custom(error_msg))
                    .and_then(move |(status_code, message)| {
                        Ok(Response::builder()
                            .status(status_code)
                            .body(message)
                            .unwrap())
                    })
            },
        );

    // POST /accounts/:account_id/messages (optional idempotency-key header)
    // Body is a Vec<u8> object
    let messages_endpoint = account_id.and(warp::path("messages"));
    let messages = warp::post2()
        .and(messages_endpoint)
        .and(warp::path::end())
        .and(idempotency)
        .and(warp::body::concat())
        .and(with_engine.clone())
        .and(with_store.clone())
        .and_then(
            move |id: String,
                  idempotency_key: Option<String>,
                  body: warp::body::FullBody,
                  engine: E,
                  store: S| {
                // Gets called by our settlement engine, forwards the request outwards
                // until it reaches the peer's settlement engine.
                let message = Vec::from_buf(body);
                let input = format!("{}{:?}", id, message);
                let input_hash = get_hash_of(input.as_ref());

                // Wrap do_send_outgoing_message in a closure to be invoked by
                // the idempotency wrapper
                let receive_message_fn = move || engine.receive_message(id, message);
                make_idempotent_call(store, receive_message_fn, input_hash, idempotency_key)
                    // TODO Replace with error case with response
                    .map_err(move |(_status_code, error_msg)| warp::reject::custom(error_msg))
                    .and_then(move |(status_code, message)| {
                        Ok(Response::builder()
                            .status(status_code)
                            .body(message)
                            .unwrap())
                    })
            },
        );

    accounts.or(settlements).or(messages).boxed()
}

// Helper function that returns any idempotent data that corresponds to a
// provided idempotency key. It fails if the hash of the input that
// generated the idempotent data does not match the hash of the provided input.
fn check_idempotency<S>(
    store: S,
    idempotency_key: String,
    input_hash: [u8; 32],
) -> impl Future<Item = Option<(StatusCode, Bytes)>, Error = String>
where
    S: IdempotentEngineStore + Clone + Send + Sync + 'static,
{
    store
        .load_idempotent_data(idempotency_key.clone())
        .map_err(move |_| {
            let error_msg = format!(
                "Couldn't load idempotent data for idempotency key {:?}",
                idempotency_key
            );
            error!("{}", error_msg);
            error_msg
        })
        .and_then(move |ret: Option<IdempotentEngineData>| {
            if let Some(ret) = ret {
                if ret.2 == input_hash {
                    Ok(Some((ret.0, ret.1)))
                } else {
                    Ok(Some((
                        StatusCode::from_u16(409).unwrap(),
                        Bytes::from(&b"Provided idempotency key is tied to other input"[..]),
                    )))
                }
            } else {
                Ok(None)
            }
        })
}

fn make_idempotent_call<S, F>(
    store: S,
    f: F,
    input_hash: [u8; 32],
    idempotency_key: Option<String>,
) -> impl Future<Item = (StatusCode, String), Error = (StatusCode, String)>
where
    F: FnOnce() -> Box<dyn Future<Item = ApiResponse, Error = ApiResponse> + Send>,
    S: IdempotentEngineStore + Clone + Send + Sync + 'static,
{
    if let Some(idempotency_key) = idempotency_key {
        // If there an idempotency key was provided, check idempotency
        // and the key was not present or conflicting with an existing
        // key, perform the call and save the idempotent return data
        Either::A(
            check_idempotency(store.clone(), idempotency_key.clone(), input_hash)
                .map_err(|err| (StatusCode::from_u16(502).unwrap(), err))
                .and_then(move |ret: Option<(StatusCode, Bytes)>| {
                    if let Some(ret) = ret {
                        let resp = (ret.0, String::from_utf8_lossy(&ret.1).to_string());
                        if ret.0.is_success() {
                            Either::A(Either::A(ok(resp)))
                        } else {
                            Either::A(Either::B(err(resp)))
                        }
                    } else {
                        Either::B(
                            f().map_err({
                                let store = store.clone();
                                let idempotency_key = idempotency_key.clone();
                                move |ret: (StatusCode, String)| {
                                    spawn(store.save_idempotent_data(
                                        idempotency_key,
                                        input_hash,
                                        ret.0,
                                        Bytes::from(ret.1.clone()),
                                    ));
                                    (ret.0, ret.1)
                                }
                            })
                            .and_then(
                                move |ret: (StatusCode, String)| {
                                    store
                                        .save_idempotent_data(
                                            idempotency_key,
                                            input_hash,
                                            ret.0,
                                            Bytes::from(ret.1.clone()),
                                        )
                                        .map_err({
                                            let ret = ret.clone();
                                            move |_| ret
                                        })
                                        .and_then(move |_| Ok(ret))
                                },
                            ),
                        )
                    }
                }),
        )
    } else {
        // otherwise just make the call without any idempotency saves
        Either::B(
            f().map_err(move |ret: (StatusCode, String)| ret)
                .and_then(move |ret: (StatusCode, String)| Ok(ret)),
        )
    }
}

fn get_hash_of(preimage: &[u8]) -> [u8; 32] {
    let mut hash = [0; 32];
    hash.copy_from_slice(digest(&SHA256, preimage).as_ref());
    hash
}

#[cfg(test)]
mod tests {
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::ApiResponse;
    use serde_json::json;

    #[derive(Clone)]
    struct TestEngine;

    #[derive(Debug, Clone)]
    pub struct TestAccount;

    #[derive(Clone)]
    pub struct TestStore {
        #[allow(clippy::all)]
        pub cache: Arc<RwLock<HashMap<String, (StatusCode, String, [u8; 32])>>>,
        pub cache_hits: Arc<RwLock<u64>>,
    }

    fn test_store() -> TestStore {
        TestStore {
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_hits: Arc::new(RwLock::new(0)),
        }
    }

    impl IdempotentEngineStore for TestStore {
        fn load_idempotent_data(
            &self,
            idempotency_key: String,
        ) -> Box<dyn Future<Item = Option<IdempotentEngineData>, Error = ()> + Send> {
            let cache = self.cache.read();
            if let Some(data) = cache.get(&idempotency_key) {
                let mut guard = self.cache_hits.write();
                *guard += 1; // used to test how many times this branch gets executed
                Box::new(ok(Some((data.0, Bytes::from(data.1.clone()), data.2))))
            } else {
                Box::new(ok(None))
            }
        }

        fn save_idempotent_data(
            &self,
            idempotency_key: String,
            input_hash: [u8; 32],
            status_code: StatusCode,
            data: Bytes,
        ) -> Box<dyn Future<Item = (), Error = ()> + Send> {
            let mut cache = self.cache.write();
            cache.insert(
                idempotency_key,
                (
                    status_code,
                    String::from_utf8_lossy(&data).to_string(),
                    input_hash,
                ),
            );
            Box::new(ok(()))
        }
    }

    pub static IDEMPOTENCY: &str = "abcd01234";

    impl SettlementEngine for TestEngine {
        fn send_money(
            &self,
            _account_id: String,
            _money: Quantity,
        ) -> Box<dyn Future<Item = ApiResponse, Error = ApiResponse> + Send> {
            Box::new(ok((StatusCode::from_u16(200).unwrap(), "OK".to_string())))
        }

        fn receive_message(
            &self,
            _account_id: String,
            _message: Vec<u8>,
        ) -> Box<dyn Future<Item = ApiResponse, Error = ApiResponse> + Send> {
            Box::new(ok((
                StatusCode::from_u16(200).unwrap(),
                "RECEIVED".to_string(),
            )))
        }

        fn create_account(
            &self,
            _account_id: String,
        ) -> Box<dyn Future<Item = ApiResponse, Error = ApiResponse> + Send> {
            Box::new(ok((
                StatusCode::from_u16(201).unwrap(),
                "CREATED".to_string(),
            )))
        }
    }

    #[test]
    fn idempotent_execute_settlement() {
        let store = test_store();
        let engine = TestEngine;
        let api = create_settlement_engine_filter(engine, store.clone());

        let settlement_call = move |id, amount, scale| {
            warp::test::request()
                .method("POST")
                .path(&format!("/accounts/{}/settlements", id))
                .body(json!(Quantity::new(amount, scale)).to_string())
                .header("Idempotency-Key", IDEMPOTENCY.clone())
                .reply(&api)
        };

        let ret = settlement_call("1".to_owned(), 100, 6);
        assert_eq!(ret.status().as_u16(), 200);
        assert_eq!(ret.body(), "OK");

        // is idempotent
        let ret = settlement_call("1".to_owned(), 100, 6);
        assert_eq!(ret.status().as_u16(), 200);
        assert_eq!(ret.body(), "OK");

        // // fails with different id and same data
        let ret = settlement_call("42".to_owned(), 100, 6);
        // assert_eq!(ret.status().as_u16(), 409);
        assert_eq!(
            ret.body(),
            "Unhandled rejection: Provided idempotency key is tied to other input"
        );

        // fails with same id and different data
        let ret = settlement_call("1".to_owned(), 42, 6);
        // assert_eq!(ret.status().as_u16(), 409);
        assert_eq!(
            ret.body(),
            "Unhandled rejection: Provided idempotency key is tied to other input"
        );

        // fails with different id and different data
        let ret = settlement_call("42".to_owned(), 42, 6);
        // assert_eq!(ret.status().as_u16(), 409);
        assert_eq!(
            ret.body(),
            "Unhandled rejection: Provided idempotency key is tied to other input"
        );

        let cache = store.cache.read();
        let cached_data = cache.get(&IDEMPOTENCY.to_string()).unwrap();

        let cache_hits = store.cache_hits.read();
        assert_eq!(*cache_hits, 4);
        assert_eq!(cached_data.0, 200);
        assert_eq!(cached_data.1, "OK".to_string());
    }

    #[test]
    fn idempotent_receive_message() {
        let store = test_store();
        let engine = TestEngine;
        let api = create_settlement_engine_filter(engine, store.clone());

        let messages_call = move |id, msg| {
            warp::test::request()
                .method("POST")
                .path(&format!("/accounts/{}/messages", id))
                .body(msg)
                .header("Idempotency-Key", IDEMPOTENCY.clone())
                .reply(&api)
        };

        let ret = messages_call("1", vec![0]);
        assert_eq!(ret.status().as_u16(), 200);
        assert_eq!(ret.body(), "RECEIVED");

        // is idempotent
        let ret = messages_call("1", vec![0]);
        assert_eq!(ret.status().as_u16(), 200);
        assert_eq!(ret.body(), "RECEIVED");

        // // fails with different id and same data
        let ret = messages_call("42", vec![0]);
        // assert_eq!(ret.status().as_u16(), 409);
        assert_eq!(
            ret.body(),
            "Unhandled rejection: Provided idempotency key is tied to other input"
        );

        // fails with same id and different data
        let ret = messages_call("1", vec![42]);
        // assert_eq!(ret.status().as_u16(), 409);
        assert_eq!(
            ret.body(),
            "Unhandled rejection: Provided idempotency key is tied to other input"
        );

        // fails with different id and different data
        let ret = messages_call("42", vec![42]);
        // assert_eq!(ret.status().as_u16(), 409);
        assert_eq!(
            ret.body(),
            "Unhandled rejection: Provided idempotency key is tied to other input"
        );

        let cache = store.cache.read();
        let cached_data = cache.get(&IDEMPOTENCY.to_string()).unwrap();

        let cache_hits = store.cache_hits.read();
        assert_eq!(*cache_hits, 4);
        assert_eq!(cached_data.0, 200);
        assert_eq!(cached_data.1, "RECEIVED".to_string());
    }

    #[test]
    fn idempotent_create_account() {
        let store = test_store();
        let engine = TestEngine;
        let api = create_settlement_engine_filter(engine, store.clone());

        let create_account_call = move |id| {
            warp::test::request()
                .method("POST")
                .path("/accounts")
                .body(id)
                .header("Idempotency-Key", IDEMPOTENCY.clone())
                .reply(&api)
        };

        let ret = create_account_call("1");
        assert_eq!(ret.status().as_u16(), 201);
        assert_eq!(ret.body(), "CREATED");

        // is idempotent
        let ret = create_account_call("1");
        assert_eq!(ret.status().as_u16(), 201);
        assert_eq!(ret.body(), "CREATED");

        // fails with different id
        let ret = create_account_call("42");
        // assert_eq!(ret.status().as_u16(), 409);
        assert_eq!(
            ret.body(),
            "Unhandled rejection: Provided idempotency key is tied to other input"
        );

        let cache = store.cache.read();
        let cached_data = cache.get(&IDEMPOTENCY.to_string()).unwrap();

        let cache_hits = store.cache_hits.read();
        assert_eq!(*cache_hits, 2);
        assert_eq!(cached_data.0, 201);
        assert_eq!(cached_data.1, "CREATED".to_string());
    }
}
