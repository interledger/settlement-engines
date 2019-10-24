/// # Settlement Engine API
///
/// Web service which exposes settlement related endpoints as described in RFC536,
/// See [forum discussion](https://forum.interledger.org/t/settlement-architecture/545) for more context.
/// All endpoints are idempotent.
use crate::SettlementEngine;
use bytes::buf::FromBuf;
use futures::Future;
use hyper::Response;
use interledger_http::{
    error::default_rejection_handler,
    idempotency::{make_idempotent_call, IdempotentStore},
};
use interledger_settlement::Quantity;
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};

use warp::{self, reject::Rejection, Filter};

#[derive(Serialize, Deserialize, Debug, Clone, Hash)]
pub struct CreateAccount {
    id: String,
}

pub fn create_settlement_engine_filter<E, S>(
    engine: E,
    store: S,
) -> warp::filters::BoxedFilter<(impl warp::Reply,)>
where
    E: SettlementEngine + Clone + Send + Sync + 'static,
    S: IdempotentStore + Clone + Send + Sync + 'static,
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
        .and(warp::body::json())
        .and(with_engine.clone())
        .and(with_store.clone())
        .and_then(
            move |idempotency_key: Option<String>,
                  account_id: CreateAccount,
                  engine: E,
                  store: S| {
                let account_id = account_id.id;
                let input_hash = get_hash_of(account_id.as_ref());

                // Wrap do_send_outgoing_message in a closure to be invoked by
                // the idempotency wrapper
                let create_account_fn = move || engine.create_account(account_id);
                make_idempotent_call(store, create_account_fn, input_hash, idempotency_key)
                    .map_err::<_, Rejection>(move |err| err.into())
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
                    .map_err::<_, Rejection>(move |err| err.into())
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
                    .map_err::<_, Rejection>(move |err| err.into())
                    .and_then(move |(status_code, message)| {
                        Ok(Response::builder()
                            .status(status_code)
                            .body(message)
                            .unwrap())
                    })
            },
        );

    accounts
        .or(settlements)
        .or(messages)
        .recover(default_rejection_handler)
        .boxed()
}

// Helper function that returns any idempotent data that corresponds to a
// provided idempotency key. It fails if the hash of the input that
// generated the idempotent data does not match the hash of the provided input.
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
    use bytes::Bytes;
    use futures::future::ok;
    use http::StatusCode;
    use interledger_http::error::ApiError;
    use interledger_http::idempotency::IdempotentData;
    use serde_json::{json, Value};

    fn check_error_status_and_message(response: Response<Bytes>, status_code: u16, message: &str) {
        let err: Value = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(response.status().as_u16(), status_code);
        assert_eq!(err.get("status").unwrap(), status_code);
        assert_eq!(err.get("detail").unwrap(), message);
    }

    #[derive(Clone)]
    struct TestEngine;

    #[derive(Debug, Clone)]
    pub struct TestAccount;

    #[derive(Clone)]
    pub struct TestStore {
        #[allow(clippy::all)]
        pub cache: Arc<RwLock<HashMap<String, IdempotentData>>>,
        pub cache_hits: Arc<RwLock<u64>>,
    }

    fn test_store() -> TestStore {
        TestStore {
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_hits: Arc::new(RwLock::new(0)),
        }
    }

    impl IdempotentStore for TestStore {
        fn load_idempotent_data(
            &self,
            idempotency_key: String,
        ) -> Box<dyn Future<Item = Option<IdempotentData>, Error = ()> + Send> {
            let cache = self.cache.read();
            if let Some(data) = cache.get(&idempotency_key) {
                let mut guard = self.cache_hits.write();
                *guard += 1; // used to test how many times this branch gets executed
                Box::new(ok(Some(data.clone())))
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
                IdempotentData::new(status_code, data, input_hash),
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
        ) -> Box<dyn Future<Item = ApiResponse, Error = ApiError> + Send> {
            Box::new(ok((StatusCode::from_u16(200).unwrap(), Bytes::from("OK"))))
        }

        fn receive_message(
            &self,
            _account_id: String,
            _message: Vec<u8>,
        ) -> Box<dyn Future<Item = ApiResponse, Error = ApiError> + Send> {
            Box::new(ok((
                StatusCode::from_u16(200).unwrap(),
                Bytes::from("RECEIVED"),
            )))
        }

        fn create_account(
            &self,
            _account_id: String,
        ) -> Box<dyn Future<Item = ApiResponse, Error = ApiError> + Send> {
            Box::new(ok((
                StatusCode::from_u16(201).unwrap(),
                Bytes::from("CREATED"),
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
                .header("Idempotency-Key", IDEMPOTENCY)
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
        check_error_status_and_message(ret, 409, "Provided idempotency key is tied to other input");

        // fails with same id and different data
        let ret = settlement_call("1".to_owned(), 42, 6);
        check_error_status_and_message(ret, 409, "Provided idempotency key is tied to other input");

        // fails with different id and different data
        let ret = settlement_call("42".to_owned(), 42, 6);
        check_error_status_and_message(ret, 409, "Provided idempotency key is tied to other input");

        let cache = store.cache.read();
        let cached_data = cache.get(&IDEMPOTENCY.to_string()).unwrap();

        let cache_hits = store.cache_hits.read();
        assert_eq!(*cache_hits, 4);
        assert_eq!(cached_data.status, 200);
        assert_eq!(cached_data.body, "OK".to_string());
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
                .header("Idempotency-Key", IDEMPOTENCY)
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
        check_error_status_and_message(ret, 409, "Provided idempotency key is tied to other input");

        // fails with same id and different data
        let ret = messages_call("1", vec![42]);
        check_error_status_and_message(ret, 409, "Provided idempotency key is tied to other input");

        // fails with different id and different data
        let ret = messages_call("42", vec![42]);
        check_error_status_and_message(ret, 409, "Provided idempotency key is tied to other input");

        let cache = store.cache.read();
        let cached_data = cache.get(&IDEMPOTENCY.to_string()).unwrap();

        let cache_hits = store.cache_hits.read();
        assert_eq!(*cache_hits, 4);
        assert_eq!(cached_data.status, 200);
        assert_eq!(cached_data.body, "RECEIVED".to_string());
    }

    #[test]
    fn idempotent_create_account() {
        let store = test_store();
        let engine = TestEngine;
        let api = create_settlement_engine_filter(engine, store.clone());

        let create_account_call = move |id: &str| {
            warp::test::request()
                .method("POST")
                .path("/accounts")
                .body(json!(CreateAccount { id: id.to_string() }).to_string())
                .header("Idempotency-Key", IDEMPOTENCY)
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
        check_error_status_and_message(ret, 409, "Provided idempotency key is tied to other input");

        let cache = store.cache.read();
        let cached_data = cache.get(&IDEMPOTENCY.to_string()).unwrap();

        let cache_hits = store.cache_hits.read();
        assert_eq!(*cache_hits, 2);
        assert_eq!(cached_data.status, 201);
        assert_eq!(cached_data.body, "CREATED".to_string());
    }
}
