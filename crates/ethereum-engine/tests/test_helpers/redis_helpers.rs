// Copied from https://github.com/mitsuhiko/redis-rs/blob/9a1777e8a90c82c315a481cdf66beb7d69e681a2/tests/support/mod.rs
#![allow(dead_code)]

use redis_crate as redis;

use std::env;
use std::fs;
use std::process;
use std::thread::sleep;
use std::time::{Duration, Instant};

use std::path::PathBuf;

use futures::Future;

use redis::{ConnectionAddr, ConnectionInfo, RedisError};

use tokio::timer::Delay;

#[allow(unused)]
pub fn connection_info_to_string(info: ConnectionInfo) -> String {
    match info.addr.as_ref() {
        ConnectionAddr::Tcp(url, port) => format!("redis://{}:{}/{}", url, port, info.db),
        ConnectionAddr::Unix(path) => {
            format!("redis+unix:{}?db={}", path.to_str().unwrap(), info.db)
        }
    }
}

pub fn get_open_port(try_port: Option<u16>) -> u16 {
    if let Some(port) = try_port {
        let listener = net2::TcpBuilder::new_v4().unwrap();
        listener.reuse_address(true).unwrap();
        if let Ok(listener) = listener.bind(&format!("127.0.0.1:{}", port)) {
            return listener.listen(1).unwrap().local_addr().unwrap().port();
        }
    }

    for _i in 0..1000 {
        let listener = net2::TcpBuilder::new_v4().unwrap();
        listener.reuse_address(true).unwrap();
        if let Ok(listener) = listener.bind("127.0.0.1:0") {
            return listener.listen(1).unwrap().local_addr().unwrap().port();
        }
    }
    panic!("Cannot find open port!");
}

pub fn delay(ms: u64) -> impl Future<Item = (), Error = ()> {
    Delay::new(Instant::now() + Duration::from_millis(ms)).map_err(|err| panic!(err))
}

#[derive(PartialEq)]
enum ServerType {
    Tcp,
    Unix,
}

pub struct RedisServer {
    pub process: process::Child,
    addr: redis::ConnectionAddr,
}

impl ServerType {
    fn get_intended() -> ServerType {
        match env::var("REDISRS_SERVER_TYPE")
            .ok()
            .as_ref()
            .map(|x| &x[..])
        {
            // Default to unix socket unlike original version
            Some("tcp") => ServerType::Tcp,
            _ => ServerType::Unix,
        }
    }
}

impl RedisServer {
    pub fn spawn_with_port(port: u16) -> std::process::Child {
        let mut cmd = process::Command::new("redis-server");
        cmd.stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .arg("--port")
            .arg(port.to_string())
            .arg("--bind")
            .arg("127.0.0.1");

        cmd.spawn().expect(
            "Could not spawn redis-server process, please ensure \
             that all redis components are installed",
        )
    }

    pub fn new() -> RedisServer {
        let server_type = ServerType::get_intended();
        let mut cmd = process::Command::new("redis-server");
        cmd.stdout(process::Stdio::null())
            .stderr(process::Stdio::null());

        let addr = match server_type {
            ServerType::Tcp => {
                // this is technically a race but we can't do better with
                // the tools that redis gives us :(
                let listener = net2::TcpBuilder::new_v4()
                    .unwrap()
                    .reuse_address(true)
                    .unwrap()
                    .bind("127.0.0.1:0")
                    .unwrap()
                    .listen(1)
                    .unwrap();
                let server_port = listener.local_addr().unwrap().port();
                cmd.arg("--port")
                    .arg(server_port.to_string())
                    .arg("--bind")
                    .arg("127.0.0.1");
                redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), server_port)
            }
            ServerType::Unix => {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = format!("/tmp/redis-rs-test-{}-{}.sock", a, b);
                cmd.arg("--port").arg("0").arg("--unixsocket").arg(&path);
                redis::ConnectionAddr::Unix(PathBuf::from(&path))
            }
        };

        let process = cmd.spawn().expect(
            "Could not spawn redis-server process, please ensure \
             that all redis components are installed",
        );
        RedisServer { process, addr }
    }

    pub fn wait(&mut self) {
        self.process.wait().unwrap();
    }

    pub fn get_client_addr(&self) -> &redis::ConnectionAddr {
        &self.addr
    }

    pub fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let redis::ConnectionAddr::Unix(ref path) = *self.get_client_addr() {
            fs::remove_file(&path).ok();
        }
    }
}

impl Default for RedisServer {
    fn default() -> Self {
        RedisServer::new()
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct TestContext {
    pub server: RedisServer,
    pub client: redis::Client,
}

impl TestContext {
    pub fn new() -> TestContext {
        let server = RedisServer::new();

        let client = redis::Client::open(redis::ConnectionInfo {
            addr: Box::new(server.get_client_addr().clone()),
            db: 0,
            passwd: None,
        })
        .unwrap();
        let mut con;

        let millisecond = Duration::from_millis(1);
        loop {
            match client.get_connection() {
                Err(err) => {
                    if err.is_connection_refusal() {
                        sleep(millisecond);
                    } else {
                        panic!("Could not connect: {}", err);
                    }
                }
                Ok(x) => {
                    con = x;
                    break;
                }
            }
        }
        redis::cmd("FLUSHALL").execute(&mut con);

        TestContext { server, client }
    }

    // This one was added and not in the original file
    pub fn get_client_connection_info(&self) -> redis::ConnectionInfo {
        redis::ConnectionInfo {
            addr: Box::new(self.server.get_client_addr().clone()),
            db: 0,
            passwd: None,
        }
    }

    pub fn connection(&self) -> redis::Connection {
        self.client.get_connection().unwrap()
    }

    pub fn async_connection(
        &self,
    ) -> impl Future<Item = redis::aio::Connection, Error = RedisError> {
        self.client.get_async_connection()
    }

    pub fn stop_server(&mut self) {
        self.server.stop();
    }

    pub fn shared_async_connection(
        &self,
    ) -> impl Future<Item = redis::aio::SharedConnection, Error = RedisError> {
        self.client.get_shared_async_connection()
    }
}

impl Default for TestContext {
    fn default() -> Self {
        TestContext::new()
    }
}
