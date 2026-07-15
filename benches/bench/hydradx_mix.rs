//! Benchmarks a realistic HydraDX-style dApp page load: a batch fetching the current
//! header, a storage read (e.g. an asset balance or pool reserve), node health, and the
//! runtime version, run concurrently from an increasing number of simulated browser
//! sessions.
//!
//! This exercises the full subway pipeline end-to-end (client -> `inject_params` ->
//! `cache` -> upstream) rather than a single isolated middleware, so it also acts as a
//! guard for the config-validated middleware ordering fix: `cache` must run after
//! `inject_params`/`block_tag`, otherwise responses get cached under unresolved block
//! tags. The middleware list below is deliberately in the now-enforced valid order.

use criterion::{criterion_group, Criterion};
use futures::future::join_all;
use jsonrpsee::core::params::BatchRequestBuilder;
use tokio::runtime::Runtime as TokioRuntime;

use subway::{
    config::{Config, MiddlewaresConfig, RpcDefinitions, RpcMethod},
    extensions::{
        api::SubstrateApiConfig, cache::CacheConfig, client::ClientConfig, server::ServerConfig, ExtensionsConfig,
    },
};

use crate::helpers::{
    self,
    client::{rpc_params, ws_client, ClientT},
    HYDRADX_METHODS,
};

const UPSTREAM_ENDPOINT: &str = "127.0.0.1:9977";
const SUBWAY_SERVER_ADDR: &str = "127.0.0.1";
const SUBWAY_SERVER_PORT: u16 = 9978;

fn config() -> Config {
    Config {
        extensions: ExtensionsConfig {
            client: Some(ClientConfig {
                endpoints: vec![format!("ws://{}", UPSTREAM_ENDPOINT)],
                shuffle_endpoints: false,
            }),
            server: Some(ServerConfig {
                listen_address: SUBWAY_SERVER_ADDR.to_string(),
                port: SUBWAY_SERVER_PORT,
                max_connections: 1024 * 1024,
                max_subscriptions_per_connection: 1024,
                max_batch_size: None,
                request_timeout_seconds: 120,
                http_methods: Vec::new(),
                cors: None,
            }),
            substrate_api: Some(SubstrateApiConfig {
                stale_timeout_seconds: 5_000,
            }),
            cache: Some(CacheConfig {
                default_size: 10_000,
                default_ttl_seconds: Some(6),
            }),
            ..Default::default()
        },
        middlewares: MiddlewaresConfig {
            methods: vec!["inject_params".to_string(), "cache".to_string(), "upstream".to_string()],
            subscriptions: vec![],
        },
        rpcs: RpcDefinitions {
            methods: HYDRADX_METHODS
                .iter()
                .map(|method| RpcMethod {
                    method: method.to_string(),
                    params: vec![],
                    response: None,
                    cache: None,
                    delay_ms: None,
                    track_submissions: false,
                    rate_limit_weight: 1,
                })
                .collect(),
            subscriptions: vec![],
            aliases: vec![],
        },
    }
}

async fn server() -> subway::server::SubwayServerHandle {
    subway::server::build(config()).await.unwrap()
}

/// Simulates `concurrent_sessions` browser tabs each loading the page at once, where a
/// page load issues one batch request covering the header/storage/health/runtime-version
/// call mix above.
fn hydradx_page_load(c: &mut Criterion) {
    let rt = TokioRuntime::new().unwrap();
    let (_url, _upstream) = rt.block_on(helpers::ws_server(rt.handle().clone(), UPSTREAM_ENDPOINT));
    let subway_server = rt.block_on(server());
    let url = format!("ws://{}", subway_server.addr);

    let mut batch = BatchRequestBuilder::new();
    for method in HYDRADX_METHODS {
        batch.insert(method, rpc_params![]).unwrap();
    }

    let mut group = c.benchmark_group("hydradx_mix/page_load");
    for &concurrent_sessions in &[1usize, 10, 100] {
        group.bench_function(format!("{concurrent_sessions}_sessions"), |b| {
            b.to_async(&rt).iter_with_setup(
                || {
                    let clients = (0..concurrent_sessions).map(|_| ws_client(&url)).collect::<Vec<_>>();
                    // We have to use `block_in_place` here since `b.to_async(rt)` automatically enters the
                    // runtime context and simply calling `block_on` here will cause the code to panic.
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(join_all(clients)))
                },
                |clients| async {
                    let tasks = clients.into_iter().map(|client| {
                        let batch = batch.clone();
                        rt.spawn(async move {
                            client.batch_request::<serde_json::Value>(batch).await.unwrap();
                        })
                    });
                    join_all(tasks).await;
                },
            )
        });
    }
    group.finish();
}

criterion_group!(hydradx_mix_benches, hydradx_page_load);
