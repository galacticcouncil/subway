use std::time::Duration;

use jsonrpsee::types::ErrorObjectOwned;

pub const SYNC_FAST_CALL: &str = "fast_call";
pub const ASYNC_FAST_CALL: &str = "fast_call_async";
pub const SYNC_MEM_CALL: &str = "memory_intense";
pub const ASYNC_MEM_CALL: &str = "memory_intense_async";
pub const SYNC_SLOW_CALL: &str = "slow_call";
pub const ASYNC_SLOW_CALL: &str = "slow_call_async";
pub const SUB_METHOD_NAME: &str = "sub";
pub const UNSUB_METHOD_NAME: &str = "unsub";
pub const ASYNC_INJECT_CALL: &str = "inject_call_async";

pub const SYNC_METHODS: [&str; 3] = [SYNC_FAST_CALL, SYNC_MEM_CALL, SYNC_SLOW_CALL];
pub const ASYNC_METHODS: [&str; 3] = [ASYNC_FAST_CALL, ASYNC_MEM_CALL, ASYNC_SLOW_CALL];

// 1 KiB = 1024 bytes
pub const KIB: usize = 1024;
pub const MIB: usize = 1024 * KIB;
pub const SLOW_CALL: Duration = Duration::from_millis(1);

/// Run jsonrpsee WebSocket server for benchmarks.
pub async fn ws_server(handle: tokio::runtime::Handle, url: &str) -> (String, jsonrpsee::server::ServerHandle) {
    use jsonrpsee::{core::server::SubscriptionMessage, server::ServerBuilder};

    let server = ServerBuilder::default()
        .max_request_body_size(u32::MAX)
        .max_response_body_size(u32::MAX)
        .max_connections(10 * 1024)
        .custom_tokio_runtime(handle)
        .build(url)
        .await
        .unwrap();

    let mut module = gen_rpc_module();

    module
        .register_subscription(
            SUB_METHOD_NAME,
            SUB_METHOD_NAME,
            UNSUB_METHOD_NAME,
            |_params, pending, _ctx, _| async move {
                let sink = pending.accept().await?;
                let msg = SubscriptionMessage::from_json(&"Hello")?;
                sink.send(msg).await?;
                Ok(())
            },
        )
        .unwrap();
    module
        .register_subscription(
            "chain_subscribeNewHeads",
            "chain_newHead",
            "chain_unsubscribeNewHeads",
            |_params, pending, _ctx, _| async move {
                let sink = pending.accept().await?;
                let msg = SubscriptionMessage::from_json(&serde_json::json!({ "number": "0x4321" }))?;
                sink.send(msg).await?;
                Ok(())
            },
        )
        .unwrap();
    module
        .register_subscription(
            "chain_subscribeFinalizedHeads",
            "chain_finalizedHead",
            "chain_unsubscribeFinalizedHeads",
            |_params, pending, _ctx, _| async move {
                let sink = pending.accept().await?;
                let msg = SubscriptionMessage::from_json(&serde_json::json!({ "number": "0x4321" }))?;
                sink.send(msg).await?;
                Ok(())
            },
        )
        .unwrap();

    let addr = format!("ws://{}", server.local_addr().unwrap());
    let handle = server.start(module);
    (addr, handle)
}

fn gen_rpc_module() -> jsonrpsee::RpcModule<()> {
    let mut module = jsonrpsee::RpcModule::new(());

    module
        .register_method(SYNC_FAST_CALL, |_, _, _| Ok::<_, ErrorObjectOwned>("lo"))
        .unwrap();
    module
        .register_async_method(ASYNC_FAST_CALL, |_, _, _| async {
            Result::<_, ErrorObjectOwned>::Ok("lo")
        })
        .unwrap();

    module
        .register_method(SYNC_MEM_CALL, |_, _, _| Ok::<_, ErrorObjectOwned>("A".repeat(MIB)))
        .unwrap();

    module
        .register_async_method(ASYNC_MEM_CALL, |_, _, _| async move {
            Result::<_, ErrorObjectOwned>::Ok("A".repeat(MIB))
        })
        .unwrap();

    module
        .register_method(SYNC_SLOW_CALL, |_, _, _| {
            std::thread::sleep(SLOW_CALL);
            Ok::<_, ErrorObjectOwned>("slow call")
        })
        .unwrap();

    module
        .register_async_method(ASYNC_SLOW_CALL, |_, _, _| async move {
            tokio::time::sleep(SLOW_CALL).await;
            Result::<_, ErrorObjectOwned>::Ok("slow call async")
        })
        .unwrap();

    module
        .register_async_method(ASYNC_INJECT_CALL, |_, _, _| async move {
            tokio::time::sleep(SLOW_CALL).await;
            Result::<_, ErrorObjectOwned>::Ok("inject call async")
        })
        .unwrap();

    module
        .register_async_method("chain_getBlockHash", |_, _, _| async move {
            tokio::time::sleep(SLOW_CALL).await;
            Result::<_, ErrorObjectOwned>::Ok("0x42")
        })
        .unwrap();

    // A handful of calls typical of a Substrate DeFi frontend's page load: current
    // header, a storage read (e.g. an asset balance or pool reserve), node health and
    // runtime version. Used by the `hydradx_mix` benchmark.
    module
        .register_async_method(HYDRADX_CHAIN_GET_HEADER, |_, _, _| async move {
            Result::<_, ErrorObjectOwned>::Ok(serde_json::json!({
                "number": "0x4321",
                "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            }))
        })
        .unwrap();
    module
        .register_async_method(HYDRADX_STATE_GET_STORAGE, |_, _, _| async move {
            Result::<_, ErrorObjectOwned>::Ok("0x0000000000000000")
        })
        .unwrap();
    module
        .register_method(HYDRADX_SYSTEM_HEALTH, |_, _, _| {
            Ok::<_, ErrorObjectOwned>(serde_json::json!({ "peers": 10, "isSyncing": false, "shouldHavePeers": true }))
        })
        .unwrap();
    module
        .register_method(HYDRADX_STATE_GET_RUNTIME_VERSION, |_, _, _| {
            Ok::<_, ErrorObjectOwned>(serde_json::json!({ "specVersion": 100, "transactionVersion": 1 }))
        })
        .unwrap();

    module
}

pub const HYDRADX_CHAIN_GET_HEADER: &str = "chain_getHeader";
pub const HYDRADX_STATE_GET_STORAGE: &str = "state_getStorage";
pub const HYDRADX_SYSTEM_HEALTH: &str = "system_health";
pub const HYDRADX_STATE_GET_RUNTIME_VERSION: &str = "state_getRuntimeVersion";

pub const HYDRADX_METHODS: [&str; 4] = [
    HYDRADX_CHAIN_GET_HEADER,
    HYDRADX_STATE_GET_STORAGE,
    HYDRADX_SYSTEM_HEALTH,
    HYDRADX_STATE_GET_RUNTIME_VERSION,
];

pub mod client {
    use jsonrpsee::client_transport::ws::{Url, WsTransportClientBuilder};
    use jsonrpsee::ws_client::{WsClient, WsClientBuilder};

    pub use jsonrpsee::core::client::{ClientT, SubscriptionClientT};
    pub use jsonrpsee::http_client::HeaderMap;
    pub use jsonrpsee::rpc_params;

    pub async fn ws_client(url: &str) -> WsClient {
        WsClientBuilder::default()
            .max_request_size(u32::MAX)
            .max_concurrent_requests(1024 * 1024)
            .build(url)
            .await
            .unwrap()
    }

    pub async fn ws_handshake(url: &str, headers: HeaderMap) {
        let url: Url = url.parse().unwrap();
        WsTransportClientBuilder::default()
            .max_request_size(u32::MAX)
            .set_headers(headers)
            .build(url)
            .await
            .unwrap();
    }
}
