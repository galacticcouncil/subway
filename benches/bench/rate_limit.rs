use criterion::{criterion_group, Criterion};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use governor::DefaultKeyedRateLimiter;
use governor::Jitter;
use governor::RateLimiter;
use jsonrpsee::server::middleware::rpc::RpcServiceT;
use jsonrpsee::types::Request;
use jsonrpsee::{MethodResponse, ResponsePayload};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use subway::extensions::rate_limit::{build_quota, ConnectionRateLimit, IpRateLimit};

#[derive(Clone)]
struct MockService;
impl RpcServiceT<'static> for MockService {
    type Future = BoxFuture<'static, MethodResponse>;

    fn call(&self, req: Request<'static>) -> Self::Future {
        async move { MethodResponse::response(req.id, ResponsePayload::success("ok"), 1024) }.boxed()
    }
}

pub fn connection_rate_limit(c: &mut Criterion) {
    let rate_limit = ConnectionRateLimit::new(
        MockService,
        NonZeroU32::new(1000).unwrap(),
        Duration::from_millis(1000),
        Jitter::up_to(Duration::from_millis(10)),
        Default::default(),
    );

    c.bench_function("rate_limit/connection_rate_limit", |b| {
        b.iter(|| rate_limit.call(Request::new("test".into(), None, jsonrpsee::types::Id::Number(1))))
    });
}

pub fn ip_rate_limit(c: &mut Criterion) {
    let burst = NonZeroU32::new(1000).unwrap();
    let quota = build_quota(burst, Duration::from_millis(1000));
    let limiter = RateLimiter::keyed(quota);
    let rate_limit = IpRateLimit::new(
        MockService,
        "::1".to_string(),
        std::sync::Arc::new(limiter),
        Jitter::up_to(Duration::from_millis(10)),
        Default::default(),
    );

    c.bench_function("rate_limit/ip_rate_limit", |b| {
        b.iter(|| rate_limit.call(Request::new("test".into(), None, jsonrpsee::types::Id::Number(1))))
    });
}

/// Approximates the read-heavy traffic pattern of a HydraDX-style trading dApp: many
/// concurrent browser sessions (each pinned to its own IP, or its own spoofed/legitimate
/// `X-Forwarded-For` value behind a load balancer) hitting the gateway at once. Measures
/// whether the per-request hot path (`IpRateLimit::call`) regresses as the keyed
/// limiter's backing map grows to track more distinct clients -- it shouldn't, since
/// housekeeping (`retain_recent`/`shrink_to_fit`) now runs on its own periodic task
/// instead of inline with request handling.
pub fn ip_rate_limit_high_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("rate_limit/ip_rate_limit_high_cardinality");
    for &known_ips in &[10usize, 1_000, 50_000] {
        let burst = NonZeroU32::new(1000).unwrap();
        let quota = build_quota(burst, Duration::from_millis(1000));
        let limiter: Arc<DefaultKeyedRateLimiter<String>> = Arc::new(RateLimiter::keyed(quota));

        // simulate `known_ips` other concurrent sessions/wallets already tracked by the
        // limiter, growing its backing map without affecting the bucket of the IP under test.
        for i in 0..known_ips {
            let _ = limiter.check_key(&format!("203.0.113.{}.{}", i / 255, i % 255));
        }

        let rate_limit = IpRateLimit::new(
            MockService,
            "198.51.100.1".to_string(),
            limiter.clone(),
            Jitter::up_to(Duration::from_millis(10)),
            Default::default(),
        );

        group.bench_function(format!("{known_ips}_known_ips"), |b| {
            b.iter(|| rate_limit.call(Request::new("test".into(), None, jsonrpsee::types::Id::Number(1))))
        });
    }
    group.finish();
}

criterion_group!(
    rate_limit_benches,
    connection_rate_limit,
    ip_rate_limit,
    ip_rate_limit_high_cardinality
);
