use futures::{future::BoxFuture, FutureExt};
use jsonrpsee::server::middleware::rpc::RpcServiceT;
use jsonrpsee::types::Request;
use jsonrpsee::MethodResponse;
use substrate_prometheus_endpoint::{CounterVec, HistogramVec, U64};

use std::fmt::Display;

use crate::extensions::rate_limit::MethodWeights;

/// Label used for any method name that isn't one of the gateway's configured RPC
/// methods, so a client sending arbitrary/garbage method names can't grow the metrics
/// registry's label set without bound.
const UNKNOWN_METHOD_LABEL: &str = "<unknown>";

#[derive(Clone, Copy)]
pub enum Protocol {
    Ws,
    Http,
}

impl Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Ws => "ws",
            Self::Http => "http",
        };
        write!(f, "{}", str)
    }
}

#[derive(Clone)]
pub struct PrometheusService<S> {
    inner: S,
    protocol: Protocol,
    call_times: HistogramVec,
    calls_started: CounterVec<U64>,
    calls_finished: CounterVec<U64>,
    known_methods: MethodWeights,
}

impl<S> PrometheusService<S> {
    pub fn new(
        inner: S,
        protocol: Protocol,
        call_times: &HistogramVec,
        calls_started: &CounterVec<U64>,
        calls_finished: &CounterVec<U64>,
        known_methods: MethodWeights,
    ) -> Self {
        Self {
            inner,
            protocol,
            calls_started: calls_started.clone(),
            calls_finished: calls_finished.clone(),
            call_times: call_times.clone(),
            known_methods,
        }
    }
}

impl<'a, S> RpcServiceT<'a> for PrometheusService<S>
where
    S: RpcServiceT<'a> + Send + Sync + Clone + 'static,
{
    type Future = BoxFuture<'a, MethodResponse>;

    fn call(&self, req: Request<'a>) -> Self::Future {
        let protocol = self.protocol.to_string();
        // this middleware runs ahead of jsonrpsee's own method-existence check, so an
        // unrecognized method name here is client-controlled input, not a real method
        let method = if self.known_methods.contains(&req.method) {
            req.method.to_string()
        } else {
            UNKNOWN_METHOD_LABEL.to_string()
        };

        let histogram = self.call_times.with_label_values(&[&protocol, &method]);
        let started = self.calls_started.with_label_values(&[&protocol, &method]);
        let finished = self.calls_finished.clone();

        let service = self.inner.clone();
        async move {
            started.inc();

            let timer = histogram.start_timer();
            let res = service.call(req).await;
            timer.stop_and_record();
            finished
                .with_label_values(&[&protocol, &method, &res.is_error().to_string()])
                .inc();

            res
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RpcMethod;
    use jsonrpsee::{types::Id, ResponsePayload};
    use substrate_prometheus_endpoint::{HistogramOpts, Opts};

    #[derive(Clone)]
    struct MockService;
    impl<'a> RpcServiceT<'a> for MockService {
        type Future = BoxFuture<'a, MethodResponse>;

        fn call(&self, req: Request<'a>) -> Self::Future {
            async move { MethodResponse::response(req.id, ResponsePayload::success("ok"), 1024) }.boxed()
        }
    }

    fn method_weights(methods: &[&str]) -> MethodWeights {
        let methods = methods
            .iter()
            .map(|m| RpcMethod {
                method: m.to_string(),
                cache: None,
                params: vec![],
                response: None,
                delay_ms: None,
                track_submissions: false,
                rate_limit_weight: 1,
            })
            .collect::<Vec<_>>();
        MethodWeights::from_config(&methods)
    }

    #[tokio::test]
    async fn unrecognized_methods_are_bucketed_under_one_label() {
        let call_times = HistogramVec::new(HistogramOpts::new("t", "t"), &["protocol", "method"]).unwrap();
        let calls_started = CounterVec::new(Opts::new("s", "s"), &["protocol", "method"]).unwrap();
        let calls_finished = CounterVec::new(Opts::new("f", "f"), &["protocol", "method", "is_error"]).unwrap();

        let service = PrometheusService::new(
            MockService,
            Protocol::Ws,
            &call_times,
            &calls_started,
            &calls_finished,
            method_weights(&["known_method"]),
        );

        service
            .call(Request::new("known_method".into(), None, Id::Number(1)))
            .await;
        // two different, never-configured method names -- must not create two labels
        service
            .call(Request::new("bogus_method_a".into(), None, Id::Number(2)))
            .await;
        service
            .call(Request::new("bogus_method_b".into(), None, Id::Number(3)))
            .await;

        assert_eq!(calls_started.with_label_values(&["ws", "known_method"]).get(), 1);
        assert_eq!(calls_started.with_label_values(&["ws", UNKNOWN_METHOD_LABEL]).get(), 2);
        assert_eq!(calls_started.with_label_values(&["ws", "bogus_method_a"]).get(), 0);
        assert_eq!(calls_started.with_label_values(&["ws", "bogus_method_b"]).get(), 0);
    }
}
