use async_trait::async_trait;
use opentelemetry::trace::FutureExt;

use crate::{
    extensions::prometheus::{get_rpc_metrics, RpcMetrics},
    middlewares::{CallRequest, CallResult, Middleware, MiddlewareBuilder, NextFn, RpcMethod, TRACER},
    utils::{TypeRegistry, TypeRegistryRef},
};

/// Records whether a submission method's upstream response was accepted or
/// rejected (and by which json-rpc error code) -- meant for author_submitExtrinsic
/// / eth_sendRawTransaction, where the rejection reason is the useful signal.
/// Never alters the request or response, purely observational.
pub struct SubmissionTrackerMiddleware {
    metrics: RpcMetrics,
}

impl SubmissionTrackerMiddleware {
    pub fn new(metrics: RpcMetrics) -> Self {
        Self { metrics }
    }
}

#[async_trait]
impl MiddlewareBuilder<RpcMethod, CallRequest, CallResult> for SubmissionTrackerMiddleware {
    async fn build(
        method: &RpcMethod,
        extensions: &TypeRegistryRef,
    ) -> Option<Box<dyn Middleware<CallRequest, CallResult>>> {
        if !method.track_submissions {
            return None;
        }

        let metrics = get_rpc_metrics(extensions).await;
        Some(Box::new(Self::new(metrics)))
    }
}

#[async_trait]
impl Middleware<CallRequest, CallResult> for SubmissionTrackerMiddleware {
    async fn call(
        &self,
        request: CallRequest,
        context: TypeRegistry,
        next: NextFn<CallRequest, CallResult>,
    ) -> CallResult {
        async move {
            let method = request.method.clone();
            let result = next(request, context).await;

            match &result {
                Ok(_) => self.metrics.submission_accepted(&method),
                Err(err) => self.metrics.submission_rejected(&method, err.code()),
            }

            result
        }
        .with_context(TRACER.context("submission_tracker"))
        .await
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt as _;
    use jsonrpsee::types::{ErrorObjectOwned, ErrorObject};
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn build_disabled_without_track_submissions() {
        let ext = crate::extensions::ExtensionsConfig::default()
            .create_registry()
            .await
            .expect("Failed to create registry");

        let middleware = SubmissionTrackerMiddleware::build(
            &RpcMethod {
                method: "foo".to_string(),
                cache: None,
                params: vec![],
                response: None,
                delay_ms: None,
                track_submissions: false,
                rate_limit_weight: 1,
            },
            &ext,
        )
        .await;
        assert!(middleware.is_none());
    }

    #[tokio::test]
    async fn build_enabled_with_track_submissions() {
        let ext = crate::extensions::ExtensionsConfig::default()
            .create_registry()
            .await
            .expect("Failed to create registry");

        let middleware = SubmissionTrackerMiddleware::build(
            &RpcMethod {
                method: "author_submitExtrinsic".to_string(),
                cache: None,
                params: vec![],
                response: None,
                delay_ms: None,
                track_submissions: true,
                rate_limit_weight: 1,
            },
            &ext,
        )
        .await;
        assert!(middleware.is_some());
    }

    #[tokio::test]
    async fn passes_through_accepted_result() {
        let middleware = SubmissionTrackerMiddleware::new(RpcMetrics::noop());

        let res = middleware
            .call(
                CallRequest::new("author_submitExtrinsic", vec![json!("0xaa")]),
                Default::default(),
                Box::new(move |_, _| async move { Ok(json!("0xhash")) }.boxed()),
            )
            .await;
        assert_eq!(res.unwrap(), json!("0xhash"));
    }

    #[tokio::test]
    async fn passes_through_rejected_result() {
        let middleware = SubmissionTrackerMiddleware::new(RpcMetrics::noop());

        let err: ErrorObjectOwned = ErrorObject::owned(1013, "Already Imported", None::<()>);
        let res = middleware
            .call(
                CallRequest::new("author_submitExtrinsic", vec![json!("0xaa")]),
                Default::default(),
                Box::new(move |_, _| async move { Err(err) }.boxed()),
            )
            .await;
        assert_eq!(res.unwrap_err().code(), 1013);
    }
}
