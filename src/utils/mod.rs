mod cache;
mod type_registry;

pub use cache::*;
pub use type_registry::*;

pub mod errors {
    use jsonrpsee::types::{
        error::{
            CALL_EXECUTION_FAILED_CODE, INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, INVALID_PARAMS_CODE,
            INVALID_PARAMS_MSG,
        },
        ErrorObjectOwned,
    };

    pub fn invalid_params<T: ToString>(msg: T) -> ErrorObjectOwned {
        ErrorObjectOwned::owned(INVALID_PARAMS_CODE, INVALID_PARAMS_MSG, Some(msg.to_string()))
    }

    pub fn failed<T: ToString>(msg: T) -> ErrorObjectOwned {
        ErrorObjectOwned::owned(
            CALL_EXECUTION_FAILED_CODE,
            "Call Execution Failed",
            Some(msg.to_string()),
        )
    }

    pub fn internal_error<T: ToString>(msg: T) -> ErrorObjectOwned {
        ErrorObjectOwned::owned(INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, Some(msg.to_string()))
    }

    pub fn map_error(err: jsonrpsee::core::client::Error) -> ErrorObjectOwned {
        use jsonrpsee::core::client::Error::*;
        match err {
            Call(e) => e,
            // These can carry arbitrary underlying details (DNS/TLS/connection failures,
            // or a fragment of a malformed upstream response body) that could reveal
            // internal network topology to the client. Log the details, return a generic
            // message. Other variants (e.g. `RequestTimeout`) have fixed, safe messages
            // and pass through as before.
            err @ (Transport(_) | RestartNeeded(_) | ParseError(_)) => {
                tracing::debug!("Upstream request failed: {err:?}");
                internal_error("Upstream request failed")
            }
            x => internal_error(x),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use jsonrpsee::core::client::Error as ClientError;

        #[test]
        fn map_error_redacts_transport_details() {
            let err = map_error(ClientError::Transport(anyhow::anyhow!(
                "connect to 10.0.0.42:9944 failed: connection refused"
            )));
            assert_eq!(err.message(), INTERNAL_ERROR_MSG);
            let data = err.data().unwrap().to_string();
            assert!(!data.contains("10.0.0.42"), "leaked internal address: {data}");
            assert_eq!(data, "\"Upstream request failed\"");
        }

        #[test]
        fn map_error_preserves_safe_messages() {
            let err = map_error(ClientError::RequestTimeout);
            assert_eq!(err.data().unwrap().to_string(), "\"Request timeout\"");
        }
    }
}

pub mod telemetry {
    use jsonrpsee::{types::error::ErrorCode, types::ErrorObjectOwned};
    use opentelemetry::{
        global::{self, BoxedSpan},
        trace::{get_active_span, Status, TraceContextExt, Tracer as _},
        Context, KeyValue,
    };
    use std::borrow::Cow;

    #[derive(Clone, Copy, Debug)]
    pub struct Tracer(&'static str);

    impl Tracer {
        pub const fn new(name: &'static str) -> Self {
            Self(name)
        }

        pub fn span(&self, span_name: impl Into<Cow<'static, str>>) -> BoxedSpan {
            global::tracer(self.0).start(span_name)
        }

        pub fn context(&self, span_name: impl Into<Cow<'static, str>>) -> Context {
            let span = self.span(span_name);
            Context::current_with_span(span)
        }

        pub fn span_ok(&self) {
            get_active_span(|span| {
                span.set_status(Status::Ok);
            });
        }

        pub fn span_error(&self, err: &ErrorObjectOwned) {
            get_active_span(|span| {
                span.set_status(Status::error(err.message().to_string()));
                span.set_attribute(KeyValue::new("error.type", format!("{}", ErrorCode::from(err.code()))));
                span.set_attribute(KeyValue::new("error.msg", err.message().to_string()));
                span.set_attribute(KeyValue::new("error.stack", format!("{}", err)));
            });
        }
    }
}
