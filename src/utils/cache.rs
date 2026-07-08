use crate::middlewares::CallResult;
use blake2::{digest::Output, Digest};
use futures::future::BoxFuture;
use jsonrpsee::core::JsonValue;
use jsonrpsee::types::ErrorObjectOwned;
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug)]
pub struct CacheKey<D: Digest>(pub Output<D>);

impl<D: Digest> Clone for CacheKey<D> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<D: Digest> CacheKey<D> {
    pub fn new(method: &String, params: &[JsonValue]) -> Self {
        let mut hasher = D::new();
        hasher.update(method.as_bytes());
        for p in params {
            hasher.update(p.to_string().as_bytes());
        }

        Self(hasher.finalize())
    }
}

impl<D: Digest> PartialEq for CacheKey<D> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_slice() == other.0.as_slice()
    }
}

impl<D: Digest> Eq for CacheKey<D> {}

impl<D: Digest> std::hash::Hash for CacheKey<D> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_slice().hash(state);
    }
}

#[derive(Clone, Debug)]
pub enum CacheValue {
    Pending(watch::Receiver<Option<Result<JsonValue, ErrorObjectOwned>>>),
    Value(JsonValue),
}

#[derive(Clone)]
pub struct Cache<D: Digest> {
    cache: moka::future::Cache<CacheKey<D>, CacheValue>,
    // Serializes the "check empty, then claim it as pending" step in `get_or_insert_with`
    // so concurrent first-time requests for the same cold key can't all become fetch
    // "leaders" at once. Only ever held across a couple of fast in-memory moka
    // operations, never across the actual upstream fetch.
    claim_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl<D: Digest + 'static> Cache<D> {
    pub fn new(size: NonZeroUsize, ttl: Option<Duration>) -> Self {
        let size = size.get();
        let mut builder = moka::future::Cache::<CacheKey<D>, CacheValue>::builder()
            .max_capacity(size as u64)
            .initial_capacity(size);

        if let Some(duration) = ttl {
            builder = builder.time_to_live(duration);
        }

        let cache = builder.build();

        Self {
            cache,
            claim_lock: Default::default(),
        }
    }

    pub async fn get(&self, key: &CacheKey<D>) -> Option<JsonValue> {
        match self.cache.get(key).await {
            Some(CacheValue::Value(value)) => Some(value),
            Some(CacheValue::Pending(mut rx)) => {
                let value = rx.borrow();
                if value.is_some() {
                    return value.clone().unwrap().ok();
                }
                drop(value);
                let _ = rx.changed().await;
                let value = rx.borrow();
                if value.is_some() {
                    value.clone().unwrap().ok()
                } else {
                    tracing::error!("Cache: Unreachable code");
                    None
                }
            }
            None => None,
        }
    }

    pub async fn insert(&self, key: CacheKey<D>, value: JsonValue) {
        self.cache.insert(key, CacheValue::Value(value)).await;
    }

    pub async fn get_or_insert_with<F>(&self, key: CacheKey<D>, f: F) -> CallResult
    where
        F: FnOnce() -> BoxFuture<'static, CallResult>,
    {
        let fetch = || async {
            let (tx, rx) = watch::channel(None);
            self.cache.insert(key.clone(), CacheValue::Pending(rx)).await;
            let value = f().await;
            let _ = tx.send(Some(value.clone()));
            match &value {
                Ok(value) => {
                    self.cache.insert(key.clone(), CacheValue::Value(value.clone())).await;
                }
                Err(_) => {
                    self.cache.remove(&key).await;
                }
            };
            value
        };

        // Returns `Some` with the resolved value once the pending fetch we're watching
        // completes, or `None` if it got canceled without ever resolving (the caller
        // should then try to become the new leader and fetch again).
        async fn wait_for_pending(rx: &mut watch::Receiver<Option<CallResult>>) -> Option<CallResult> {
            {
                // limit the scope of value
                let value = rx.borrow();
                if value.is_some() {
                    return value.clone();
                }
            }

            let _ = rx.changed().await;

            let value = rx.borrow();
            value.clone()
        }

        match self.cache.get(&key).await {
            Some(CacheValue::Value(value)) => return Ok(value),
            Some(CacheValue::Pending(mut rx)) => {
                if let Some(value) = wait_for_pending(&mut rx).await {
                    return value;
                }
                // initial fetch got canceled; fall through to (re-)claim it below
            }
            None => {}
        }

        // Serialize check-then-claim: without this, two concurrent first-time requests
        // for the same cold key would both see no entry and both call `fetch()`,
        // duplicating the upstream call this method exists to deduplicate.
        let _guard = self.claim_lock.lock().await;

        match self.cache.get(&key).await {
            Some(CacheValue::Value(value)) => Ok(value),
            Some(CacheValue::Pending(mut rx)) => match wait_for_pending(&mut rx).await {
                Some(value) => value,
                None => fetch().await,
            },
            None => fetch().await,
        }
    }

    pub async fn remove(&self, key: &CacheKey<D>) {
        self.cache.remove(key).await;
    }

    pub async fn sync(&self) {
        self.cache.run_pending_tasks().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use jsonrpsee::types::error::reject_too_big_request;
    use serde_json::json;

    #[tokio::test]
    async fn get_insert_remove() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        assert_eq!(cache.get(&key).await, None);

        cache.insert(key.clone(), json!("value")).await;

        assert_eq!(cache.get(&key).await, Some(json!("value")));

        cache.remove(&key).await;

        assert_eq!(cache.get(&key).await, None);
    }

    #[tokio::test]
    async fn get_or_insert_with_basic() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h1 = tokio::spawn(async move {
            let value = cache2
                .get_or_insert_with(key2.clone(), || {
                    async move {
                        let _ = rx.await;
                        Ok(json!("value"))
                    }
                    .boxed()
                })
                .await;
            assert_eq!(value, Ok(json!("value")));
        });

        tokio::task::yield_now().await;

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h2 = tokio::spawn(async move {
            let value = cache2
                .get_or_insert_with(key2, || {
                    async {
                        panic!();
                    }
                    .boxed()
                })
                .await;
            assert_eq!(value, Ok(json!("value")));
        });

        tokio::task::yield_now().await;

        tx.send(()).unwrap();

        h1.await.unwrap();
        h2.await.unwrap();

        assert_eq!(cache.get(&key).await, Some(json!("value")));
    }

    #[tokio::test]
    async fn get_or_insert_with_handle_canceled_request() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h1 = tokio::spawn(async move {
            let _ = cache2
                .get_or_insert_with(key2.clone(), || {
                    async move {
                        let _ = rx.await;
                        panic!();
                    }
                    .boxed()
                })
                .await;
            unreachable!();
        });

        tokio::task::yield_now().await;

        let cache2 = cache.clone();
        let key2 = key.clone();
        let h2 = tokio::spawn(async move {
            let value = cache2
                .get_or_insert_with(key2, || async { Ok(json!("value")) }.boxed())
                .await;
            assert_eq!(value, Ok(json!("value")));
        });

        tokio::task::yield_now().await;

        h1.abort(); // first request failed for whatever reason

        h1.await.unwrap_err();
        h2.await.unwrap(); // second request should still work

        assert_eq!(cache.get(&key).await, Some(json!("value")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn get_or_insert_with_dedupes_concurrent_cold_requests() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(10).unwrap(), None);
        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        let fetch_count = Arc::new(AtomicUsize::new(0));
        let concurrency = 20;
        let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));

        let tasks = (0..concurrency)
            .map(|_| {
                let cache = cache.clone();
                let key = key.clone();
                let fetch_count = fetch_count.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    // synchronize all tasks so they enter `get_or_insert_with` for the
                    // same cold key at the same time, instead of relying on scheduling luck
                    barrier.wait().await;
                    cache
                        .get_or_insert_with(key, move || {
                            let fetch_count = fetch_count.clone();
                            async move {
                                fetch_count.fetch_add(1, Ordering::SeqCst);
                                // give every other task a chance to (incorrectly) also
                                // become a fetch "leader" before this one finishes
                                tokio::time::sleep(Duration::from_millis(20)).await;
                                Ok(json!("value"))
                            }
                            .boxed()
                        })
                        .await
                })
            })
            .collect::<Vec<_>>();

        for task in tasks {
            assert_eq!(task.await.unwrap(), Ok(json!("value")));
        }

        assert_eq!(
            fetch_count.load(Ordering::SeqCst),
            1,
            "expected only one upstream fetch for concurrent cold requests"
        );
    }

    #[tokio::test]
    async fn get_or_insert_with_error() {
        let cache = Cache::<blake2::Blake2b512>::new(NonZeroUsize::new(1).unwrap(), None);

        let key = CacheKey::<blake2::Blake2b512>::new(&"key".to_string(), &[]);

        let value = cache
            .get_or_insert_with(key.clone(), || async move { Err(reject_too_big_request(100)) }.boxed())
            .await;
        assert_eq!(value, Err(reject_too_big_request(100)));

        assert_eq!(cache.get(&key).await, None);
    }
}
