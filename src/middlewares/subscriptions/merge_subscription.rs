use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use blake2::Blake2b512;
use jsonrpsee::{core::JsonValue, SubscriptionMessage};
use opentelemetry::trace::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::{
    config::MergeStrategy,
    extensions::{client::Client, merge_subscription::MergeSubscription},
    middlewares::{
        Middleware, MiddlewareBuilder, NextFn, RpcSubscription, SubscriptionRequest, SubscriptionResult, TRACER,
    },
    utils::{errors, CacheKey, TypeRegistry, TypeRegistryRef},
};

#[derive(Serialize, Deserialize, Debug)]
struct StorageChanges {
    block: String,
    changes: Vec<(String, Option<String>)>,
}

fn merge_storage_changes(current_value: JsonValue, new_value: JsonValue) -> Result<JsonValue, serde_json::Error> {
    let mut current = serde_json::from_value::<StorageChanges>(current_value)?;
    let StorageChanges { block, changes } = serde_json::from_value::<StorageChanges>(new_value)?;

    let changed_keys = changes.iter().map(|(key, _)| key).collect::<BTreeSet<_>>();

    // replace block hash
    current.block = block;
    // remove changed keys
    current.changes.retain(|(key, _)| !changed_keys.contains(key));
    // append new changes
    current.changes.extend(changes);

    serde_json::to_value(current)
}

fn handle_value_change(
    merge_strategy: MergeStrategy,
    current_value: Option<JsonValue>,
    new_value: JsonValue,
) -> JsonValue {
    if let Some(current_value) = current_value {
        match merge_strategy {
            MergeStrategy::Replace => new_value,
            MergeStrategy::MergeStorageChanges => {
                merge_storage_changes(current_value, new_value.clone()).unwrap_or(new_value)
            }
        }
    } else {
        new_value
    }
}

type UpstreamSubscription = broadcast::Sender<SubscriptionMessage>;

pub struct MergeSubscriptionMiddleware {
    client: Arc<Client>,
    merge_strategy: MergeStrategy,
    keep_alive_seconds: u64,
    upstream_subs: Arc<RwLock<HashMap<CacheKey<Blake2b512>, UpstreamSubscription>>>,
    current_values: Arc<RwLock<HashMap<CacheKey<Blake2b512>, JsonValue>>>,
    // Serializes "check absent, then create+register" so concurrent subscribers for a
    // not-yet-existing key don't each independently create their own duplicate upstream
    // subscription (only the fast, in-memory recheck is repeated under this lock; the
    // slow part -- the actual `client.subscribe` round-trip -- still only runs once).
    create_lock: Arc<Mutex<()>>,
}

impl MergeSubscriptionMiddleware {
    pub fn new(client: Arc<Client>, merge_strategy: MergeStrategy, keep_alive_seconds: Option<u64>) -> Self {
        Self {
            client,
            merge_strategy,
            keep_alive_seconds: keep_alive_seconds.unwrap_or(60), // 60s
            upstream_subs: Arc::new(RwLock::new(HashMap::new())),
            current_values: Arc::new(RwLock::new(HashMap::new())),
            create_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Looks up an existing upstream subscription for `key` and, if found, subscribes to
    /// it immediately while still holding the map's read lock. This must happen under the
    /// lock (rather than returning the sender for the caller to subscribe to later) so it
    /// can't race with the forwarder task's own atomic receiver-count check when deciding
    /// whether to retire an idle subscription -- see the `interval.tick()`/`subscription.next()`
    /// arms below.
    async fn find_existing_subscription(
        &self,
        key: &CacheKey<Blake2b512>,
    ) -> Option<broadcast::Receiver<SubscriptionMessage>> {
        let subs = self.upstream_subs.read().await;
        subs.get(key).map(|tx| tx.subscribe())
    }

    async fn get_upstream_subscription(
        &self,
        key: CacheKey<Blake2b512>,
        subscribe: String,
        params: Vec<JsonValue>,
        unsubscribe: String,
    ) -> Result<
        Box<dyn FnOnce() -> broadcast::Receiver<SubscriptionMessage> + Sync + Send + 'static>,
        jsonrpsee::core::client::Error,
    > {
        if let Some(rx) = self.find_existing_subscription(&key).await {
            tracing::trace!("Found existing upstream subscription for {}", &subscribe);
            return Ok(Box::new(move || rx));
        }

        // Serialize check-then-create: without this, two concurrent subscribers landing
        // here for the same not-yet-existing key would each create their own upstream
        // subscription, and whichever inserts into `upstream_subs` last would silently
        // orphan the other's forwarder task from the map.
        let _guard = self.create_lock.lock().await;

        if let Some(rx) = self.find_existing_subscription(&key).await {
            tracing::trace!(
                "Found existing upstream subscription for {} (created concurrently)",
                &subscribe
            );
            return Ok(Box::new(move || rx));
        }

        tracing::trace!("Create new upstream subscription for {}", &subscribe);

        let mut subscription = self.client.subscribe(&subscribe, params.clone(), &unsubscribe).await?;

        let (tx, _) = broadcast::channel(1024);

        self.upstream_subs.write().await.insert(key.clone(), tx.clone());

        let merge_strategy = self.merge_strategy;
        let client = self.client.clone();
        let upstream_subs = self.upstream_subs.clone();
        let current_values = self.current_values.clone();
        let keep_alive_seconds = self.keep_alive_seconds;

        let subscribe = Box::new(move || {
            let rx = tx.subscribe();

            tokio::spawn(async move {
                // this ticker acts like a waker to help cleanup subscriptions
                let mut interval = tokio::time::interval(Duration::from_secs(keep_alive_seconds));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                interval.reset();

                // Atomically checks whether there are still no receivers and, if so,
                // retires this subscription by removing it from the map. Must hold the
                // write lock across the check-and-remove so a new subscriber's
                // `find_existing_subscription` (which subscribes while holding the read
                // lock) can't race with this decision: either they see this entry and
                // subscribe before we retire it (receiver_count is then > 0, so we don't
                // retire), or we retire first and they see no entry and create a fresh
                // subscription instead. Without this, a new subscriber could be handed a
                // receiver for a `tx` we're about to drop, and would then hang forever
                // with no error since they still hold their own sender clone.
                let try_retire = || {
                    let upstream_subs = upstream_subs.clone();
                    let tx = tx.clone();
                    let key = key.clone();
                    async move {
                        let mut subs = upstream_subs.write().await;
                        if tx.receiver_count() == 0 {
                            subs.remove(&key);
                            true
                        } else {
                            false
                        }
                    }
                };

                loop {
                    tokio::select! {
                        resp = subscription.next() => {
                            if tx.receiver_count() == 0 && try_retire().await {
                                break;
                            }

                            interval.reset();

                            if let Some(Ok(value)) = resp {
                                // update current value
                                let current_value = current_values.read().await.get(&key).cloned();
                                current_values.write().await.insert(key.clone(), handle_value_change(merge_strategy, current_value, value.clone()));

                                if let Ok(message) = SubscriptionMessage::from_json(&value) {
                                    if let Err(err) = tx.send(message.clone()) {
                                        tracing::error!("Failed to send message: {}", err);
                                        break;
                                    }
                                }
                            } else {
                                match client.subscribe(&subscribe, params.clone(), &unsubscribe).await {
                                    Ok(new_subscription) => {
                                        subscription = new_subscription;
                                    }
                                    Err(err) => {
                                        tracing::error!("failed to resubscribe {:?}", err);
                                        break;
                                    }
                                }
                            }
                        }
                        _ = interval.tick() => {
                            if tx.receiver_count() == 0 && try_retire().await {
                                break;
                            }
                        }
                    }
                }

                upstream_subs.write().await.remove(&key);
                current_values.write().await.remove(&key);
                if let Err(err) = subscription.unsubscribe().await {
                    tracing::error!("Failed to unsubscription {:?}", err);
                }
            });

            rx
        });

        Ok(subscribe)
    }
}

#[async_trait]
impl MiddlewareBuilder<RpcSubscription, SubscriptionRequest, SubscriptionResult> for MergeSubscriptionMiddleware {
    async fn build(
        method: &RpcSubscription,
        extensions: &TypeRegistryRef,
    ) -> Option<Box<dyn Middleware<SubscriptionRequest, SubscriptionResult>>> {
        let merge_strategy = method.merge_strategy?;

        let ext = extensions.read().await;
        let client = ext.get::<Client>().expect("Client extension not found");

        let merge_subscription = ext
            .get::<MergeSubscription>()
            .expect("MergeSubscription extension not found");

        Some(Box::new(MergeSubscriptionMiddleware::new(
            client,
            merge_strategy,
            merge_subscription.config.keep_alive_seconds,
        )))
    }
}

#[async_trait]
impl Middleware<SubscriptionRequest, SubscriptionResult> for MergeSubscriptionMiddleware {
    async fn call(
        &self,
        request: SubscriptionRequest,
        _context: TypeRegistry,
        _next: NextFn<SubscriptionRequest, SubscriptionResult>,
    ) -> SubscriptionResult {
        async move {
            let key = CacheKey::new(&request.subscribe, &request.params);

            let SubscriptionRequest {
                subscribe,
                params,
                unsubscribe,
                pending_sink,
            } = request;

            let subscribe = match self
                .get_upstream_subscription(key.clone(), subscribe, params.to_owned(), unsubscribe)
                .await
            {
                Ok(subscribe) => subscribe,
                Err(err) => {
                    pending_sink.reject(errors::map_error(err)).await;
                    return Ok(());
                }
            };

            // accept pending subscription
            let sink = match pending_sink.accept().await {
                Ok(sink) => sink,
                Err(e) => {
                    tracing::trace!("Failed to accept pending subscription {e:?}");
                    return Ok(());
                }
            };

            let current_values = self.current_values.clone();

            // send any current value and broadcast new values
            tokio::spawn(async move {
                // read lock before subscribing to make sure we don't miss any value
                let read_lock = current_values.read().await;
                let mut stream = subscribe();

                // send current value if any
                if let Some(current_value) = read_lock
                    .get(&key)
                    .map(|x| SubscriptionMessage::from_json(&x).ok())
                    .unwrap_or(None)
                {
                    if let Err(e) = sink.send(current_value).await {
                        tracing::trace!("subscription sink closed {e:?}");
                        return;
                    }
                }
                drop(read_lock);

                loop {
                    tokio::select! {
                        resp = stream.recv() => {
                            match resp {
                                Ok(new_value) => {
                                    if let Err(e) = sink.send(new_value).await {
                                        tracing::trace!("subscription sink closed {e:?}");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    // remote upstream subscription failed, drop subscription
                                    tracing::trace!("subscription stream error {e}");
                                    break;
                                }
                            }
                        }
                        _ = sink.closed() => {
                            tracing::trace!("subscription sink closed");
                            break;
                        }
                    }
                }
            });

            Ok(())
        }
        .with_context(TRACER.context("merge_subscription"))
        .await
    }
}

#[test]
fn merge_storage_changes_works() {
    use serde_json::json;

    let current = json!({
        "block": "0x01",
        "changes": [
            ["1", Some("foo")],
            ["2", null]
        ]
    });
    let update = json!({
        "block": "0x02",
        "changes": [
            ["2", Some("bar")]
        ]
    });
    let current = merge_storage_changes(current, update).unwrap();
    assert_eq!(
        current,
        json!({
            "block": "0x02",
            "changes": [
                ["1", Some("foo")],
                ["2", Some("bar")]
            ]
        })
    );

    let update = json!({
        "block": "0x03",
        "changes": [
            ["1", null],
        ]
    });
    let current = merge_storage_changes(current, update).unwrap();
    assert_eq!(
        current,
        json!({
            "block": "0x03",
            "changes": [
                ["2", Some("bar")],
                ["1", null],
            ]
        })
    );

    let update = json!({
        "block": "0x03",
        "changes": [
            ["3", Some("foobar")],
        ]
    });
    let current = merge_storage_changes(current, update).unwrap();
    assert_eq!(
        current,
        json!({
            "block": "0x03",
            "changes": [
                ["2", Some("bar")],
                ["1", null],
                ["3", Some("foobar")],
            ]
        })
    );
}
