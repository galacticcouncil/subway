use tokio::sync::{watch, RwLock};

pub struct ValueHandle<T> {
    inner: RwLock<watch::Receiver<Option<T>>>,
}

impl<T: Clone> ValueHandle<T> {
    pub fn new(value: watch::Receiver<Option<T>>) -> Self {
        Self {
            inner: RwLock::new(value),
        }
    }

    /// Waits for a value to become available. Returns `None` if the producer side has
    /// shut down for good (e.g. the upstream chain doesn't support the subscription this
    /// value is derived from) and so no value will ever arrive.
    pub async fn read(&self) -> Option<T> {
        let read_guard = self.inner.read().await;
        let val = (*read_guard).borrow().to_owned();
        drop(read_guard);

        if let Some(val) = val {
            return Some(val);
        }

        let mut write_guard = self.inner.write().await;

        loop {
            if write_guard.changed().await.is_err() {
                // producer dropped, no value will ever arrive
                return None;
            }
            if let Some(value) = (*write_guard).borrow().to_owned() {
                return Some(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn awaits_value() {
        let (value_tx, value_rx) = tokio::sync::watch::channel::<Option<u32>>(None);

        let value_handle = ValueHandle::new(value_rx);

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            value_tx.send_replace(Some(1));
            tokio::time::sleep(Duration::from_millis(1)).await;
            value_tx.send_replace(None);
            tokio::time::sleep(Duration::from_millis(1)).await;
            value_tx.send_replace(Some(2));
        });

        tokio::spawn(async move {
            assert_eq!(value_handle.read().await, Some(1));
            // after 2 millis value is none but it will await for next value
            tokio::time::sleep(Duration::from_millis(2)).await;
            assert_eq!(value_handle.read().await, Some(2));
            assert_eq!(value_handle.read().await, Some(2));
        })
        .await
        .unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn returns_none_when_producer_is_dropped() {
        let (value_tx, value_rx) = tokio::sync::watch::channel::<Option<u32>>(None);
        let value_handle = ValueHandle::new(value_rx);

        drop(value_tx);

        assert_eq!(value_handle.read().await, None);
    }

    #[tokio::test]
    async fn returns_none_when_producer_is_dropped_while_waiting() {
        let (value_tx, value_rx) = tokio::sync::watch::channel::<Option<u32>>(None);
        let value_handle = ValueHandle::new(value_rx);

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            drop(value_tx);
        });

        assert_eq!(value_handle.read().await, None);
        handle.await.unwrap();
    }
}
