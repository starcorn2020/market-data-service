use std::time::Duration;

use feed_sim::{FeedSubscriber, SubscriberConfig};
use marketdata_types::BookMessage;

use crate::BoxError;
use crate::config::UpstreamConfig;

use super::Upstream;

/// The real (default) [`Upstream`] implementation — wraps
/// [`FeedSubscriber`] to seal `feed_sim` types inside this file. When
/// swapping the upstream (e.g. real iceoryx2), just add a sibling
/// `iceoryx2.rs` implementing the [`Upstream`] trait; ingest and the
/// service assembly site change by zero.
pub struct FeedSimUpstream {
    inner: FeedSubscriber,
}

impl FeedSimUpstream {
    /// Construct and immediately start the upstream background thread.
    pub fn new(cfg: UpstreamConfig) -> Result<Self, BoxError> {
        let sc: SubscriberConfig = cfg.into();
        let inner = FeedSubscriber::new(sc).map_err(|e| -> BoxError {
            format!("feed-sim subscriber init failed: {e:?}").into()
        })?;
        Ok(Self { inner })
    }
}

impl Upstream for FeedSimUpstream {
    fn receive(&self) -> Result<Option<BookMessage>, BoxError> {
        match self.inner.receive() {
            // `FeedSample` is deref'd and value-copied here (~408 bytes,
            // `Copy`). Cross-thread transfer must be by value — a
            // reference would be tied to the sample's lifetime and could
            // not cross the ingest / service boundary. At the default
            // 1k msg/s, value copies amount to ~408 KB/s — no bottleneck.
            Ok(Some(sample)) => Ok(Some(*sample)),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("feed-sim receive failed: {e:?}").into()),
        }
    }

    fn wait(&self, duration: Duration) -> Result<(), ()> {
        self.inner.wait(duration)
    }

    fn total_generated(&self) -> u64 {
        self.inner.total_generated()
    }
}
