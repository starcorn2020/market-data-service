use std::ops::Deref;

use marketdata_types::BookMessage;

/// Owned wrapper around a [`BookMessage`] returned from
/// [`FeedSubscriber::receive`](crate::FeedSubscriber::receive).
///
/// Mirrors the iceoryx2 `Sample` shape: derefs to the inner message so
/// you can write `sample.gateway_seq` or `sample.best_bid()` directly. If
/// you need to retain the message after the sample is dropped, copy it
/// (`BookMessage` is `Copy`) or call [`into_inner`](Self::into_inner).
pub struct FeedSample {
    inner: BookMessage,
}

impl FeedSample {
    pub(crate) fn new(msg: BookMessage) -> Self {
        Self { inner: msg }
    }

    /// Consumes the sample and returns the owned message.
    pub fn into_inner(self) -> BookMessage {
        self.inner
    }
}

impl Deref for FeedSample {
    type Target = BookMessage;
    fn deref(&self) -> &BookMessage {
        &self.inner
    }
}

impl AsRef<BookMessage> for FeedSample {
    fn as_ref(&self) -> &BookMessage {
        &self.inner
    }
}
