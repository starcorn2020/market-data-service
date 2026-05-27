//! Upstream feed abstraction layer.
//!
//! The **only** place in the whole service crate that may `use
//! feed_sim::*` — every cross-module call goes through the [`Upstream`]
//! trait, sealing vendor types inside this module:
//!
//! 1. Anything that depends on `Upstream` (`ingest.rs` / mock tests)
//!    cannot see `feed_sim::FeedSubscriber`.
//! 2. To swap `feed-sim` for a real iceoryx2 (or any other upstream),
//!    just add `upstream/iceoryx2.rs` implementing the `Upstream` trait;
//!    nothing else changes.

use std::time::Duration;

use marketdata_types::BookMessage;

use crate::BoxError;

mod feed_sim;
mod mock;

pub use feed_sim::FeedSimUpstream;
pub use mock::{MockHandle, MockUpstream, make_book};

/// The ingest path's sole dependency on the upstream feed.
///
/// Implementers must guarantee:
///
/// - `receive` is a **non-blocking** try-recv (`Ok(None)` means "no data
///   right now"; it does **not** mean the stream has ended).
/// - `wait(d)` is the only shutdown signal channel: `Err(())` = upstream
///   fully drained + closed; only then does the ingest loop exit.
/// - Methods take `&self`; the caller `move`s the upstream into the
///   ingest thread for exclusive use.
pub trait Upstream: Send {
    /// Non-blocking try-recv of a single message. `Ok(None)` = buffer is
    /// currently empty; `Ok(Some(_))` = got one message.
    fn receive(&self) -> Result<Option<BookMessage>, BoxError>;

    /// Blocks for `duration` and returns; `Err(())` = upstream is fully
    /// drained and closed = the only legitimate end signal.
    //
    // `Result<(), ()>` matches the contract baked into
    // `feed_sim::FeedSubscriber::wait` — `Err(())` is feed-sim's sole
    // legitimate end signal (no error variant to distinguish); switching
    // to a custom error type would break the feed-sim boundary
    // correspondence. clippy `result_unit_err` is ignored here.
    #[allow(clippy::result_unit_err)]
    fn wait(&self, duration: Duration) -> Result<(), ()>;

    /// Cumulative count of messages generated / buffered (for sanity check).
    fn total_generated(&self) -> u64;
}
