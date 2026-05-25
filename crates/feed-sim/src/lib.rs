//! Simulated upstream feed.
//!
//! This crate stands in for the real shared-memory feed gateway. Its public
//! API mirrors the production iceoryx2 subscriber so that swapping the real
//! source in later requires changing only the type, not your service:
//!
//! ```no_run
//! use std::time::Duration;
//! use feed_sim::{FeedSubscriber, SubscriberConfig};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let sub = FeedSubscriber::new(SubscriberConfig::from_env()?)?;
//! let poll_interval = Duration::from_millis(100);
//!
//! while sub.wait(poll_interval).is_ok() {
//!     loop {
//!         match sub.receive()? {
//!             Some(sample) => {
//!                 // sample derefs to &BookMessage
//!                 let _ = sample.gateway_seq;
//!             }
//!             None => break,
//!         }
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! # Semantics
//!
//! - [`FeedSubscriber::new`] starts a background generator thread that paces
//!   a deterministic stream of [`BookMessage`](marketdata_types::BookMessage)s
//!   into a bounded internal buffer.
//! - [`FeedSubscriber::receive`] is non-blocking: `Ok(Some(sample))` if a
//!   message is buffered, `Ok(None)` otherwise (including "upstream done,
//!   buffer drained"). It does not report shutdown on its own.
//! - [`FeedSubscriber::wait`] sleeps for the given duration and returns
//!   `Err(())` once the upstream is fully drained — same shutdown signal
//!   you would get from the real subscriber. Drive your poll loop off
//!   this, not off `receive`.
//! - If you don't call [`FeedSubscriber::receive`] fast enough, the buffer
//!   fills and the simulator **drops messages**. That mirrors what the real
//!   subscriber does on overflow.

#![warn(missing_docs)]

mod config;
mod error;
mod generator;
mod rng;
mod sample;
mod subscriber;

pub use config::{Pacing, SubscriberConfig};
pub use error::{FeedSimError, Result};
pub use sample::FeedSample;
pub use subscriber::FeedSubscriber;

// Re-export the type the consumer cares about for one-import ergonomics.
pub use marketdata_types::BookMessage;
