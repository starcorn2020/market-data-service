//! `feed_sim::FeedSubscriber` 的 [`Upstream`] adapter。
//!
//! 整个 service crate 里**唯一** `use feed_sim::*` 的文件，是 I4 不变量的密封点。

use std::time::Duration;

use anyhow::Context;
use feed_sim::{FeedSubscriber, SubscriberConfig};
use marketdata_types::BookMessage;

use crate::config::UpstreamConfig;

use super::Upstream;

/// `feed_sim::FeedSubscriber` 的 newtype 包装。
///
/// 注意 `FeedSubscriber` 是 `!Sync`（内部持有 `std::sync::mpsc::Receiver`），
/// 因此 `FeedSimUpstream` 也是 `!Sync`，**只能由单一线程独占**——这正好契合
/// GUIDELINE §3.5 的"整个 service 只能有一个 ingest 点呼叫 receive"。
pub struct FeedSimUpstream {
    inner: FeedSubscriber,
}

impl FeedSimUpstream {
    /// 构造并立即启动上游背景执行緒。
    pub fn new(cfg: UpstreamConfig) -> anyhow::Result<Self> {
        let sc: SubscriberConfig = cfg.into();
        let inner = FeedSubscriber::new(sc)
            .map_err(|e| anyhow::anyhow!("{e:?}"))
            .context("feed-sim subscriber init failed")?;
        Ok(Self { inner })
    }
}

impl Upstream for FeedSimUpstream {
    fn receive(&self) -> anyhow::Result<Option<BookMessage>> {
        match self.inner.receive() {
            // FeedSample 在此处 deref 然后值拷贝（408 bytes / Copy）。
            // 跨线程传递必须传值，引用会被 sample 生命周期绑住。
            Ok(Some(sample)) => Ok(Some(*sample)),
            Ok(None) => Ok(None),
            Err(e) => Err(anyhow::anyhow!("feed-sim receive failed: {e:?}")),
        }
    }

    fn wait(&self, duration: Duration) -> Result<(), ()> {
        self.inner.wait(duration)
    }

    fn total_generated(&self) -> u64 {
        self.inner.total_generated()
    }
}
