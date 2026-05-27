use std::time::Duration;

use feed_sim::{FeedSubscriber, SubscriberConfig};
use marketdata_types::BookMessage;

use crate::BoxError;
use crate::config::UpstreamConfig;

use super::Upstream;

/// 真实 (默认) [`Upstream`] 实作 —— 包 [`FeedSubscriber`], 把 `feed_sim` 类型
/// 封死在本档内。换上游 (例如真实 iceoryx2) 时, 仅需新增同级 `iceoryx2.rs`
/// 实现 [`Upstream`] trait, ingest / service 装配端 0 改动。
pub struct FeedSimUpstream {
    inner: FeedSubscriber,
}

impl FeedSimUpstream {
    /// 构造并立即启动上游背景执行緒。
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
            // `FeedSample` 在此处 deref 后值拷贝 (~408 bytes, `Copy`)。跨线程
            // 传递必须传值 —— 引用会被 sample 生命周期绑住, 无法跨 ingest /
            // service 边界。值拷贝在 1k msg/s 默认速率下 ~ 408 KB/s, 无瓶颈。
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
