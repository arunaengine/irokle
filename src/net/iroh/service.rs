// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded served-plan admission. Waiting descriptors hold no frontier.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::PeerId;

const WAITING: usize = 128;
const ACTIVE: usize = 16;
const PER_PEER: usize = 2;

pub(super) struct PlanQueue {
    descriptors: Arc<Semaphore>,
    active: Arc<Semaphore>,
    peers: Mutex<BTreeMap<PeerId, (Arc<Semaphore>, usize)>>,
}

pub(super) struct PlanWait {
    queue: Arc<PlanQueue>,
    peer: PeerId,
    share: Arc<Semaphore>,
    _descriptor: OwnedSemaphorePermit,
}

pub(super) struct PlanPermit {
    _active: OwnedSemaphorePermit,
    _share: OwnedSemaphorePermit,
    _wait: PlanWait,
}

impl Default for PlanQueue {
    fn default() -> Self {
        Self {
            descriptors: Arc::new(Semaphore::new(WAITING)),
            active: Arc::new(Semaphore::new(ACTIVE)),
            peers: Mutex::default(),
        }
    }
}

impl PlanQueue {
    #[cfg(test)]
    pub(super) fn counts(&self) -> (usize, usize) {
        (
            ACTIVE - self.active.available_permits(),
            WAITING - self.descriptors.available_permits(),
        )
    }

    pub(super) fn register(self: &Arc<Self>, peer: PeerId) -> io::Result<PlanWait> {
        let descriptor = Arc::clone(&self.descriptors)
            .try_acquire_owned()
            .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "sync plan queue is full"))?;
        let mut peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (share, users) = peers
            .entry(peer)
            .or_insert_with(|| (Arc::new(Semaphore::new(PER_PEER)), 0));
        *users += 1;
        Ok(PlanWait {
            queue: Arc::clone(self),
            peer,
            share: Arc::clone(share),
            _descriptor: descriptor,
        })
    }

    pub(super) fn close(&self) {
        self.descriptors.close();
        self.active.close();
        let peers = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (share, _) in peers.values() {
            share.close();
        }
    }
}

impl PlanWait {
    pub(super) async fn enter(self) -> io::Result<PlanPermit> {
        let closed = |_| io::Error::new(io::ErrorKind::Interrupted, "sync plan queue closed");
        let share = Arc::clone(&self.share)
            .acquire_owned()
            .await
            .map_err(closed)?;
        let active = Arc::clone(&self.queue.active)
            .acquire_owned()
            .await
            .map_err(closed)?;
        Ok(PlanPermit {
            _active: active,
            _share: share,
            _wait: self,
        })
    }
}

impl Drop for PlanWait {
    fn drop(&mut self) {
        let mut peers = self
            .queue
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, users)) = peers.get_mut(&self.peer) {
            *users -= 1;
            if *users == 0 {
                peers.remove(&self.peer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(n: u8) -> PeerId {
        PeerId::from_bytes([n; 32])
    }

    #[tokio::test]
    async fn queued_order() {
        let queue = Arc::new(PlanQueue::default());
        let mut active = Vec::new();
        for n in 0..ACTIVE {
            active.push(
                queue
                    .register(peer(n as u8))
                    .unwrap()
                    .enter()
                    .await
                    .unwrap(),
            );
        }
        let mut waiting = (ACTIVE..WAITING)
            .map(|n| Box::pin(queue.register(peer(n as u8)).unwrap().enter()))
            .collect::<Vec<_>>();
        assert_eq!(
            queue.register(peer(255)).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        for next in &mut waiting {
            tokio::task::yield_now().await;
            assert!(futures::poll!(next.as_mut()).is_pending());
        }
        for (n, mut next) in waiting.into_iter().enumerate() {
            drop(active.pop());
            tokio::task::yield_now().await;
            let permit = futures::poll!(next.as_mut()).map(Result::unwrap);
            assert!(permit.is_ready(), "goal {n} was overtaken");
            let std::task::Poll::Ready(permit) = permit else {
                unreachable!()
            };
            active.push(permit);
        }
        drop(active);
        assert!(queue.peers.lock().unwrap().is_empty());
        assert_eq!(queue.descriptors.available_permits(), WAITING);
        assert_eq!(queue.active.available_permits(), ACTIVE);
    }

    #[tokio::test]
    async fn shares_and_cancel() {
        let queue = Arc::new(PlanQueue::default());
        let one = queue.register(peer(1)).unwrap().enter().await.unwrap();
        let two = queue.register(peer(1)).unwrap().enter().await.unwrap();
        let mut third = Box::pin(queue.register(peer(1)).unwrap().enter());
        assert!(futures::poll!(third.as_mut()).is_pending());
        let other = queue.register(peer(2)).unwrap().enter().await.unwrap();
        assert_eq!(queue.active.available_permits(), ACTIVE - 3);
        drop(third);
        drop((one, two, other));
        assert!(queue.peers.lock().unwrap().is_empty());
        assert_eq!(queue.descriptors.available_permits(), WAITING);
    }

    #[tokio::test]
    async fn close_wakes_waiters() {
        let queue = Arc::new(PlanQueue::default());
        let one = queue.register(peer(1)).unwrap().enter().await.unwrap();
        let two = queue.register(peer(1)).unwrap().enter().await.unwrap();
        let mut waiting = Box::pin(queue.register(peer(1)).unwrap().enter());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        queue.close();
        assert_eq!(
            waiting.await.err().unwrap().kind(),
            io::ErrorKind::Interrupted
        );
        assert_eq!(queue.active.available_permits(), ACTIVE - 2);
        drop((one, two));
        assert!(queue.peers.lock().unwrap().is_empty());
        assert_eq!(queue.active.available_permits(), ACTIVE);
    }
}
