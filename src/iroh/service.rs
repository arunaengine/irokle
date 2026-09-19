// SPDX-License-Identifier: MIT OR Apache-2.0
//! Bounded served-plan admission. Waiting descriptors hold no frontier.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{PeerId, Storage, TopicId};
use std::time::{Duration, Instant};

const WAITING: usize = 128;
const ACTIVE: usize = 16;
const PER_PEER: usize = 2;

type Key = (PeerId, TopicId);
type Engine<S> = Arc<Mutex<crate::sync::SyncEngine<S>>>;

struct RunningPlan<'a, S: Storage> {
    store: &'a PlanStore<S>,
    key: Key,
    engine: &'a Engine<S>,
}

impl<S: Storage> Drop for RunningPlan<'_, S> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        let mut stored = self
            .store
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if stored
            .entries
            .get(&self.key)
            .is_some_and(|(engine, _)| Arc::ptr_eq(engine, self.engine))
        {
            stored.remove(self.key);
        }
    }
}

pub(super) struct PlanStore<S: Storage> {
    inner: Mutex<Stored<S>>,
}

struct Stored<S: Storage> {
    entries: BTreeMap<Key, (Engine<S>, Instant)>,
    expiry: BTreeSet<(Instant, Key)>,
    peers: BTreeMap<PeerId, usize>,
}

impl<S: Storage> Default for PlanStore<S> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Stored {
                entries: BTreeMap::new(),
                expiry: BTreeSet::new(),
                peers: BTreeMap::new(),
            }),
        }
    }
}

impl<S: Storage> Stored<S> {
    fn remove(&mut self, key: Key) {
        if let Some((_, expiry)) = self.entries.remove(&key) {
            self.expiry.remove(&(expiry, key));
            if let Some(count) = self.peers.get_mut(&key.0) {
                *count -= 1;
                if *count == 0 {
                    self.peers.remove(&key.0);
                }
            }
        }
    }
}

impl<S: Storage> PlanStore<S> {
    pub(super) fn with_plan<R>(
        &self,
        key: Key,
        template: &crate::sync::SyncEngine<S>,
        run: impl FnOnce(&crate::sync::SyncEngine<S>) -> crate::Result<R>,
    ) -> crate::Result<R> {
        let now = Instant::now();
        let expiry = now + Duration::from_secs(60);
        let engine = {
            let mut stored = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while let Some((at, old)) = stored.expiry.first().copied() {
                if at > now {
                    break;
                }
                let busy = stored
                    .entries
                    .get(&old)
                    .is_some_and(|(engine, _)| Arc::strong_count(engine) > 1);
                if busy {
                    stored.expiry.remove(&(at, old));
                    if let Some(entry) = stored.entries.get_mut(&old) {
                        entry.1 = expiry;
                    }
                    stored.expiry.insert((expiry, old));
                } else {
                    stored.remove(old);
                }
            }
            if let Some((engine, old)) = stored.entries.get_mut(&key) {
                let (engine, old) = (Arc::clone(engine), std::mem::replace(old, expiry));
                stored.expiry.remove(&(old, key));
                stored.expiry.insert((expiry, key));
                engine
            } else {
                if stored.entries.len() >= WAITING
                    || stored.peers.get(&key.0).copied().unwrap_or(0) >= WAITING / 2
                {
                    return Err(crate::Error::SyncCapacity(
                        "served goal descriptors are occupied".into(),
                    ));
                }
                let engine = Arc::new(Mutex::new(template.session_plan()));
                stored.entries.insert(key, (Arc::clone(&engine), expiry));
                stored.expiry.insert((expiry, key));
                *stored.peers.entry(key.0).or_default() += 1;
                engine
            }
        };
        let guard = match engine.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(crate::Error::SyncCapacity("topic planner is busy".into()));
            }
            Err(std::sync::TryLockError::Poisoned(error)) => {
                let mut guard = error.into_inner();
                *guard = template.session_plan();
                guard
            }
        };
        let _running = RunningPlan {
            store: self,
            key,
            engine: &engine,
        };
        let result = run(&guard);
        if matches!(&result, Err(crate::Error::StaleIncarnation)) {
            guard.release_plan(key.0, key.1);
        }
        let idle = guard.plan_idle();
        drop(guard);
        if idle {
            let mut stored = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if Arc::strong_count(&engine) == 2
                && stored
                    .entries
                    .get(&key)
                    .is_some_and(|(current, _)| Arc::ptr_eq(current, &engine))
            {
                stored.remove(key);
            }
        }
        result
    }

    pub(super) fn clear(&self) {
        let mut stored = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stored.entries.clear();
        stored.expiry.clear();
        stored.peers.clear();
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }
}

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
    use crate::net::iroh::service::*;
    use crate::tests::support::*;

    #[test]
    fn goals_release() {
        let log = oplog::Oplog::new();
        let signer = Ed25519Signer::from_bytes(&[217; 32]);
        let peers = [peer(1), peer(2), peer(3)];
        let template =
            crate::sync::SyncEngine::new(log.clone(), signer.peer_id()).with_page_visits(1, 16);
        let store = PlanStore::default();
        let mut requests = Vec::new();
        for index in 0..64_u64 {
            let topic = TopicId::hash(index.to_le_bytes());
            let actor = actor_id_for(topic, signer.peer_id());
            let genesis = log
                .create_topic_genesis(
                    topic,
                    actor,
                    TopicGenesis::new(Note::TYPE_ID, peers.into_iter().chain([signer.peer_id()])),
                    &signer,
                )
                .unwrap();
            let mut summary = template.summary(topic).unwrap();
            summary.actor_clock = ActorClock::new();
            let request = crate::sync::SyncRequest {
                topic_id: topic,
                known: Default::default(),
                wants: Default::default(),
                actor_range_hints: vec![crate::sync::ActorRangeHint {
                    actor_id: actor,
                    from_exclusive: 0,
                    to_inclusive: 1,
                }],
                genesis: Some(genesis.id),
                credit: Default::default(),
                window: Default::default(),
            };
            for peer in &peers[..2] {
                let page = store
                    .with_plan((*peer, topic), &template, |engine| {
                        engine.response_with(
                            *peer,
                            &request,
                            crate::sync::PageBudget::from_credit(request.credit),
                            &summary,
                        )
                    })
                    .unwrap();
                assert!(page.continued);
            }
            requests.push((request, summary));
        }
        assert_eq!(store.len(), WAITING);
        let (request, summary) = &requests[0];
        assert!(matches!(
            store.with_plan((peers[2], request.topic_id), &template, |_| Ok(())),
            Err(Error::SyncCapacity(_))
        ));
        assert!(template.page_work().kept_bytes > 0);
        store.clear();
        assert_eq!(store.len(), 0);
        assert_eq!(template.page_work().kept_bytes, 0);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: crate::Result<()> =
                store.with_plan((peers[0], request.topic_id), &template, |engine| {
                    assert!(
                        engine
                            .response_with(
                                peers[0],
                                request,
                                crate::sync::PageBudget::from_credit(request.credit),
                                summary
                            )?
                            .continued
                    );
                    panic!("planner job failed");
                });
        }));
        assert!(panicked.is_err());
        assert_eq!(store.len(), 0);
        assert_eq!(template.page_work().kept_bytes, 0);
    }

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
