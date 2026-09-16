//! Ownership faults combined in one run: reclamation beside a new session and
//! quota pressure, competing activations beside a retained writer and a paused
//! reader, and expiry beside admission. Every replacement stays exact.

use super::ownership::{admit, assert_bytes_exact, bytes, history, reader_node};
use super::support::*;

use crate::node::ReceiveOutcome;
use crate::storage::StagingLimits;
use crate::sync::SyncData;

/// An expired namespace discarded before a retained view writes to it: the
/// write fails as stale, and the freed bytes let another topic's staging fit.
fn assert_expiry_frees<S: Storage>(open: impl Fn(StagingLimits) -> S) {
    let (first_source, first_topic, first_ops) = history(170, 12, 256);
    let (second_source, second_topic, second_ops) = history(171, 12, 256);
    let (first_ops, second_ops) = (&first_ops[..7], &second_ops[..7]);
    let limit = bytes(first_ops).max(bytes(second_ops)) + 64;
    let storage = open(StagingLimits {
        total_bytes: limit,
        ..StagingLimits::MEMORY
    });
    let reader = reader_node(storage.clone());
    let receive = |source: &Irokle, topic_id, ops: &[Op]| {
        reader.receive_sync_outcome(
            source.peer_id(),
            SyncData {
                topic_id,
                ops: ops.to_vec(),
            },
        )
    };
    assert!(matches!(
        receive(&first_source, first_topic, &first_ops[..5]),
        Ok(ReceiveOutcome::Staged(_))
    ));
    let first = storage.provisional_topics().unwrap().remove(0);
    let retained = storage.provisional_store(&first).unwrap().unwrap();
    let refused = receive(&second_source, second_topic, second_ops);
    assert!(
        matches!(refused, Err(Error::StagingCapacity(_))),
        "{refused:?}"
    );

    reader.expire_bootstraps(u64::MAX).unwrap();
    assert!(storage.provisional_topics().unwrap().is_empty());
    let late = admit(&retained, first.source, &first_ops[5..]);
    assert!(matches!(late, Err(Error::StaleIncarnation)), "{late:?}");
    assert!(matches!(
        receive(&second_source, second_topic, second_ops),
        Ok(ReceiveOutcome::Staged(_))
    ));
    let held = storage.provisional_topics().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(
        (held[0].topic_id, held[0].bytes),
        (second_topic, bytes(second_ops))
    );
    assert_bytes_exact(&storage);
}

#[test]
fn memory_expiry_frees() {
    assert_expiry_frees(|limits| MemoryStorage::new().with_staging_limits(limits));
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_expiry_frees() {
    let dirs = std::sync::Mutex::new(Vec::new());
    assert_expiry_frees(|limits| {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::FjallStorage::open(dir.path())
            .unwrap()
            .with_staging_limits(limits);
        dirs.lock().unwrap().push(dir);
        storage
    });
}

#[cfg(feature = "fjall")]
mod fjall {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::super::ownership::fjall::{assert_hidden, pause_at, staged};
    use super::super::ownership::{contents, listed, stage};
    use super::*;
    use crate::storage::{AdmissionEffects, FjallStorage, Hook};

    /// Clearing beside staging at the total limit must preserve the new session.
    /// Its stale view fails, and all byte counts remain exact.
    #[test]
    fn reclaim_beside_pressure() {
        let dir = tempfile::tempdir().unwrap();
        let (old_source, old_topic, old_ops) = history(172, 30, 64);
        let (new_source, new_topic, new_ops) = history(173, 30, 64);
        let (busy_source, busy_topic, busy_ops) = history(174, 30, 64);
        let limit = bytes(&new_ops[..20]) + bytes(&busy_ops[..20]);
        let storage = FjallStorage::open(dir.path())
            .unwrap()
            .with_staging_limits(StagingLimits {
                total_bytes: limit,
                ..StagingLimits::MEMORY
            });
        let (old, _) = staged(&storage, old_source.peer_id(), old_topic, &old_ops[..20]);
        let retained = storage.provisional_store(&old).unwrap().unwrap();
        let (busy, _) = staged(&storage, busy_source.peer_id(), busy_topic, &busy_ops[..20]);

        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        pause_at(&storage, Hook::DeleteChunk, 0, &gate);
        let clearing = thread::spawn({
            let storage = storage.clone();
            move || storage.discard_provisional(&old)
        });
        gate.wait_arrival();
        let (new, _) = staged(&storage, new_source.peer_id(), new_topic, &new_ops[..20]);
        let over = stage(&storage, &busy, &busy_ops[20..]);
        assert!(matches!(over, Err(Error::StagingCapacity(_))), "{over:?}");
        let late = admit(&retained, old_source.peer_id(), &old_ops[20..]);
        let before = contents(&storage, &new);
        drop(release);
        assert!(clearing.join().unwrap().unwrap());

        assert!(matches!(late, Err(Error::StaleIncarnation)), "{late:?}");
        assert_eq!(contents(&storage, &new), before);
        assert_eq!(before.2, bytes(&new_ops[..20]));
        let total: u64 = storage
            .provisional_topics()
            .unwrap()
            .iter()
            .map(|provisional| provisional.bytes)
            .sum();
        assert_eq!(total, limit);
        assert_bytes_exact(&storage);
        drop((storage, retained));
        let reopened = FjallStorage::open(dir.path()).unwrap();
        assert_eq!(contents(&reopened, &new), before);
        assert_eq!(
            listed(&reopened, &busy).unwrap().bytes,
            bytes(&busy_ops[..20])
        );
        assert_bytes_exact(&reopened);
    }

    /// Paused activation retains records while staging and prepublication views live.
    /// Resume writes nothing; the stale view fails and the reader sees no topic.
    /// Reopen sees exactly the published topic and append.
    #[test]
    fn activations_beside_reader() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path()).unwrap();
        let other = storage.clone();
        let (source, topic_id, ops) = history(175, 30, 8);
        let actor = actor_id_for(topic_id, source.peer_id());
        let (provisional, state) = staged(&storage, source.peer_id(), topic_id, &ops);
        let retained = storage.provisional_store(&provisional).unwrap().unwrap();

        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        let copies = AtomicUsize::new(0);
        storage.set_hook({
            let gate = Arc::clone(&gate);
            move |at| match at {
                Hook::CopyChunk if copies.fetch_add(1, Ordering::SeqCst) == 0 => {
                    gate.pass();
                    Ok(())
                }
                Hook::DeleteChunk => Err(Error::Storage("slot kept for the late copy".into())),
                _ => Ok(()),
            }
        });
        let late = thread::spawn({
            let (storage, provisional, state) =
                (storage.clone(), provisional.clone(), state.clone());
            move || storage.activate_provisional(&provisional, &state, AdmissionEffects::default())
        });
        gate.wait_arrival();

        let opened = Arc::new(Barrier::new(2));
        let published = Arc::new(Barrier::new(2));
        let reading = thread::spawn({
            let (storage, opened, published) =
                (storage.clone(), Arc::clone(&opened), Arc::clone(&published));
            let ops = ops.clone();
            move || {
                storage
                    .read_snapshot(|read| {
                        opened.wait();
                        published.wait();
                        assert!(read.topic_view(&topic_id, None)?.is_none());
                        assert!(read.list_op_ids(&topic_id)?.is_empty());
                        for op in &ops {
                            assert!(read.get_op(&op.id)?.is_none());
                        }
                        Ok(())
                    })
                    .unwrap();
            }
        });
        opened.wait();
        assert!(
            other
                .activate_provisional(&provisional, &state, AdmissionEffects::default())
                .is_err(),
            "the slot was not cleared"
        );
        published.wait();
        reading.join().unwrap();
        let written = admit(&retained, source.peer_id(), &ops[..1]);
        assert!(
            matches!(written, Err(Error::StaleIncarnation)),
            "{written:?}"
        );
        let appended = source
            .open_topic::<Note>(topic_id)
            .unwrap()
            .publish(Note {
                text: "after".into(),
            })
            .unwrap()
            .meta
            .op_id;
        let appended = source.storage().get_op(&appended).unwrap().unwrap();
        admit(&other, source.peer_id(), &[appended]).unwrap();
        let view = |storage: &FjallStorage| {
            (
                storage.topic_view(&topic_id, None).unwrap(),
                storage.list_op_ids(&topic_id).unwrap(),
                storage.actor_range(&topic_id, &actor, 0, 64).unwrap(),
            )
        };
        let before = view(&other);
        drop(release);
        assert!(matches!(
            late.join().unwrap(),
            Err(Error::AdmissionConflict)
        ));
        assert_eq!(view(&other), before);

        drop((storage, other, retained));
        let reopened = FjallStorage::open(dir.path()).unwrap();
        assert_eq!(view(&reopened), before);
        let (fresh_source, fresh_topic, fresh_ops) = history(176, 2, 8);
        let (fresh, _) = staged(&reopened, fresh_source.peer_id(), fresh_topic, &fresh_ops);
        assert_eq!(contents(&reopened, &fresh).2, bytes(&fresh_ops));
        assert_hidden(
            &reopened,
            fresh_topic,
            actor_id_for(fresh_topic, fresh_source.peer_id()),
            &fresh_ops,
        );
        assert_eq!(reopened.provisional_topics().unwrap().len(), 1);
    }
}
