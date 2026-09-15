//! Evidence and effects must stay bound to the branch they were read from when
//! a genesis reset commits in the middle of building them.

use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{SyncData, SyncEngine};

/// Two genesis candidates for one topic signed by the same author, so their
/// events take the same actor positions. `old` has the larger genesis id and
/// loses the tie-break against `new`.
pub(super) struct Branches {
    pub(super) topic_id: TopicId,
    pub(super) author: Ed25519Signer,
    pub(super) member: Ed25519Signer,
    pub(super) third: Ed25519Signer,
    pub(super) old: (Op, Op),
    pub(super) new: (Op, Op),
}

pub(super) fn branches(seed: u8) -> Branches {
    let topic_id = TopicId::hash([b"branch-race".as_slice(), &[seed]].concat());
    let author = Ed25519Signer::from_bytes(&[seed; 32]);
    let member = Ed25519Signer::from_bytes(&[seed.wrapping_add(1); 32]);
    let third = Ed25519Signer::from_bytes(&[seed.wrapping_add(2); 32]);
    let fourth = Ed25519Signer::from_bytes(&[seed.wrapping_add(3); 32]);
    let (_, _, left_genesis, left_event) = forked_side(
        MemoryStorage::new(),
        topic_id,
        seed,
        [member.peer_id(), third.peer_id()],
        "left",
    );
    let (_, _, right_genesis, right_event) = forked_side(
        MemoryStorage::new(),
        topic_id,
        seed,
        [member.peer_id(), third.peer_id(), fourth.peer_id()],
        "right",
    );
    let left = (left_genesis, left_event);
    let right = (right_genesis, right_event);
    let (old, new) = if left.0.id > right.0.id {
        (left, right)
    } else {
        (right, left)
    };
    assert_eq!(old.1.signed.body.actor_seq, new.1.signed.body.actor_seq);
    Branches {
        topic_id,
        author,
        member,
        third,
        old,
        new,
    }
}

/// Replace the old branch in `storage` with the new one.
pub(super) fn reset_to_new<S: Storage>(storage: &S, branches: &Branches) {
    let log = Oplog::with_storage(storage.clone());
    if storage.topic_state(&branches.topic_id).unwrap().is_none() {
        log.receive_ops(vec![branches.old.0.clone()]).unwrap();
    }
    let admitted = log
        .receive_ops_from_peer_evicting(
            Some(branches.author.peer_id()),
            vec![branches.new.0.clone(), branches.new.1.clone()],
        )
        .unwrap();
    assert_eq!(admitted.evictions.len(), 1, "the old branch was replaced");
}

#[cfg(feature = "iroh")]
fn assert_bound_admission<S: Storage>(storage: S) {
    for pending in [false, true] {
        let branches = branches(if pending { 206 } else { 207 });
        let topic = branches.topic_id;
        let store = StaleReadStorage::new(storage.clone());
        let log = Oplog::with_storage(store.clone());
        log.receive_ops(vec![branches.old.0.clone()]).unwrap();
        let node = Irokle::with_storage(
            store.clone(),
            NodeConfig {
                signer: branches.member.clone(),
                ..NodeConfig::default()
            },
        )
        .unwrap();
        let op = if pending {
            old_followers(&branches).0
        } else {
            branches.old.1.clone()
        };
        let gate = Arc::new(Gate::default());
        let release = gate.releaser();
        store.arm_read(
            if pending {
                GatePoint::Sync(topic, "pending")
            } else {
                GatePoint::Admit(topic)
            },
            Arc::clone(&gate),
        );
        let received = thread::spawn({
            let author = branches.author.peer_id();
            let genesis = branches.old.0.id;
            move || {
                node.receive_bound(
                    author,
                    SyncData {
                        topic_id: topic,
                        ops: vec![op],
                    },
                    Some(genesis),
                )
            }
        });
        gate.wait_arrival();
        assert!(gate.arrived() && !gate.has_left() && !received.is_finished());
        reset_to_new(&store, &branches);
        let before = store.topic_view(&topic, None).unwrap();
        let obligations = store.all_sync_obligations().unwrap();
        drop(release);
        assert!(matches!(
            received.join().unwrap(),
            Err(Error::StaleIncarnation)
        ));
        assert_eq!(store.topic_view(&topic, None).unwrap(), before);
        assert_eq!(store.all_sync_obligations().unwrap(), obligations);
        assert!(store.pending_missing_deps(&topic).unwrap().is_empty());
        assert_eq!(
            store.list_op_ids(&topic).unwrap(),
            [branches.new.0.id, branches.new.1.id].into()
        );
    }
}

#[cfg(feature = "iroh")]
#[test]
fn memory_bound_admission() {
    assert_bound_admission(MemoryStorage::new());
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[test]
fn fjall_bound_admission() {
    let directory = tempfile::tempdir().unwrap();
    assert_bound_admission(crate::storage::FjallStorage::open(directory.path()).unwrap());
}

/// An ack is built while a reset replaces the branch it started reading. It
/// must not pair the old genesis with the new branch's clock, which would prove
/// an old-branch position the member never held.
fn assert_ack_keeps_branch<S: Storage>(inner: S, isolation: Isolation) {
    let branches = branches(150);
    let topic_id = branches.topic_id;
    let author = branches.author.peer_id();
    let member = branches.member.peer_id();

    let author_log = Oplog::new();
    author_log
        .receive_ops(vec![branches.old.0.clone(), branches.old.1.clone()])
        .unwrap();
    let author_sync = SyncEngine::new(author_log.clone(), author);
    author_sync
        .put_obligation(member, topic_id, [branches.old.1.id].into())
        .unwrap();

    // The member holds only the old genesis, not the event it owes.
    let storage = StaleReadStorage::new(inner);
    let member_log = Oplog::with_storage(storage.clone());
    member_log
        .receive_ops_from_peer(Some(author), vec![branches.old.0.clone()])
        .unwrap();
    let member_sync = SyncEngine::new(member_log, member);

    // Pause right after the member read its view of the old branch.
    let writer = storage.clone();
    let signer = branches.member.clone();
    let received = interleave(
        &storage,
        (GatePoint::View(topic_id), 0),
        isolation,
        move || {
            member_sync.receive_data(
                author,
                member,
                SyncData {
                    topic_id,
                    ops: Vec::new(),
                },
            )
        },
        move || reset_to_new(&writer, &branches),
    );
    let (mut ack, _) = received.unwrap();
    ack.sign(&signer).unwrap();

    let _ = author_sync.apply_ack(&ack);
    assert!(
        author_log
            .storage()
            .has_sync_obligations(&member, &topic_id)
            .unwrap(),
        "an ack mixing two branches cleared old-branch work: {ack:?}"
    );
}

#[test]
fn ack_keeps_branch() {
    assert_ack_keeps_branch(MemoryStorage::new(), Isolation::Blocks);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_ack_keeps_branch() {
    let dir = tempfile::tempdir().unwrap();
    assert_ack_keeps_branch(
        crate::storage::FjallStorage::open(dir.path()).unwrap(),
        Isolation::Commits,
    );
}

/// A reached query reads an old op's metadata, then a reset installs the new
/// branch and new-branch evidence for the same actor position. The new clock
/// must not prove the old op, for one peer or for all peers.
#[test]
fn reached_keeps_branch() {
    for all_peers in [false, true] {
        let branches = branches(160);
        let member = branches.member.peer_id();
        let old_event = branches.old.1.id;
        let storage = StaleReadStorage::new(MemoryStorage::new());
        Oplog::with_storage(storage.clone())
            .receive_ops_from_peer(
                Some(branches.author.peer_id()),
                vec![branches.old.0.clone(), branches.old.1.clone()],
            )
            .unwrap();

        let gate = Arc::new(Gate::default());
        let _release = gate.releaser();
        storage.arm_read(GatePoint::Meta(old_event), Arc::clone(&gate));
        let querying = thread::spawn({
            let storage = storage.clone();
            let gate = Arc::clone(&gate);
            move || {
                let reached = if all_peers {
                    storage
                        .peers_reached_op(&old_event)
                        .unwrap()
                        .contains(&member)
                } else {
                    storage.peer_reached_op(&member, &old_event).unwrap()
                };
                gate.skip();
                reached
            }
        });
        gate.wait_arrival();
        storage.disarm_read();
        reset_to_new(&storage, &branches);
        let mut clock = ActorClock::new();
        clock.observe(
            branches.new.1.signed.body.actor_id,
            branches.new.1.signed.body.actor_seq,
        );
        storage
            .apply_peer_ack(crate::storage::PeerAck {
                peer_id: member,
                topic_id: branches.topic_id,
                genesis: Some(branches.new.0.id),
                heads: [branches.new.1.id].into(),
                clock,
            })
            .unwrap();
        gate.release();
        assert!(
            !querying.join().unwrap(),
            "new-branch evidence proved an old-branch op (all peers: {all_peers})"
        );
    }
}

/// Forwarding work for received data must not be written after a reset
/// discarded that data, or the store owes a peer ops that no longer exist.
#[test]
fn forward_keeps_branch() {
    let branches = branches(170);
    let topic_id = branches.topic_id;
    let author = branches.author.peer_id();
    let third = branches.third.peer_id();
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let node = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: branches.member.clone(),
            ..NodeConfig::default()
        },
    )
    .unwrap();
    node.receive_sync_data_from(
        author,
        SyncData {
            topic_id,
            ops: vec![branches.old.0.clone()],
        },
    )
    .unwrap();

    let gate = Arc::new(Gate::default());
    let _release = gate.releaser();
    storage.arm_read(GatePoint::PeerAck(third), Arc::clone(&gate));
    let receiving = thread::spawn({
        let node = node.clone();
        let gate = Arc::clone(&gate);
        let event = branches.old.1.clone();
        move || {
            let received = node.receive_sync_data_from(
                author,
                SyncData {
                    topic_id,
                    ops: vec![event],
                },
            );
            gate.skip();
            received.map(drop)
        }
    });
    gate.wait_arrival();
    storage.disarm_read();
    reset_to_new(&storage, &branches);
    gate.release();
    let _ = receiving.join().unwrap();

    let rows = storage.sync_obligations(&third, &topic_id).unwrap();
    assert!(
        rows.is_empty(),
        "forwarding work survived the reset that discarded its ops: {rows:?}"
    );
}

/// The data epoch moves with every reset of a topic and never with an append,
/// so caches keyed by genesis and epoch expire exactly when data is discarded.
fn assert_epoch_tracks_resets<S: Storage>(storage: S) {
    let branches = branches(180);
    let topic_id = branches.topic_id;
    let log = Oplog::with_storage(storage.clone());
    log.receive_ops_from_peer(
        Some(branches.author.peer_id()),
        vec![branches.old.0.clone()],
    )
    .unwrap();
    let epoch = |storage: &S| storage.topic_view(&topic_id, None).unwrap().unwrap().epoch;
    let first = epoch(&storage);
    log.receive_ops_from_peer(
        Some(branches.author.peer_id()),
        vec![branches.old.1.clone()],
    )
    .unwrap();
    assert_eq!(epoch(&storage), first, "an append keeps the epoch");

    reset_to_new(&storage, &branches);
    let view = storage.topic_view(&topic_id, None).unwrap().unwrap();
    assert!(view.epoch > first, "a reset advances the epoch");
    assert_eq!(view.state.genesis, branches.new.0.id);
    assert!(view.state.members.contains(&branches.member.peer_id()));
    assert!(view.state.members.contains(&branches.third.peer_id()));
    assert_eq!(view.state.heads, [branches.new.1.id].into());
    assert_eq!(
        view.tips.get(&branches.new.1.signed.body.actor_id),
        Some(&(branches.new.1.signed.body.actor_seq, branches.new.1.id))
    );
    assert_eq!(
        view.fingerprint,
        storage.topic_fingerprint(&topic_id).unwrap()
    );
}

#[test]
fn memory_epoch_tracks_resets() {
    assert_epoch_tracks_resets(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_epoch_tracks_resets() {
    let dir = tempfile::tempdir().unwrap();
    assert_epoch_tracks_resets(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Obligation ids resolved against one branch must not be written once a reset
/// replaced it: the write is conditioned on the genesis the ids were read from.
#[test]
fn obligation_keeps_branch() {
    let branches = branches(190);
    let topic_id = branches.topic_id;
    let third = branches.third.peer_id();
    let old_event = branches.old.1.id;
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let log = Oplog::with_storage(storage.clone());
    log.receive_ops_from_peer(
        Some(branches.author.peer_id()),
        vec![branches.old.0.clone(), branches.old.1.clone()],
    )
    .unwrap();
    let sync = SyncEngine::new(log, branches.member.peer_id());

    let gate = Arc::new(Gate::default());
    let _release = gate.releaser();
    storage.arm_read(GatePoint::Meta(old_event), Arc::clone(&gate));
    let writing = thread::spawn({
        let gate = Arc::clone(&gate);
        move || {
            let written = sync.put_obligation(third, topic_id, [old_event].into());
            gate.skip();
            written
        }
    });
    gate.wait_arrival();
    storage.disarm_read();
    reset_to_new(&storage, &branches);
    gate.release();
    assert!(matches!(
        writing.join().unwrap(),
        Err(Error::StaleIncarnation)
    ));
    assert!(
        storage
            .sync_obligations(&third, &topic_id)
            .unwrap()
            .is_empty()
    );
}

/// Sync state is dropped only for a peer that is still absent from the branch
/// the caller judged, never for a member of a replacement branch.
fn assert_clear_keeps_branch<S: Storage>(storage: S) {
    let branches = branches(200);
    let topic_id = branches.topic_id;
    let third = branches.third.peer_id();
    let outsider = Ed25519Signer::from_bytes(&[209; 32]).peer_id();
    reset_to_new(&storage, &branches);
    let genesis = Some(branches.new.0.id);
    let clock = storage.actor_clock(&topic_id).unwrap();
    for peer in [third, outsider] {
        storage
            .put_sync_obligation(
                crate::storage::SyncObligation::clock(peer, topic_id, clock.clone()),
                genesis,
            )
            .unwrap();
    }
    let stale = Some(branches.old.0.id);
    assert_eq!(
        storage
            .clear_peer_sync_state(&outsider, &topic_id, stale)
            .unwrap(),
        0
    );
    assert_eq!(
        storage
            .clear_peer_sync_state(&third, &topic_id, genesis)
            .unwrap(),
        0
    );
    assert_eq!(
        storage
            .clear_peer_sync_state(&outsider, &topic_id, genesis)
            .unwrap(),
        1
    );
    assert!(storage.has_sync_obligations(&third, &topic_id).unwrap());
    assert!(matches!(
        storage.put_sync_obligation(
            crate::storage::SyncObligation::clock(third, topic_id, clock),
            stale,
        ),
        Err(Error::StaleIncarnation)
    ));
}

#[test]
fn memory_clear_keeps_branch() {
    assert_clear_keeps_branch(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_clear_keeps_branch() {
    let dir = tempfile::tempdir().unwrap();
    assert_clear_keeps_branch(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Events beyond the first: the author's next old-branch event, its new-branch
/// counterpart at the same position, and a two-op old chain by the third member.
fn old_followers(branches: &Branches) -> (Op, Op, Op, Op) {
    let topic_id = branches.topic_id;
    let note = |text: &str| EventEnvelope::encode_event(&Note { text: text.into() }).unwrap();
    let old = vec![branches.old.0.clone(), branches.old.1.clone()];
    let author_log = Oplog::new();
    author_log.receive_ops(old.clone()).unwrap();
    let author_actor = actor_id_for(topic_id, branches.author.peer_id());
    let next = author_log
        .create_event_op(topic_id, author_actor, note("next"), &branches.author)
        .unwrap();
    let new_log = Oplog::new();
    new_log
        .receive_ops(vec![branches.new.0.clone(), branches.new.1.clone()])
        .unwrap();
    let replacement = new_log
        .create_event_op(
            topic_id,
            author_actor,
            note("replacement"),
            &branches.author,
        )
        .unwrap();
    assert_eq!(
        replacement.signed.body.actor_seq,
        next.signed.body.actor_seq
    );
    let third_log = Oplog::new();
    third_log.receive_ops(old).unwrap();
    let third_actor = actor_id_for(topic_id, branches.third.peer_id());
    let first = third_log
        .create_event_op(topic_id, third_actor, note("first"), &branches.third)
        .unwrap();
    let second = third_log
        .create_event_op(topic_id, third_actor, note("second"), &branches.third)
        .unwrap();
    (next, replacement, first, second)
}

/// Runs `run` on its own thread and returns once it waits at `point`.
fn paused<S: Storage, T: Send + 'static>(
    storage: &StaleReadStorage<S>,
    point: GatePoint,
    gate: &Arc<Gate>,
    run: impl FnOnce() -> T + Send + 'static,
) -> thread::JoinHandle<T> {
    storage.arm_read(point, Arc::clone(gate));
    let handle = thread::spawn({
        let gate = Arc::clone(gate);
        move || {
            let result = run();
            gate.skip();
            result
        }
    });
    gate.wait_arrival();
    assert!(!handle.is_finished(), "{point:?} was never paused");
    handle
}

/// A reset commits while an ack, forwarding effects, a page plan and a pending
/// drain are paused. No old-branch work proves or owes anything on the new
/// branch, the eviction names exactly the discarded records, usage stays exact.
/// Under [`Isolation::Blocks`] a reset cannot commit while a snapshot read is
/// paused, so the ack and page pauses run only on a store that isolates them.
fn assert_reset_pauses<S: Storage>(
    inner: S,
    isolation: Isolation,
    usage: impl Fn(&S) -> (u64, u64),
) {
    let branches = branches(210);
    let topic_id = branches.topic_id;
    let author = branches.author.peer_id();
    let member = branches.member.peer_id();
    let third = branches.third.peer_id();
    let (old_genesis, old_event) = (branches.old.0.id, branches.old.1.id);
    let (next, replacement, first, second) = old_followers(&branches);

    let author_log = Oplog::new();
    author_log
        .receive_ops(vec![branches.old.0.clone(), branches.old.1.clone()])
        .unwrap();
    let author_sync = SyncEngine::new(author_log.clone(), author);
    author_sync
        .put_obligation(member, topic_id, [next.id].into())
        .unwrap();

    let storage = StaleReadStorage::new(inner.clone());
    let node = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: branches.member.clone(),
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let receive = |source: PeerId, ops: Vec<Op>| {
        node.receive_sync_data_from(source, SyncData { topic_id, ops })
    };
    receive(author, vec![branches.old.0.clone(), branches.old.1.clone()]).unwrap();
    receive(third, vec![second.clone()]).unwrap();
    assert!(
        storage
            .pending_missing_deps(&topic_id)
            .unwrap()
            .contains(&first.id)
    );

    let gates = [(); 4].map(|()| Arc::new(Gate::default()));
    let _releases = gates.each_ref().map(Gate::releaser);
    let snapshots = matches!(isolation, Isolation::Commits);
    let member_sync = SyncEngine::new(Oplog::with_storage(storage.clone()), member);
    let acking = snapshots.then(|| {
        paused(&storage, GatePoint::View(topic_id), &gates[0], move || {
            member_sync.receive_data(
                author,
                member,
                SyncData {
                    topic_id,
                    ops: Vec::new(),
                },
            )
        })
    });
    // Forwarding effects commit inside the admission transaction, so the
    // admission pauses at its dependency read just before building them.
    let forwarding = paused(&storage, GatePoint::Meta(old_event), &gates[1], {
        let node = node.clone();
        let next = next.clone();
        move || {
            node.receive_sync_data_from(
                author,
                SyncData {
                    topic_id,
                    ops: vec![next],
                },
            )
        }
    });
    let draining = paused(&storage, GatePoint::Meta(second.id), &gates[2], {
        let node = node.clone();
        let first = first.clone();
        move || {
            node.receive_sync_data_from(
                third,
                SyncData {
                    topic_id,
                    ops: vec![first],
                },
            )
        }
    });
    #[cfg(feature = "iroh")]
    let planning = snapshots.then(|| {
        paused(&storage, GatePoint::Meta(old_event), &gates[3], {
            let node = node.clone();
            let request = sync::SyncRequest {
                topic_id,
                known: BTreeSet::new(),
                wants: BTreeSet::new(),
                actor_range_hints: vec![sync::ActorRangeHint {
                    actor_id: branches.old.1.signed.body.actor_id,
                    from_exclusive: 0,
                    to_inclusive: u64::MAX,
                }],
                genesis: Some(old_genesis),
                credit: sync::SyncCredit::default(),
                window: crate::sync::ActorWindow::default(),
            };
            move || {
                node.response_page(
                    third,
                    &request,
                    sync::PageBudget::from_credit(request.credit),
                )
            }
        })
    });

    storage.disarm_read();
    let discarded = storage.list_op_ids(&topic_id).unwrap();
    assert!(
        discarded.contains(&first.id),
        "the drain paused after its commit"
    );
    reset_to_new(&storage, &branches);
    Oplog::with_storage(storage.clone())
        .receive_ops_from_peer(Some(author), vec![replacement.clone()])
        .unwrap();
    for gate in &gates {
        gate.release();
    }

    if let Some(Ok((mut ack, _))) = acking.map(|acking| acking.join().unwrap()) {
        ack.sign(&branches.member).unwrap();
        let _ = author_sync.apply_ack(&ack);
    }
    assert!(
        author_log
            .storage()
            .has_sync_obligations(&member, &topic_id)
            .unwrap(),
        "an ack built across the reset cleared old-branch work"
    );
    let _ = forwarding.join().unwrap();
    let _ = draining.join().unwrap();
    #[cfg(feature = "iroh")]
    if let Some(Ok(page)) = planning.map(|planning| planning.join().unwrap()) {
        assert!(
            page.ops.iter().all(|op| discarded.contains(&op.id)),
            "a page for the old branch carried new-branch ops"
        );
    }
    assert!(
        storage
            .sync_obligations(&third, &topic_id)
            .unwrap()
            .is_empty(),
        "forwarding work for discarded ops survived the reset"
    );

    let view = storage.topic_view(&topic_id, None).unwrap().unwrap();
    assert_eq!(view.state.genesis, branches.new.0.id);
    assert_eq!(view.state.heads, [replacement.id].into());
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        [branches.new.0.id, branches.new.1.id, replacement.id].into()
    );
    let evictions = storage.pending_evictions().unwrap();
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0].losing_genesis, old_genesis);
    assert_eq!(
        evictions[0]
            .evicted
            .iter()
            .map(|evicted| evicted.op_id)
            .chain([old_genesis])
            .collect::<BTreeSet<_>>(),
        discarded
    );

    // Pending records left behind are counted exactly once each.
    let mut waiting = std::collections::BTreeMap::new();
    for dep in storage.pending_missing_deps(&topic_id).unwrap() {
        for (_, op) in storage.pending_waiters(&dep).unwrap() {
            waiting.insert(op.id, op);
        }
    }
    let bytes = waiting
        .values()
        .map(|op| crate::storage::pending_op_bytes(op).unwrap() as u64)
        .sum::<u64>();
    assert_eq!(usage(&inner), (waiting.len() as u64, bytes));

    // New-branch evidence for the same positions proves no old-branch op.
    storage
        .apply_peer_ack(crate::storage::PeerAck {
            peer_id: member,
            topic_id,
            genesis: Some(branches.new.0.id),
            heads: [replacement.id].into(),
            clock: view.clock.clone(),
        })
        .unwrap();
    for old in [old_event, next.id] {
        assert!(!storage.peer_reached_op(&member, &old).unwrap());
        assert!(!storage.peers_reached_op(&old).unwrap().contains(&member));
    }
}

#[test]
fn memory_reset_pauses() {
    assert_reset_pauses(MemoryStorage::new(), Isolation::Blocks, |storage| {
        let (ops, bytes, _, _) = storage.pending_usage(&PeerId::from_bytes([0; 32]));
        (ops, bytes)
    });
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_pauses() {
    let dir = tempfile::tempdir().unwrap();
    assert_reset_pauses(
        crate::storage::FjallStorage::open(dir.path()).unwrap(),
        Isolation::Commits,
        |storage| {
            let (ops, bytes, _, _) = storage.pending_usage(&PeerId::from_bytes([0; 32]));
            (ops, bytes)
        },
    );
}
