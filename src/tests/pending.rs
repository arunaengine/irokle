//! Buffered ops are rejected only for immutable reasons and retained while a
//! later arrival can still prove them.

use crate::tests::support::*;

use crate::oplog::Oplog;

/// An event signed by `signer` at `seq` behind `prev`, depending on `deps`.
fn event_op(
    signer: &Ed25519Signer,
    topic_id: TopicId,
    seq: u64,
    prev: Option<&Op>,
    deps: &[&Op],
    text: &str,
) -> Op {
    let generation = deps
        .iter()
        .map(|dep| dep.signed.body.generation + 1)
        .max()
        .unwrap_or_default();
    Op::sign(
        OpBody {
            topic_id,
            author: signer.peer_id(),
            actor_id: actor_id_for(topic_id, signer.peer_id()),
            actor_seq: seq,
            actor_prev: prev.map(|op| op.id),
            deps: deps.iter().map(|dep| dep.id).collect(),
            generation,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
            ),
        },
        signer,
    )
    .unwrap()
}

struct Members {
    alice: Irokle,
    bob: Ed25519Signer,
    carol: Ed25519Signer,
    topic_id: TopicId,
    genesis: Op,
}

fn members(seed: u8) -> Members {
    let alice = node(seed);
    let bob = Ed25519Signer::from_bytes(&[seed.wrapping_add(1); 32]);
    let carol = Ed25519Signer::from_bytes(&[seed.wrapping_add(2); 32]);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id(), carol.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
    Members {
        topic_id: topic.id(),
        alice,
        bob,
        carol,
        genesis,
    }
}

fn buffered<S: Storage>(storage: &S, topic_id: &TopicId, op: &Op) -> bool {
    storage.get_op(&op.id).unwrap().is_none()
        && !storage.pending_missing_deps(topic_id).unwrap().is_empty()
}

/// A member's op names as predecessor an id that later arrives as another
/// actor's op. Once that content is known the edge is impossible, so the op
/// and its descendant go while a valid op waiting on the same id is admitted.
fn assert_rejects_foreign<S: Storage>(storage: S) {
    let m = members(210);
    let d = event_op(&m.carol, m.topic_id, 1, None, &[&m.genesis], "d");
    let bob_first = event_op(&m.bob, m.topic_id, 1, None, &[&m.genesis], "b1");
    let wrong = event_op(&m.bob, m.topic_id, 2, Some(&d), &[&d], "wrong");
    let child = event_op(&m.bob, m.topic_id, 3, Some(&wrong), &[&wrong], "child");
    let sibling = event_op(&m.carol, m.topic_id, 2, Some(&d), &[&d], "sibling");
    let log = Oplog::with_storage(storage.clone());
    let source = Some(m.alice.peer_id());
    log.receive_ops_from_peer(source, vec![m.genesis.clone()])
        .unwrap();
    for op in [&child, &wrong, &sibling] {
        log.receive_ops_from_peer(source, vec![op.clone()]).unwrap();
    }
    assert!(buffered(&storage, &m.topic_id, &wrong));

    let admitted = log.receive_ops_from_peer(source, vec![d.clone()]).unwrap();
    assert_eq!(admitted, [d.id, sibling.id].into());
    assert!(storage.get_op(&wrong.id).unwrap().is_none());
    assert!(storage.pending_waiters(&wrong.id).unwrap().is_empty());
    assert!(
        storage
            .pending_missing_deps(&m.topic_id)
            .unwrap()
            .is_empty()
    );
    assert!(storage.ready_pending_ops().unwrap().is_empty());
    // Bob's real chain is untouched by the rejection.
    assert_eq!(
        log.receive_ops_from_peer(source, vec![bob_first.clone()])
            .unwrap(),
        [bob_first.id].into()
    );
}

#[test]
fn memory_rejects_foreign() {
    assert_rejects_foreign(MemoryStorage::new());
}

/// A predecessor of the right actor but the wrong sequence is just as final.
#[test]
fn rejects_impossible_seq() {
    let m = members(214);
    let first = event_op(&m.bob, m.topic_id, 1, None, &[&m.genesis], "one");
    let skipping = event_op(&m.bob, m.topic_id, 3, Some(&first), &[&first], "three");
    let log = Oplog::new();
    let source = Some(m.alice.peer_id());
    log.receive_ops_from_peer(source, vec![m.genesis.clone()])
        .unwrap();
    log.receive_ops_from_peer(source, vec![skipping.clone()])
        .unwrap();
    assert!(buffered(log.storage(), &m.topic_id, &skipping));

    log.receive_ops_from_peer(source, vec![first.clone()])
        .unwrap();
    assert!(log.storage().get_op(&skipping.id).unwrap().is_none());
    assert!(
        log.storage()
            .pending_missing_deps(&m.topic_id)
            .unwrap()
            .is_empty()
    );
}

/// A buffered op is never rejected by latest membership, since its author may be invited
/// by an unseen control: it is admitted once its frontier proves membership, in either
/// arrival order, and rejected once the frontier proves the opposite.
#[test]
fn causal_membership_orders() {
    for invite_first in [false, true] {
        let m = members(216);
        let dave = Ed25519Signer::from_bytes(&[231; 32]);
        let topic = m.alice.open_topic::<Note>(m.topic_id).unwrap();
        topic.add_peer(dave.peer_id()).unwrap();
        let ops = oplog::topological(m.alice.storage(), &m.topic_id).unwrap();
        let invite = ops[1].clone();
        let invited = event_op(&dave, m.topic_id, 1, None, &[&invite], "invited");
        let erin = Ed25519Signer::from_bytes(&[230; 32]);
        let uninvited = event_op(&erin, m.topic_id, 1, None, &[&m.genesis], "uninvited");

        let log = Oplog::new();
        let source = Some(m.alice.peer_id());
        log.receive_ops_from_peer(source, vec![m.genesis.clone()])
            .unwrap();
        if invite_first {
            log.receive_ops_from_peer(source, vec![invite.clone()])
                .unwrap();
            assert_eq!(
                log.receive_ops_from_peer(source, vec![invited.clone()])
                    .unwrap(),
                [invited.id].into()
            );
        } else {
            log.receive_ops_from_peer(source, vec![invited.clone()])
                .unwrap();
            assert!(buffered(log.storage(), &m.topic_id, &invited));
            assert_eq!(
                log.receive_ops_from_peer(source, vec![invite.clone()])
                    .unwrap(),
                [invite.id, invited.id].into()
            );
        }
        // A frontier without the invitation proves the author was no member.
        assert!(matches!(
            log.receive_ops_from_peer(source, vec![uninvited.clone()]),
            Err(Error::NotTopicMember)
        ));
    }
}

/// An unproven author's op waiting on a missing dependency is retained under
/// the source's quota rather than rejected by the latest membership.
#[test]
fn retains_unproven_author() {
    let m = members(220);
    let outsider = Ed25519Signer::from_bytes(&[232; 32]);
    let missing = event_op(&m.bob, m.topic_id, 1, None, &[&m.genesis], "missing");
    let unproven = event_op(&outsider, m.topic_id, 1, None, &[&missing], "unproven");
    let log = Oplog::new();
    let source = Some(m.alice.peer_id());
    log.receive_ops_from_peer(source, vec![m.genesis.clone()])
        .unwrap();
    assert!(
        log.receive_ops_from_peer(source, vec![unproven.clone()])
            .unwrap()
            .is_empty()
    );
    assert_eq!(log.storage().pending_waiters(&missing.id).unwrap().len(), 1);

    // The dependency proves the author was never invited on that frontier.
    log.receive_ops_from_peer(source, vec![missing.clone()])
        .unwrap();
    assert!(log.storage().get_op(&unproven.id).unwrap().is_none());
    assert!(
        log.storage()
            .pending_waiters(&missing.id)
            .unwrap()
            .is_empty()
    );
    assert!(
        log.storage()
            .pending_missing_deps(&m.topic_id)
            .unwrap()
            .is_empty()
    );
}

/// A removed member's buffered op behind a dependency that never arrives keeps
/// its dependency unresolved, but neither hides the admitted history nor empties the ack.
#[test]
fn orphan_keeps_history() {
    let m = members(234);
    let topic = m.alice.open_topic::<Note>(m.topic_id).unwrap();
    topic
        .publish(Note {
            text: "kept".into(),
        })
        .unwrap();
    topic.remove_peer(m.bob.peer_id()).unwrap();
    let missing = OpId::hash(b"never stored");
    let orphan = Op::sign(
        OpBody {
            topic_id: m.topic_id,
            author: m.bob.peer_id(),
            actor_id: actor_id_for(m.topic_id, m.bob.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: [missing].into(),
            generation: 1,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "orphan".into(),
                })
                .unwrap(),
            ),
        },
        &m.bob,
    )
    .unwrap();
    let data = sync::SyncData {
        topic_id: m.topic_id,
        ops: vec![orphan.clone()],
    };
    let (ack, _) = m
        .alice
        .receive_sync_data_from(m.bob.peer_id(), data)
        .unwrap();

    assert!(buffered(m.alice.storage(), &m.topic_id, &orphan));
    assert_eq!(
        m.alice.topic_unresolved(m.topic_id).unwrap(),
        [missing].into()
    );
    assert_eq!(ack.heads, topic.heads().unwrap());
    assert_eq!(ack.clock, topic.actor_clock().unwrap());
    let history = topic
        .history(crate::history::HistoryOrder::OldestFirst)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].event.text, "kept");
    let after = topic
        .history_after(
            &crate::history::HistoryCursor {
                genesis: m.genesis.id,
                clock: ActorClock::new(),
            },
            crate::history::HistoryOrder::OldestFirst,
        )
        .unwrap();
    assert_eq!(after.len(), 1);
}

/// Buffered ops of `count` distinct authors waiting on one missing op of
/// `topic_id`, each charged to `source`.
fn fill_waiters<S: Storage>(storage: &S, m: &Members, source: PeerId, count: u8, seed: u8) {
    let missing = event_op(
        &m.bob,
        m.topic_id,
        1,
        None,
        &[&m.genesis],
        &format!("fill-missing-{seed}"),
    );
    for index in 0..count {
        let author = Ed25519Signer::from_bytes(&[
            seed, index, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, 26, 27, 28, 29, 30,
        ]);
        let op = event_op(&author, m.topic_id, 1, None, &[&missing], "fill");
        let meta = crate::storage::OpMeta {
            id: op.id,
            topic_id: m.topic_id,
            author: author.peer_id(),
            actor_id: op.signed.body.actor_id,
            actor_seq: 1,
            actor_prev: None,
            deps: [missing.id].into(),
            generation: op.signed.body.generation,
            observed_clock: ActorClock::new(),
            ready: false,
            missing_deps: [missing.id].into(),
        };
        storage.put_pending_op(source, op, meta).unwrap();
    }
}

/// A healthy topic's admission, fingerprint and view read no payload of a
/// buffered op that belongs to another topic.
fn assert_skips_unrelated<S: Storage>(storage: S, counters: impl Fn(&S) -> crate::CounterSnapshot) {
    let busy = members(236);
    let healthy = members(240);
    let log = Oplog::with_storage(storage.clone());
    log.receive_ops_from_peer(Some(busy.alice.peer_id()), vec![busy.genesis.clone()])
        .unwrap();
    fill_waiters(&storage, &busy, busy.alice.peer_id(), 64, 237);
    log.receive_ops_from_peer(Some(healthy.alice.peer_id()), vec![healthy.genesis.clone()])
        .unwrap();

    let before = counters(&storage).pending_payload_reads;
    let next = event_op(
        &healthy.bob,
        healthy.topic_id,
        1,
        None,
        &[&healthy.genesis],
        "next",
    );
    let admitted = log
        .receive_ops_from_peer(Some(healthy.alice.peer_id()), vec![next.clone()])
        .unwrap();
    assert_eq!(admitted, [next.id].into());
    let sync = crate::sync::SyncEngine::new(log.clone(), healthy.alice.peer_id());
    sync.fingerprint(healthy.topic_id).unwrap();
    sync.summary(healthy.topic_id).unwrap();
    assert!(
        storage
            .topic_view(&healthy.topic_id, None)
            .unwrap()
            .unwrap()
            .pending_missing
            .is_empty()
    );
    assert_eq!(
        counters(&storage).pending_payload_reads,
        before,
        "healthy-topic work decoded unrelated buffered payloads"
    );
}

#[test]
fn memory_skips_unrelated() {
    assert_skips_unrelated(MemoryStorage::new(), MemoryStorage::counters);
}

/// A rejected subtree stays rejected on its branch: re-inserting the root or
/// a child that waits on it is refused, in either order against the rejection,
/// and a reset of the topic forgets the markers.
fn assert_rejection_sticks<S: Storage>(storage: S) {
    let m = members(244);
    let d = event_op(&m.carol, m.topic_id, 1, None, &[&m.genesis], "d");
    let wrong = event_op(&m.bob, m.topic_id, 2, Some(&d), &[&d], "wrong");
    let child = event_op(&m.bob, m.topic_id, 3, Some(&wrong), &[&wrong], "child");
    let late_child = event_op(&m.bob, m.topic_id, 3, Some(&wrong), &[&wrong], "late child");
    let log = Oplog::with_storage(storage.clone());
    let source = Some(m.alice.peer_id());
    log.receive_ops_from_peer(source, vec![m.genesis.clone()])
        .unwrap();
    log.receive_ops_from_peer(source, vec![child.clone(), wrong.clone()])
        .unwrap();
    log.receive_ops_from_peer(source, vec![d.clone()]).unwrap();
    assert!(
        storage
            .pending_missing_deps(&m.topic_id)
            .unwrap()
            .is_empty()
    );

    // The invalid root is refused outright now that its content is checkable,
    // and stale copies of what waits on it are dropped rather than buffered.
    assert!(
        log.receive_ops_from_peer(source, vec![wrong.clone()])
            .is_err()
    );
    for op in [&child, &late_child] {
        assert!(
            log.receive_ops_from_peer(source, vec![op.clone()])
                .unwrap()
                .is_empty()
        );
    }
    let pending_meta = |op: &Op, missing: OpId| crate::storage::OpMeta {
        id: op.id,
        topic_id: m.topic_id,
        author: op.signed.body.author,
        actor_id: op.signed.body.actor_id,
        actor_seq: op.signed.body.actor_seq,
        actor_prev: op.signed.body.actor_prev,
        deps: op.signed.body.deps.clone(),
        generation: op.signed.body.generation,
        observed_clock: ActorClock::new(),
        ready: false,
        missing_deps: [missing].into(),
    };
    assert!(matches!(
        storage.put_pending_op(
            m.alice.peer_id(),
            late_child.clone(),
            pending_meta(&late_child, wrong.id)
        ),
        Err(Error::RejectedOp(_))
    ));
    assert!(
        storage
            .pending_missing_deps(&m.topic_id)
            .unwrap()
            .is_empty()
    );
    assert!(storage.ready_pending_ops().unwrap().is_empty());

    storage.reset_topic(&m.topic_id).unwrap();
    storage
        .put_pending_op(
            m.alice.peer_id(),
            late_child.clone(),
            pending_meta(&late_child, wrong.id),
        )
        .unwrap();
}

#[test]
fn memory_rejection_sticks() {
    assert_rejection_sticks(MemoryStorage::new());
}

/// Concurrent child insertion and subtree rejection never leave the child
/// buffered behind a rejected root, whichever commits first.
#[test]
fn rejection_races_insertion() {
    for _ in 0..32 {
        let m = members(248);
        let d = event_op(&m.carol, m.topic_id, 1, None, &[&m.genesis], "d");
        let wrong = event_op(&m.bob, m.topic_id, 2, Some(&d), &[&d], "wrong");
        let child = event_op(&m.bob, m.topic_id, 3, Some(&wrong), &[&wrong], "child");
        let storage = MemoryStorage::new();
        let log = Oplog::with_storage(storage.clone());
        let source = Some(m.alice.peer_id());
        log.receive_ops_from_peer(source, vec![m.genesis.clone()])
            .unwrap();
        log.receive_ops_from_peer(source, vec![wrong.clone()])
            .unwrap();
        assert!(!storage.pending_waiters(&d.id).unwrap().is_empty());
        let barrier = Arc::new(Barrier::new(2));
        let inserting = thread::spawn({
            let log = log.clone();
            let barrier = Arc::clone(&barrier);
            let child = child.clone();
            move || {
                barrier.wait();
                let _ = log.receive_ops_from_peer(source, vec![child]);
            }
        });
        barrier.wait();
        let _ = storage.reject_pending_subtree(&wrong.id);
        inserting.join().unwrap();
        assert!(storage.get_op(&child.id).unwrap().is_none());
        assert!(
            storage.pending_waiters(&wrong.id).unwrap().is_empty(),
            "a child was buffered behind a rejected root"
        );
    }
}

/// A large buffered op of `author` waiting on `missing`, with its metadata.
fn heavy_op(
    m: &Members,
    author: &Ed25519Signer,
    missing: &Op,
    bytes: usize,
) -> (Op, crate::storage::OpMeta) {
    let op = event_op(author, m.topic_id, 1, None, &[missing], &"x".repeat(bytes));
    let meta = crate::storage::OpMeta {
        id: op.id,
        topic_id: m.topic_id,
        author: author.peer_id(),
        actor_id: op.signed.body.actor_id,
        actor_seq: 1,
        actor_prev: None,
        deps: [missing.id].into(),
        generation: op.signed.body.generation,
        observed_clock: ActorClock::new(),
        ready: false,
        missing_deps: [missing.id].into(),
    };
    (op, meta)
}

/// The topic share bounds one topic even when every source is under its own
/// quota; duplicates through another source cost nothing; every refund is the
/// stored charge, so usage returns to zero exactly and survives a reopen.
fn assert_exact_accounting<S: Storage>(
    storage: S,
    usage: impl Fn(&S, &PeerId) -> (u64, u64, u64, u64),
    reopen: impl Fn(S) -> S,
) {
    let m = members(252);
    let missing = event_op(&m.bob, m.topic_id, 1, None, &[&m.genesis], "missing");
    let sources = [1_u8, 2, 3].map(|seed| Ed25519Signer::from_bytes(&[seed; 32]).peer_id());
    let chunk = 3 * 1024 * 1024;
    let mut stored = Vec::new();
    let mut charged = 0_u64;
    for (source_index, source) in sources[..2].iter().enumerate() {
        for index in 0..5_u8 {
            let author = Ed25519Signer::from_bytes(&[100 + source_index as u8 * 10 + index; 32]);
            let (op, meta) = heavy_op(&m, &author, &missing, chunk);
            charged += crate::storage::pending_op_bytes(&op).unwrap() as u64;
            storage
                .put_pending_op(*source, op.clone(), meta.clone())
                .unwrap();
            stored.push((*source, op, meta));
        }
    }
    let (op, meta) = heavy_op(&m, &Ed25519Signer::from_bytes(&[199; 32]), &missing, chunk);
    let refused = storage.put_pending_op(sources[2], op, meta).unwrap_err();
    assert!(refused.to_string().contains("topic"), "got {refused}");
    assert_eq!(usage(&storage, &sources[0]).0, 10);
    assert_eq!(usage(&storage, &sources[0]).1, charged);

    // A replay through another source neither charges it nor moves the charge.
    let (source, op, meta) = stored[0].clone();
    storage.put_pending_op(sources[1], op, meta).unwrap();
    assert_eq!(usage(&storage, &sources[1]).2, 5);
    assert_eq!(usage(&storage, &source).2, 5);

    let storage = reopen(storage);
    assert_eq!(usage(&storage, &sources[0]).1, charged);
    for (_, op, _) in &stored[..3] {
        storage.remove_pending_op(&op.id).unwrap();
        storage.remove_pending_op(&op.id).unwrap();
    }
    storage.reset_topic(&m.topic_id).unwrap();
    assert_eq!(usage(&storage, &sources[0]), (0, 0, 0, 0));
    assert_eq!(usage(&storage, &sources[1]), (0, 0, 0, 0));
}

#[test]
fn memory_exact_accounting() {
    assert_exact_accounting(
        MemoryStorage::new(),
        MemoryStorage::pending_usage,
        |storage| storage,
    );
}

/// With the pending pool and the bootstrap staging sessions saturated, a
/// healthy topic still admits data and clears work by ack, and every counter
/// returns exactly after rejection, expiry and reset.
#[test]
fn saturated_pools_healthy() {
    let storage = MemoryStorage::new();
    let busy = [members(2), members(6)];
    let healthy = members(10);
    let log = Oplog::with_storage(storage.clone());
    for m in busy.iter().chain([&healthy]) {
        log.receive_ops_from_peer(Some(m.alice.peer_id()), vec![m.genesis.clone()])
            .unwrap();
    }
    let sources = [20_u8, 21, 22, 23].map(|seed| Ed25519Signer::from_bytes(&[seed; 32]).peer_id());
    for (index, source) in sources.iter().enumerate() {
        let m = &busy[index / 2];
        for batch in 0..4_u8 {
            fill_waiters(&storage, m, *source, 255, index as u8 * 8 + batch);
        }
        fill_waiters(&storage, m, *source, 4, index as u8 * 8 + 4);
    }
    assert_eq!(
        storage.pending_usage(&sources[0]).0,
        4096,
        "the pool is full"
    );
    let staging_source = Ed25519Signer::from_bytes(&[30; 32]).peer_id();
    for session in 0..crate::storage::MAX_STAGED_SESSIONS_PER_SOURCE {
        let other = members(40 + session as u8 * 4);
        storage
            .open_provisional(staging_source, other.topic_id, other.genesis.id, 1)
            .unwrap();
    }
    let refused = members(200);
    assert!(
        storage
            .open_provisional(staging_source, refused.topic_id, refused.genesis.id, 1)
            .is_err()
    );

    // The healthy topic is unaffected: an event is admitted and its ack clears
    // the work owed for it.
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[10; 32]),
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let event = event_op(
        &healthy.bob,
        healthy.topic_id,
        1,
        None,
        &[&healthy.genesis],
        "healthy",
    );
    assert_eq!(
        log.receive_ops_from_peer(Some(healthy.bob.peer_id()), vec![event.clone()])
            .unwrap(),
        [event.id].into()
    );
    alice
        .put_sync_obligation(healthy.carol.peer_id(), healthy.topic_id, [event.id].into())
        .unwrap();
    let mut clock = ActorClock::new();
    clock.observe(event.signed.body.actor_id, 1);
    storage
        .apply_peer_ack(crate::storage::PeerAck {
            peer_id: healthy.carol.peer_id(),
            topic_id: healthy.topic_id,
            genesis: Some(healthy.genesis.id),
            heads: [event.id].into(),
            clock,
        })
        .unwrap();
    assert!(
        !storage
            .has_sync_obligations(&healthy.carol.peer_id(), &healthy.topic_id)
            .unwrap()
    );

    // Counters come back exactly: a reset frees one topic's share, expiry
    // frees the staging sessions.
    let before = storage.pending_usage(&sources[0]);
    storage.reset_topic(&busy[0].topic_id).unwrap();
    let after = storage.pending_usage(&sources[0]);
    assert_eq!(after.0, before.0 - 2048);
    assert_eq!((after.2, after.3), (0, 0));
    let mut ended = 0;
    for provisional in storage.provisional_topics().unwrap() {
        ended += usize::from(storage.discard_provisional(&provisional).unwrap());
    }
    assert_eq!(ended, crate::storage::MAX_STAGED_SESSIONS_PER_SOURCE);
    storage
        .open_provisional(staging_source, refused.topic_id, refused.genesis.id, 3)
        .unwrap();
    storage.reset_topic(&busy[1].topic_id).unwrap();
    assert_eq!(storage.pending_usage(&sources[2]), (0, 0, 0, 0));
}

/// A backend failure while a dependency arrives fails that receive but keeps
/// the op buffered behind it, so resending the dependency admits both.
#[test]
fn backend_failure_retains() {
    let m = members(236);
    let missing = event_op(&m.bob, m.topic_id, 1, None, &[&m.genesis], "missing");
    let child = event_op(&m.bob, m.topic_id, 2, Some(&missing), &[&missing], "child");
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let log = Oplog::with_storage(storage.clone());
    let source = Some(m.alice.peer_id());
    log.receive_ops_from_peer(source, vec![m.genesis.clone()])
        .unwrap();
    log.receive_ops_from_peer(source, vec![child.clone()])
        .unwrap();

    storage.failed_writes.lock().unwrap().insert(m.topic_id);
    assert!(matches!(
        log.receive_ops_from_peer(source, vec![missing.clone()]),
        Err(Error::Storage(_))
    ));
    assert!(buffered(&storage, &m.topic_id, &child));
    assert_eq!(storage.pending_waiters(&missing.id).unwrap().len(), 1);

    storage.failed_writes.lock().unwrap().clear();
    assert_eq!(
        log.receive_ops_from_peer(source, vec![missing.clone()])
            .unwrap(),
        BTreeSet::from([missing.id, child.id])
    );
    assert!(
        storage
            .pending_missing_deps(&m.topic_id)
            .unwrap()
            .is_empty()
    );
}

/// Two receives that each release a full topic share of buffered ops drain
/// at the same time: together they admit every released op and leave no
/// ready record behind for a later arrival to find.
fn assert_drains_complete<S: Storage>(storage: S) {
    const WAITERS: usize = crate::storage::MAX_PENDING_WAITERS_PER_DEP - 1;
    let owner = Ed25519Signer::from_bytes(&[251; 32]);
    let authors = (0..2 * (WAITERS + 1))
        .map(|index| {
            let mut seed = [252_u8; 32];
            seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Ed25519Signer::from_bytes(&seed)
        })
        .collect::<Vec<_>>();
    let log = Oplog::with_storage(storage.clone());
    let mut releases = Vec::new();
    let mut expected = BTreeSet::new();
    for topic in 0..2_u8 {
        let topic_id = TopicId::hash([b"drains-complete".as_slice(), &[topic]].concat());
        let members = authors.iter().map(Signer::peer_id).chain([owner.peer_id()]);
        let genesis = log
            .create_topic_genesis(
                topic_id,
                actor_id_for(topic_id, owner.peer_id()),
                TopicGenesis::new(Note::TYPE_ID, members),
                &owner,
            )
            .unwrap();
        let mut roots = Vec::new();
        for (half, chunk) in authors.chunks(WAITERS + 1).enumerate() {
            let root = event_op(&chunk[0], topic_id, 1, None, &[&genesis], "root");
            let source = PeerId::hash([b"drain-source".as_slice(), &[topic, half as u8]].concat());
            let waiters = chunk[1..]
                .iter()
                .map(|author| event_op(author, topic_id, 1, None, &[&root], "waiter"))
                .collect::<Vec<_>>();
            for batch in waiters.chunks(256) {
                let admitted = log
                    .receive_ops_from_peer(Some(source), batch.to_vec())
                    .unwrap();
                assert!(admitted.is_empty());
            }
            expected.extend(waiters.iter().map(|op| op.id));
            expected.insert(root.id);
            roots.push(root);
        }
        releases.push(roots);
    }
    let barrier = Arc::new(Barrier::new(2));
    let handles = releases
        .into_iter()
        .map(|roots| {
            let log = log.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                log.receive_ops(roots).unwrap()
            })
        })
        .collect::<Vec<_>>();
    let mut admitted = BTreeSet::new();
    for handle in handles {
        admitted.extend(handle.join().unwrap());
    }
    assert_eq!(admitted, expected);
    assert!(storage.ready_pending_after(None, 1).unwrap().is_empty());
}

#[test]
fn memory_drains_complete() {
    assert_drains_complete(MemoryStorage::new());
}

/// Write faults keep ready ops indexed while one receive releases a long chain.
/// The receive admits the whole chain however many visits retained ops cost.
/// Reconciliation stops while ops cannot be admitted and admits them once the fault clears.
fn assert_retained_drain<S: Storage>(inner: S) {
    // Enough retained visits that one visit window holds a few passes only.
    const RETAINED: usize = 511;
    const CHAIN: usize = 400;
    let storage = StaleReadStorage::new(inner);
    let log = Oplog::with_storage(storage.clone());
    let owner = Ed25519Signer::from_bytes(&[233; 32]);
    let authors = (0..=RETAINED)
        .map(|index| {
            let mut seed = [234_u8; 32];
            seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Ed25519Signer::from_bytes(&seed)
        })
        .collect::<Vec<_>>();
    let genesis = |label: &[u8]| {
        let topic_id = TopicId::hash([b"retained-drain".as_slice(), label].concat());
        let members = authors.iter().map(Signer::peer_id).chain([owner.peer_id()]);
        let genesis = log
            .create_topic_genesis(
                topic_id,
                actor_id_for(topic_id, owner.peer_id()),
                TopicGenesis::new(Note::TYPE_ID, members),
                &owner,
            )
            .unwrap();
        (topic_id, genesis)
    };
    let (faulty, faulty_genesis) = genesis(b"faulty");
    let (chained, chained_genesis) = genesis(b"chained");

    let root = event_op(&authors[0], faulty, 1, None, &[&faulty_genesis], "root");
    let retained = authors[1..]
        .iter()
        .map(|author| event_op(author, faulty, 1, None, &[&root], "retained"))
        .collect::<Vec<_>>();
    let faulty_source = PeerId::hash(b"retained-source");
    log.receive_ops_from_peer(Some(faulty_source), retained.clone())
        .unwrap();
    *storage.failed_ops.lock().unwrap() = retained.iter().map(|op| op.id).collect();
    log.receive_ops(vec![root]).unwrap();
    let retained_ids = retained.iter().map(|op| op.id).collect::<BTreeSet<_>>();
    let ready = |storage: &StaleReadStorage<S>| {
        storage
            .ready_pending_after(None, usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, op)| op.id)
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        ready(&storage),
        retained_ids,
        "the faulty ops are retained ready"
    );

    let mut chain: Vec<Op> = Vec::with_capacity(CHAIN);
    for seq in 1..=CHAIN as u64 {
        let deps = chain
            .last()
            .map_or(vec![&chained_genesis], |prev| vec![prev]);
        let op = event_op(&authors[0], chained, seq, chain.last(), &deps, "chain");
        chain.push(op);
    }
    log.receive_ops_from_peer(Some(PeerId::hash(b"chain-source")), chain[1..].to_vec())
        .unwrap();
    let admitted = log.receive_ops(vec![chain[0].clone()]).unwrap();
    assert_eq!(
        admitted,
        chain.iter().map(|op| op.id).collect::<BTreeSet<_>>(),
        "a receive left released chain ops behind"
    );
    assert_eq!(ready(&storage), retained_ids);
    assert!(log.reconcile_pending_ops().unwrap().is_empty());

    storage.failed_ops.lock().unwrap().clear();
    assert_eq!(log.reconcile_pending_ops().unwrap(), retained_ids);
}

#[test]
fn memory_retained_drain() {
    assert_retained_drain(MemoryStorage::new());
}

/// A buffered op behind a dependency that never arrives, and its own waiter,
/// expire once they waited longer than the limit since the first sweep saw
/// them. Admitted records stay, and nothing is left unresolved.
fn assert_pending_expires<S: Storage>(storage: S) {
    let m = members(236);
    let log = Oplog::with_storage(storage.clone());
    let source = Some(m.alice.peer_id());
    log.receive_ops_from_peer(source, vec![m.genesis.clone()])
        .unwrap();
    let missing = event_op(&m.bob, m.topic_id, 1, None, &[&m.genesis], "missing");
    let waiting = event_op(
        &m.bob,
        m.topic_id,
        2,
        Some(&missing),
        &[&missing],
        "waiting",
    );
    let behind = event_op(&m.carol, m.topic_id, 1, None, &[&waiting], "behind");
    log.receive_ops_from_peer(source, vec![waiting.clone(), behind.clone()])
        .unwrap();
    assert!(storage.is_pending(&waiting.id).unwrap());
    assert!(storage.is_pending(&behind.id).unwrap());

    assert_eq!(storage.expire_pending(1_000, 100).unwrap(), 0);
    assert_eq!(storage.expire_pending(1_100, 100).unwrap(), 0);
    assert_eq!(storage.expire_pending(1_101, 100).unwrap(), 2);
    assert!(!storage.is_pending(&waiting.id).unwrap());
    assert!(!storage.is_pending(&behind.id).unwrap());
    assert!(log.topic_unresolved(&m.topic_id).unwrap().is_empty());
    assert!(storage.get_op(&m.genesis.id).unwrap().is_some());

    // An op buffered later is timed from the sweep that first sees it.
    log.receive_ops_from_peer(source, vec![waiting.clone()])
        .unwrap();
    assert_eq!(storage.expire_pending(5_000, 100).unwrap(), 0);
    assert!(storage.is_pending(&waiting.id).unwrap());
}

#[test]
fn memory_pending_expires() {
    assert_pending_expires(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
mod fjall {
    use crate::tests::pending::*;

    #[test]
    fn rejects_foreign() {
        let dir = tempfile::tempdir().unwrap();
        assert_rejects_foreign(crate::storage::FjallStorage::open(dir.path()).unwrap());
    }

    #[test]
    fn skips_unrelated() {
        let dir = tempfile::tempdir().unwrap();
        assert_skips_unrelated(
            crate::storage::FjallStorage::open(dir.path()).unwrap(),
            crate::storage::FjallStorage::counters,
        );
    }

    #[test]
    fn rejection_sticks() {
        let dir = tempfile::tempdir().unwrap();
        assert_rejection_sticks(crate::storage::FjallStorage::open(dir.path()).unwrap());
    }

    #[test]
    fn exact_accounting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        assert_exact_accounting(
            crate::storage::FjallStorage::open(&path).unwrap(),
            crate::storage::FjallStorage::pending_usage,
            move |storage| {
                drop(storage);
                crate::storage::FjallStorage::open(&path).unwrap()
            },
        );
    }

    /// The same on a durable store, where every admission is a synced commit.
    /// Run explicitly: `cargo test --features fjall --lib pending::fjall::drains_complete -- --ignored`.
    #[test]
    #[ignore = "about two minutes of synced commits, run explicitly"]
    fn drains_complete() {
        let dir = tempfile::tempdir().unwrap();
        assert_drains_complete(crate::storage::FjallStorage::open(dir.path()).unwrap());
    }

    #[test]
    fn pending_expires() {
        let dir = tempfile::tempdir().unwrap();
        assert_pending_expires(crate::storage::FjallStorage::open(dir.path()).unwrap());
    }

    #[test]
    fn retained_drain() {
        let dir = tempfile::tempdir().unwrap();
        assert_retained_drain(
            crate::storage::FjallStorage::open_with_persist_mode(
                dir.path(),
                ::fjall::PersistMode::Buffer,
            )
            .unwrap(),
        );
    }
}
