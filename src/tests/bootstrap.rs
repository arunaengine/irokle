use std::sync::OnceLock;

use super::support::*;
use crate::node::ReceiveOutcome;
use crate::storage::StagedTopic;
use crate::sync::{SyncAck, SyncData};

const BOB_SEED: u8 = 181;

fn bob_node<S: Storage>(storage: S) -> Irokle<S> {
    Irokle::with_storage(
        storage,
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[BOB_SEED; 32]),
            ..NodeConfig::default()
        },
    )
    .unwrap()
}

fn bob_peer() -> PeerId {
    Ed25519Signer::from_bytes(&[BOB_SEED; 32]).peer_id()
}

/// A topic of `seed` with `events` notes, then an invitation for bob, oldest
/// first.
fn invited_history(seed: u8, events: usize, text_len: usize) -> (Irokle, TopicId, Vec<Op>) {
    let alice = node(seed);
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..events {
        let text = format!("{index:0>text_len$}");
        topic.publish(Note { text }).unwrap();
    }
    topic.add_peer(bob_peer()).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    (alice, topic.id(), ops)
}

fn receive<S: Storage>(
    bob: &Irokle<S>,
    source: PeerId,
    topic_id: TopicId,
    ops: &[Op],
) -> ReceiveOutcome {
    let data = SyncData {
        topic_id,
        ops: ops.to_vec(),
    };
    bob.receive_sync_outcome(source, data).unwrap()
}

fn staged(outcome: ReceiveOutcome) -> StagedTopic {
    match outcome {
        ReceiveOutcome::Staged(staged) => staged,
        ReceiveOutcome::Acked { ack, .. } => panic!("expected staging, got an ack {ack:?}"),
    }
}

fn acked(outcome: ReceiveOutcome) -> SyncAck {
    match outcome {
        ReceiveOutcome::Acked { ack, .. } => *ack,
        ReceiveOutcome::Staged(staged) => panic!("expected an ack, got staging {staged:?}"),
    }
}

/// Nothing of a staged history is visible to any topic query.
fn assert_invisible<S: Storage>(bob: &Irokle<S>, source: PeerId, topic_id: TopicId, ops: &[Op]) {
    let storage = bob.storage();
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
    assert!(
        storage
            .topic_view(&topic_id, Some(&source))
            .unwrap()
            .is_none()
    );
    assert!(bob.list_topics().unwrap().is_empty());
    assert!(storage.list_op_ids(&topic_id).unwrap().is_empty());
    assert!(storage.pending_missing_deps(&topic_id).unwrap().is_empty());
    assert!(storage.ready_pending_ops().unwrap().is_empty());
    assert!(storage.peer_ack(&source, &topic_id).unwrap().is_none());
    assert!(storage.all_sync_obligations().unwrap().is_empty());
    for op in ops {
        assert!(storage.get_op(&op.id).unwrap().is_none());
        assert!(storage.peers_reached_op(&op.id).unwrap().is_empty());
    }
}

/// The whole history of `source` is active at once, and staging is gone.
fn assert_promoted<S: Storage>(bob: &Irokle<S>, source: &Irokle, topic_id: TopicId, ack: &SyncAck) {
    let storage = bob.storage();
    let expected = source.storage().list_op_ids(&topic_id).unwrap();
    assert_eq!(storage.list_op_ids(&topic_id).unwrap(), expected);
    assert_eq!(ack.genesis, genesis_of(source.storage(), &topic_id));
    assert_eq!(ack.clock, source.storage().actor_clock(&topic_id).unwrap());
    let state = storage.topic_state(&topic_id).unwrap().unwrap();
    assert!(state.members.contains(&bob.peer_id()));
    assert!(
        storage
            .staged_bootstrap_ops(&source.peer_id(), &topic_id)
            .unwrap()
            .is_empty()
    );
    bob.open_topic::<Note>(topic_id).unwrap();
}

fn actor_of(source: &Irokle, topic_id: TopicId) -> ActorId {
    actor_id_for(topic_id, source.peer_id())
}

fn split_invitation<S: Storage>(storage: S) {
    let (alice, topic_id, ops) = invited_history(182, 256, 1);
    assert_eq!(ops.len(), 258);
    let bob = bob_node(storage);
    let actor = actor_of(&alice, topic_id);

    let first = staged(receive(&bob, alice.peer_id(), topic_id, &ops[..100]));
    assert_eq!((first.clock.get(&actor), first.ops), (100, 100));
    assert_invisible(&bob, alice.peer_id(), topic_id, &ops);
    // The acknowledging method never reports staging as an ack.
    let data = SyncData {
        topic_id,
        ops: ops[..100].to_vec(),
    };
    match bob.receive_sync_data_from(alice.peer_id(), data) {
        Err(Error::BootstrapPending { staged }) => assert_eq!(staged.get(&actor), 100),
        other => panic!("expected a pending bootstrap, got {other:?}"),
    }

    let second = staged(receive(&bob, alice.peer_id(), topic_id, &ops[100..257]));
    assert_eq!((second.clock.get(&actor), second.ops), (257, 257));
    assert_invisible(&bob, alice.peer_id(), topic_id, &ops);

    let ack = acked(receive(&bob, alice.peer_id(), topic_id, &ops[257..]));
    assert_promoted(&bob, &alice, topic_id, &ack);
}

#[test]
fn memory_split_invitation() {
    split_invitation(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_split_invitation() {
    let dir = tempfile::tempdir().unwrap();
    split_invitation(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

fn byte_split<S: Storage>(storage: S) {
    let (alice, topic_id, ops) = invited_history(183, 12, 48 * 1024);
    let budget = 128 * 1024;
    let mut chunks = vec![Vec::new()];
    let mut bytes = 0;
    for op in ops {
        let size = postcard::experimental::serialized_size(&op).unwrap();
        if bytes + size > budget {
            chunks.push(Vec::new());
            bytes = 0;
        }
        bytes += size;
        chunks.last_mut().unwrap().push(op);
    }
    assert!(chunks.len() > 3);
    let bob = bob_node(storage);
    let (last, earlier) = chunks.split_last().unwrap();
    for chunk in earlier {
        staged(receive(&bob, alice.peer_id(), topic_id, chunk));
        assert_invisible(&bob, alice.peer_id(), topic_id, chunk);
    }
    let ack = acked(receive(&bob, alice.peer_id(), topic_id, last));
    assert_promoted(&bob, &alice, topic_id, &ack);
}

#[test]
fn memory_byte_split() {
    byte_split(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_byte_split() {
    let dir = tempfile::tempdir().unwrap();
    byte_split(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Two full 4096-op stages before the stage holding the invitation.
fn large_stages<S: Storage>(storage: S) {
    static HISTORY: OnceLock<(PeerId, TopicId, Vec<Op>, BTreeSet<OpId>)> = OnceLock::new();
    let (source, topic_id, ops, ids) = HISTORY.get_or_init(|| {
        let (alice, topic_id, ops) = invited_history(184, 8192, 1);
        let ids = alice.storage().list_op_ids(&topic_id).unwrap();
        (alice.peer_id(), topic_id, ops, ids)
    });
    let bob = bob_node(storage);
    for stage in ops.chunks(4096).take(2) {
        staged(receive(&bob, *source, *topic_id, stage));
        assert!(bob.storage().topic_state(topic_id).unwrap().is_none());
    }
    assert_invisible(&bob, *source, *topic_id, &ops[..1]);
    let ack = acked(receive(&bob, *source, *topic_id, &ops[8192..]));
    assert_eq!(&bob.storage().list_op_ids(topic_id).unwrap(), ids);
    assert_eq!(ack.clock.get(&actor_id_for(*topic_id, *source)), 8194);
}

#[test]
fn memory_large_stages() {
    large_stages(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_large_stages() {
    let dir = tempfile::tempdir().unwrap();
    large_stages(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

fn reordered_segments<S: Storage>(storage: S) {
    let (alice, topic_id, ops) = invited_history(185, 20, 1);
    let bob = bob_node(storage);
    let actor = actor_of(&alice, topic_id);
    let (head, rest) = ops.split_at(8);
    let (middle, tail) = rest.split_at(8);

    let first = staged(receive(&bob, alice.peer_id(), topic_id, tail));
    assert_eq!(first.clock.get(&actor), 0);
    let second = staged(receive(&bob, alice.peer_id(), topic_id, middle));
    let repeated = staged(receive(&bob, alice.peer_id(), topic_id, middle));
    assert_eq!(second, repeated);
    let mixed = [tail, middle].concat();
    assert_eq!(
        staged(receive(&bob, alice.peer_id(), topic_id, &mixed)),
        second
    );
    assert_eq!(second.ops, (tail.len() + middle.len()) as u64);
    assert_invisible(&bob, alice.peer_id(), topic_id, &ops);

    let ack = acked(receive(&bob, alice.peer_id(), topic_id, head));
    assert_promoted(&bob, &alice, topic_id, &ack);
}

#[test]
fn memory_reordered_segments() {
    reordered_segments(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reordered_segments() {
    let dir = tempfile::tempdir().unwrap();
    reordered_segments(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Staging of one source never completes another's, and a complete history
/// from a non-member source proves nothing.
fn source_mismatch<S: Storage>(storage: S) {
    let (alice, topic_id, ops) = invited_history(186, 6, 1);
    let carol = node(187).peer_id();
    let bob = bob_node(storage);
    let (invite, history) = ops.split_last().unwrap();

    staged(receive(&bob, carol, topic_id, history));
    staged(receive(
        &bob,
        alice.peer_id(),
        topic_id,
        std::slice::from_ref(invite),
    ));
    assert_invisible(&bob, alice.peer_id(), topic_id, &ops);
    staged(receive(&bob, carol, topic_id, std::slice::from_ref(invite)));
    assert_invisible(&bob, carol, topic_id, &ops);

    let ack = acked(receive(&bob, alice.peer_id(), topic_id, history));
    assert_promoted(&bob, &alice, topic_id, &ack);
    // Promotion drops every session of the topic.
    assert!(
        bob.storage()
            .staged_bootstrap_ops(&carol, &topic_id)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn memory_source_mismatch() {
    source_mismatch(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_source_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    source_mismatch(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// An invitation the staged history revokes again promotes nothing.
fn revoked_invitation<S: Storage>(storage: S) {
    let alice = node(188);
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    topic.publish(Note { text: "a".into() }).unwrap();
    topic.add_peer(bob_peer()).unwrap();
    topic.publish(Note { text: "b".into() }).unwrap();
    topic.remove_peer(bob_peer()).unwrap();
    let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
    let bob = bob_node(storage);

    let staged = staged(receive(&bob, alice.peer_id(), topic.id(), &ops));
    assert_eq!(staged.ops, ops.len() as u64);
    assert_invisible(&bob, alice.peer_id(), topic.id(), &ops);
}

#[test]
fn memory_revoked_invitation() {
    revoked_invitation(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_revoked_invitation() {
    let dir = tempfile::tempdir().unwrap();
    revoked_invitation(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Two staged genesis candidates: the first proven one becomes active and the
/// other branch never merges into it.
fn competing_genesis<S: Storage>(storage: S) {
    let topic_id = TopicId::hash(b"bootstrap-competing-genesis");
    let side = |seed: u8| {
        let peer = Ed25519Signer::from_bytes(&[seed; 32]).peer_id();
        let (log, signer, genesis, event) =
            forked_side(MemoryStorage::new(), topic_id, seed, [peer], "side");
        let control = TopicControl::AddPeer { peer: bob_peer() };
        let actor = actor_id_for(topic_id, peer);
        let invite = log
            .create_control_op(topic_id, actor, control, &signer)
            .unwrap();
        (peer, vec![genesis, event], invite)
    };
    let (first, first_history, first_invite) = side(189);
    let (second, second_history, second_invite) = side(190);
    let bob = bob_node(storage);

    staged(receive(&bob, first, topic_id, &first_history));
    staged(receive(&bob, second, topic_id, &second_history));
    let candidates = [first_history.clone(), second_history.clone()].concat();
    assert_invisible(&bob, first, topic_id, &candidates);

    acked(receive(
        &bob,
        first,
        topic_id,
        std::slice::from_ref(&first_invite),
    ));
    let winner = [first_history, vec![first_invite]].concat();
    let winner_ids = winner.iter().map(|op| op.id).collect::<BTreeSet<_>>();
    assert!(
        bob.storage()
            .staged_bootstrap_ops(&second, &topic_id)
            .unwrap()
            .is_empty()
    );
    let other = [second_history, vec![second_invite]].concat();
    acked(receive(&bob, second, topic_id, &other));
    assert_eq!(bob.storage().list_op_ids(&topic_id).unwrap(), winner_ids);
    assert_eq!(genesis_of(bob.storage(), &topic_id), Some(winner[0].id));
}

#[test]
fn memory_competing_genesis() {
    competing_genesis(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_competing_genesis() {
    let dir = tempfile::tempdir().unwrap();
    competing_genesis(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// An op of `topic_id` at `seq` carrying a note of `size` bytes.
fn stub_op(topic_id: TopicId, seq: u64, size: usize) -> Op {
    let signer = Ed25519Signer::from_bytes(&[191; 32]);
    let note = Note {
        text: "x".repeat(size),
    };
    let body = OpBody {
        topic_id,
        author: signer.peer_id(),
        actor_id: actor_id_for(topic_id, signer.peer_id()),
        actor_seq: seq,
        actor_prev: None,
        deps: BTreeSet::new(),
        generation: 0,
        payload: TopicPayload::Event(EventEnvelope::encode_event(&note).unwrap()),
    };
    Op::sign(body, &signer).unwrap()
}

fn staging_quota<S: Storage>(storage: S) {
    use crate::storage::{MAX_STAGED_SESSIONS, MAX_STAGED_SESSIONS_PER_SOURCE};
    let topic = |index: usize| TopicId::hash(format!("quota-topic-{index}").as_bytes());
    let source = |index: usize| PeerId::hash(format!("quota-source-{index}").as_bytes());
    let stage = |owner: PeerId, topic_id: TopicId, ops: Vec<Op>, now_ms: u64| {
        storage.stage_bootstrap_ops(owner, topic_id, ops, now_ms)
    };

    let mismatch = stage(source(0), topic(0), vec![stub_op(topic(1), 1, 1)], 10);
    assert!(matches!(mismatch, Err(Error::TopicMismatch)));
    for index in 0..MAX_STAGED_SESSIONS_PER_SOURCE {
        stage(
            source(0),
            topic(index),
            vec![stub_op(topic(index), 1, 1)],
            10,
        )
        .unwrap();
    }
    let crowded = topic(MAX_STAGED_SESSIONS);
    let per_source = stage(source(0), crowded, vec![stub_op(crowded, 1, 1)], 10);
    assert!(matches!(per_source, Err(Error::Storage(_))));
    for index in MAX_STAGED_SESSIONS_PER_SOURCE..MAX_STAGED_SESSIONS {
        let owner = source(index / MAX_STAGED_SESSIONS_PER_SOURCE);
        stage(owner, topic(index), vec![stub_op(topic(index), 1, 1)], 10).unwrap();
    }
    let total = stage(source(99), crowded, vec![stub_op(crowded, 1, 1)], 10);
    assert!(matches!(total, Err(Error::Storage(_))));
    assert!(
        storage
            .staged_bootstrap_ops(&source(99), &crowded)
            .unwrap()
            .is_empty()
    );
    assert_eq!(storage.expire_bootstrap(10).unwrap(), 0);
    assert_eq!(storage.expire_bootstrap(11).unwrap(), MAX_STAGED_SESSIONS);
    stage(source(99), crowded, vec![stub_op(crowded, 1, 1)], 20).unwrap();
    assert_eq!(storage.discard_bootstrap(&source(99), &crowded).unwrap(), 1);

    // Bytes per session: four 7 MiB ops fit, a fifth is refused with its call.
    let large = (1..=5)
        .map(|seq| stub_op(crowded, seq, 7 * 1024 * 1024))
        .collect::<Vec<_>>();
    for op in &large[..4] {
        stage(source(1), crowded, vec![op.clone()], 30).unwrap();
    }
    let small = stub_op(crowded, 6, 1);
    let over = stage(source(1), crowded, vec![small, large[4].clone()], 30);
    assert!(matches!(over, Err(Error::Storage(_))));
    let full = stage(source(1), crowded, vec![large[0].clone()], 30).unwrap();
    let actor = actor_id_for(crowded, Ed25519Signer::from_bytes(&[191; 32]).peer_id());
    assert_eq!((full.ops, full.clock.get(&actor)), (4, 4));
    assert_eq!(
        storage.staged_bootstrap_ops(&source(1), &crowded).unwrap(),
        large[..4].to_vec()
    );
}

#[test]
fn memory_staging_quota() {
    staging_quota(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_staging_quota() {
    let dir = tempfile::tempdir().unwrap();
    staging_quota(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Staging survives a reopen, and a reopen after promotion shows the whole
/// history.
#[cfg(feature = "fjall")]
#[test]
fn fjall_reopen_promotion() {
    let dir = tempfile::tempdir().unwrap();
    let open = || crate::storage::FjallStorage::open(dir.path()).unwrap();
    let (alice, topic_id, ops) = invited_history(192, 30, 1);
    let (invite, history) = ops.split_last().unwrap();
    {
        let bob = bob_node(open());
        staged(receive(&bob, alice.peer_id(), topic_id, &history[..16]));
        staged(receive(&bob, alice.peer_id(), topic_id, &history[16..]));
    }

    let bob = bob_node(open());
    assert_eq!(
        bob.storage()
            .staged_bootstrap_ops(&alice.peer_id(), &topic_id)
            .unwrap(),
        history.to_vec()
    );
    assert_invisible(&bob, alice.peer_id(), topic_id, &ops);
    let ack = acked(receive(
        &bob,
        alice.peer_id(),
        topic_id,
        std::slice::from_ref(invite),
    ));
    drop(bob);

    let bob = bob_node(open());
    assert_promoted(&bob, &alice, topic_id, &ack);
}

/// A source that replaced its branch pushes the new one: the staged ops of the
/// old branch at the same positions are dropped, and the new branch promotes
/// alone once it proves the invitation.
fn replaced_branch<S: Storage>(storage: S) {
    let topic_id = TopicId::hash(b"bootstrap-replaced-branch");
    let branch = |other: u8| {
        let other = Ed25519Signer::from_bytes(&[other; 32]).peer_id();
        let (log, signer, genesis, event) =
            forked_side(MemoryStorage::new(), topic_id, 193, [other], "branch");
        (log, signer, vec![genesis, event])
    };
    let (_, source, old) = branch(194);
    let (log, _, new) = branch(195);
    let actor = actor_id_for(topic_id, source.peer_id());
    let invite = log
        .create_control_op(
            topic_id,
            actor,
            TopicControl::AddPeer { peer: bob_peer() },
            &source,
        )
        .unwrap();
    let bob = bob_node(storage);

    assert_eq!(
        staged(receive(&bob, source.peer_id(), topic_id, &old)).ops,
        2
    );
    let replaced = staged(receive(&bob, source.peer_id(), topic_id, &new));
    assert_eq!((replaced.clock.get(&actor), replaced.ops), (2, 2));
    let ack = acked(receive(
        &bob,
        source.peer_id(),
        topic_id,
        std::slice::from_ref(&invite),
    ));
    let history = [new, vec![invite]].concat();
    assert_eq!(ack.genesis, Some(history[0].id));
    assert_eq!(
        bob.storage().list_op_ids(&topic_id).unwrap(),
        history.iter().map(|op| op.id).collect()
    );
}

#[test]
fn memory_replaced_branch() {
    replaced_branch(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_replaced_branch() {
    let dir = tempfile::tempdir().unwrap();
    replaced_branch(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// The tested bootstrap bound: one staging session holds at most
/// `MAX_STAGED_OPS_PER_SESSION` ops. A call past it stores nothing, and the
/// topic stays invisible until a promotion.
fn staging_op_bound<S: Storage>(storage: S) {
    use crate::storage::MAX_STAGED_OPS_PER_SESSION;
    let topic_id = TopicId::hash(b"staging-op-bound");
    let source = PeerId::hash(b"staging-op-bound-source");
    let ops = (1..=MAX_STAGED_OPS_PER_SESSION + 1)
        .map(|seq| stub_op(topic_id, seq, 1))
        .collect::<Vec<_>>();
    for chunk in ops[..MAX_STAGED_OPS_PER_SESSION as usize].chunks(8192) {
        storage
            .stage_bootstrap_ops(source, topic_id, chunk.to_vec(), 10)
            .unwrap();
    }
    let over = storage.stage_bootstrap_ops(source, topic_id, ops[ops.len() - 1..].to_vec(), 10);
    assert!(matches!(over, Err(Error::Storage(_))), "{over:?}");
    let staged = storage.staged_topic(&source, &topic_id).unwrap();
    assert_eq!(staged.ops, MAX_STAGED_OPS_PER_SESSION);
    assert!(storage.topic_state(&topic_id).unwrap().is_none());
}

#[test]
fn memory_staging_op_bound() {
    staging_op_bound(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_staging_op_bound() {
    let dir = tempfile::tempdir().unwrap();
    staging_op_bound(crate::storage::FjallStorage::open(dir.path()).unwrap());
}
