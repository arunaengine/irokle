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
