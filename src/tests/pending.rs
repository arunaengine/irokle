//! Buffered ops are rejected only for immutable reasons and retained while a
//! later arrival can still prove them.

use super::support::*;

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
fn assert_rejects_foreign_prev<S: Storage>(storage: S) {
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
fn memory_rejects_foreign_prev() {
    assert_rejects_foreign_prev(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_rejects_foreign_prev() {
    let dir = tempfile::tempdir().unwrap();
    assert_rejects_foreign_prev(crate::storage::FjallStorage::open(dir.path()).unwrap());
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

/// Latest membership never rejects a buffered op: the author may be invited
/// by a control this node has not seen. The op is admitted once its causal
/// frontier proves membership, in either arrival order, and rejected once the
/// frontier is known and proves the opposite.
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
