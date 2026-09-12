//! Evidence and effects must stay bound to the branch they were read from when
//! a genesis reset commits in the middle of building them.

use super::support::*;

use crate::oplog::Oplog;
use crate::sync::{SyncData, SyncEngine};

/// Two genesis candidates for one topic signed by the same author, so their
/// events take the same actor positions. `old` has the larger genesis id and
/// loses the tie-break against `new`.
struct Branches {
    topic_id: TopicId,
    author: Ed25519Signer,
    member: Ed25519Signer,
    third: Ed25519Signer,
    old: (Op, Op),
    new: (Op, Op),
}

fn branches(seed: u8) -> Branches {
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
fn reset_to_new<S: Storage>(storage: &S, branches: &Branches) {
    let admitted = Oplog::with_storage(storage.clone())
        .receive_ops_from_peer_evicting(
            Some(branches.author.peer_id()),
            vec![branches.new.0.clone(), branches.new.1.clone()],
        )
        .unwrap();
    assert_eq!(admitted.evictions.len(), 1, "the old branch was replaced");
}

/// An ack is built while a reset replaces the branch it started reading. It
/// must not pair the old genesis with the new branch's clock, which would prove
/// an old-branch position the member never held.
#[test]
fn ack_keeps_branch() {
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
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let member_log = Oplog::with_storage(storage.clone());
    member_log
        .receive_ops_from_peer(Some(author), vec![branches.old.0.clone()])
        .unwrap();
    let member_sync = SyncEngine::new(member_log, member);

    let gate = Arc::new(Gate::default());
    let _release = gate.releaser();
    // Pause right after the member read its view of the old branch.
    storage.arm_read(GatePoint::View(topic_id), Arc::clone(&gate));
    let acking = thread::spawn({
        let gate = Arc::clone(&gate);
        move || {
            let received = member_sync.receive_data(
                author,
                member,
                SyncData {
                    topic_id,
                    ops: Vec::new(),
                },
            );
            gate.skip();
            received
        }
    });
    gate.wait_arrival();
    storage.disarm_read();
    reset_to_new(&storage, &branches);
    gate.release();
    let (mut ack, _) = acking.join().unwrap().unwrap();
    ack.sign(&branches.member).unwrap();

    let _ = author_sync.apply_ack(&ack);
    assert!(
        author_log
            .storage()
            .has_sync_obligations(&member, &topic_id)
            .unwrap(),
        "an ack mixing two branches cleared old-branch work: {ack:?}"
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
