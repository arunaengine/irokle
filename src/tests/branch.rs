//! Evidence and effects must stay bound to the branch they were read from when
//! a genesis reset commits in the middle of building them.

use super::support::*;

use crate::oplog::Oplog;

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
