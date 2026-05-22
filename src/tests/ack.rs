use super::support::*;
#[cfg(feature = "fjall")]
use crate::storage as crate_storage;

fn assert_clears_satisfied<S: Storage>(storage: S) {
    let ack_signer = Ed25519Signer::from_bytes(&[99; 32]);
    let peer = ack_signer.peer_id();
    let irokle = Irokle::with_storage(storage.clone(), NodeConfig::default()).unwrap();
    let topic = irokle
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let satisfied = topic
        .publish(Note {
            text: "satisfied".into(),
        })
        .unwrap();
    let unsatisfied = topic
        .publish(Note {
            text: "unsatisfied".into(),
        })
        .unwrap();
    let other_topic = irokle
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let other = other_topic
        .publish(Note {
            text: "other".into(),
        })
        .unwrap();

    irokle
        .put_sync_obligation(peer, topic.id(), [satisfied.meta.op_id].into())
        .unwrap();
    irokle
        .put_sync_obligation(peer, topic.id(), [unsatisfied.meta.op_id].into())
        .unwrap();
    irokle
        .put_sync_obligation(peer, other_topic.id(), [other.meta.op_id].into())
        .unwrap();

    let mut clock = ActorClock::new();
    clock.observe(satisfied.meta.actor_id, satisfied.meta.actor_seq);
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [satisfied.meta.op_id].into(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();
    irokle.apply_sync_ack(&ack).unwrap();

    let report = irokle.sync_report(peer, topic.id()).unwrap();
    assert_eq!(report.obligations.len(), 1);
    assert_eq!(
        report.obligations[0].op_ids,
        [unsatisfied.meta.op_id].into()
    );

    let other_report = irokle.sync_report(peer, other_topic.id()).unwrap();
    assert_eq!(other_report.obligations.len(), 1);
    assert_eq!(
        other_report.obligations[0].op_ids,
        [other.meta.op_id].into()
    );
}

#[test]
fn memory_clears_satisfied() {
    assert_clears_satisfied(MemoryStorage::new());
}

fn assert_stale_ack_ignored<S: Storage>(storage: S) {
    let ack_signer = Ed25519Signer::from_bytes(&[96; 32]);
    let peer = ack_signer.peer_id();
    let alice = Irokle::with_storage(storage.clone(), NodeConfig::default()).unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let first = topic
        .publish(Note {
            text: "first".into(),
        })
        .unwrap();
    let second = topic
        .publish(Note {
            text: "second".into(),
        })
        .unwrap();

    let mut fresh_clock = ActorClock::new();
    fresh_clock.observe(second.meta.actor_id, second.meta.actor_seq);
    let mut fresh = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [second.meta.op_id].into(),
        clock: fresh_clock,
        signature: None,
    };
    fresh.sign(&ack_signer).unwrap();
    alice.apply_sync_ack(&fresh).unwrap();

    let mut stale_clock = ActorClock::new();
    stale_clock.observe(first.meta.actor_id, first.meta.actor_seq);
    let mut stale = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [first.meta.op_id].into(),
        clock: stale_clock,
        signature: None,
    };
    stale.sign(&ack_signer).unwrap();
    alice.apply_sync_ack(&stale).unwrap();

    let stored = storage.peer_ack(&peer, &topic.id()).unwrap().unwrap();
    assert_eq!(stored.heads, [second.meta.op_id].into());
    assert!(stored.clock.get(&second.meta.actor_id) >= second.meta.actor_seq);
}

#[test]
fn memory_ignores_stale_ack() {
    assert_stale_ack_ignored(MemoryStorage::new());
}

#[test]
fn unsigned_ack_keeps_obligation() {
    let alice = node(47);
    let bob = node(48);
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic.publish(Note { text: "ack".into() }).unwrap();
    alice
        .put_sync_obligation(bob.peer_id(), topic.id(), [record.meta.op_id].into())
        .unwrap();

    let err = alice
        .apply_sync_ack(&sync::SyncAck {
            topic_id: topic.id(),
            peer_id: bob.peer_id(),
            accepted: BTreeSet::new(),
            heads: [record.meta.op_id].into(),
            clock: ActorClock::new(),
            signature: None,
        })
        .unwrap_err();

    assert!(matches!(err, Error::MissingSignature));
    assert_eq!(
        alice
            .sync_report(bob.peer_id(), topic.id())
            .unwrap()
            .obligations
            .len(),
        1
    );
}

#[test]
fn clock_clears_obligation() {
    let alice = node(28);
    let ack_signer = Ed25519Signer::from_bytes(&[98; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "clocked".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(peer, topic.id(), [record.meta.op_id].into())
        .unwrap();

    let mut clock = ActorClock::new();
    clock.observe(record.meta.actor_id, record.meta.actor_seq);
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: BTreeSet::new(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();
    alice.apply_sync_ack(&ack).unwrap();

    assert!(
        alice
            .sync_report(peer, topic.id())
            .unwrap()
            .obligations
            .is_empty()
    );
}

#[test]
fn rejects_future_clock() {
    let alice = node(94);
    let ack_signer = Ed25519Signer::from_bytes(&[95; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "future".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(peer, topic.id(), [record.meta.op_id].into())
        .unwrap();

    let mut clock = ActorClock::new();
    clock.observe(record.meta.actor_id, record.meta.actor_seq + 1);
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: BTreeSet::new(),
        clock,
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();

    let err = alice.apply_sync_ack(&ack).unwrap_err();

    assert!(matches!(err, Error::InvalidSyncAck(_)));
    assert!(
        alice
            .storage()
            .peer_ack(&peer, &topic.id())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        alice
            .sync_report(peer, topic.id())
            .unwrap()
            .obligations
            .len(),
        1
    );
}

#[test]
fn rejects_unknown_heads() {
    let alice = node(96);
    let ack_signer = Ed25519Signer::from_bytes(&[97; 32]);
    let peer = ack_signer.peer_id();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: peer,
        accepted: BTreeSet::new(),
        heads: [OpId::hash(b"unknown-head")].into(),
        clock: ActorClock::new(),
        signature: None,
    };
    ack.sign(&ack_signer).unwrap();

    let err = alice.apply_sync_ack(&ack).unwrap_err();

    assert!(matches!(err, Error::InvalidSyncAck(_)));
    assert!(
        alice
            .storage()
            .peer_ack(&peer, &topic.id())
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_clears_satisfied() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_clears_satisfied(storage);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_ignores_stale_ack() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    assert_stale_ack_ignored(storage);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_clear_persists() {
    let dir = tempfile::tempdir().unwrap();
    let ack_signer = Ed25519Signer::from_bytes(&[97; 32]);
    let peer = ack_signer.peer_id();
    let (topic_id, unsatisfied_id) = {
        let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
        let irokle = Irokle::with_storage(storage, NodeConfig::default()).unwrap();
        let topic = irokle
            .create_topic::<Note>(TopicConfig {
                initial_peers: [peer].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        let satisfied = topic
            .publish(Note {
                text: "durable-satisfied".into(),
            })
            .unwrap();
        let unsatisfied = topic
            .publish(Note {
                text: "durable-unsatisfied".into(),
            })
            .unwrap();

        irokle
            .put_sync_obligation(peer, topic.id(), [satisfied.meta.op_id].into())
            .unwrap();
        irokle
            .put_sync_obligation(peer, topic.id(), [unsatisfied.meta.op_id].into())
            .unwrap();
        let mut clock = ActorClock::new();
        clock.observe(satisfied.meta.actor_id, satisfied.meta.actor_seq);
        let mut ack = sync::SyncAck {
            topic_id: topic.id(),
            peer_id: peer,
            accepted: BTreeSet::new(),
            heads: [satisfied.meta.op_id].into(),
            clock,
            signature: None,
        };
        ack.sign(&ack_signer).unwrap();
        irokle.apply_sync_ack(&ack).unwrap();

        (topic.id(), unsatisfied.meta.op_id)
    };

    let storage = crate_storage::FjallStorage::open(dir.path()).unwrap();
    let obligations = storage.sync_obligations(&peer, &topic_id).unwrap();
    assert_eq!(obligations.len(), 1);
    assert_eq!(obligations[0].op_ids, [unsatisfied_id].into());
}
