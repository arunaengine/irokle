//! Many repair holes behind a clock that already claims them: admitted in one
//! batch, and repaired over a real exchange whose pages hold only a few ops.

use super::support::*;

/// Holes on one actor chain arrive in one batch whose id order disagrees with
/// their causal order. Each hole's ancestry walk passes the older holes, so
/// they must be admitted oldest first.
fn assert_holes_admit<S: Corrupt>(storage: S) {
    let source = node(31);
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..40 {
        topic
            .publish(Note {
                text: index.to_string(),
            })
            .unwrap();
    }
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    let log = oplog::Oplog::with_storage(storage.clone());
    log.receive_ops(ops.clone()).unwrap();
    let mut holes = ops[2..].iter().step_by(2).cloned().collect::<Vec<_>>();
    for hole in &holes {
        storage.drop_op_record(&hole.id);
    }
    log.recheck_topics().unwrap();
    assert_eq!(
        log.topic_unresolved(&topic.id()).unwrap().len(),
        holes.len()
    );
    holes.reverse();
    log.receive_ops(holes).unwrap();
    log.recheck_topics().unwrap();
    assert!(log.topic_unresolved(&topic.id()).unwrap().is_empty());
}

#[test]
fn memory_holes_admit() {
    assert_holes_admit(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_holes_admit() {
    let dir = tempfile::tempdir().unwrap();
    assert_holes_admit(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Every call either finishes the repair or reports `WouldBlock` with fewer
/// holes than before, and no call reports success while a hole is left.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn holes_repair_sliced() {
    let bind = || async {
        iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
            .bind()
            .await
            .unwrap()
    };
    let (alice_endpoint, bob_endpoint) = (bind().await, bind().await);
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .with_peer_whitelist([alice.peer_id()])
        .build()
        .unwrap();
    let alice_net = net::IrohNet::new(alice_endpoint, alice.clone()).unwrap();
    // A page of the responder holds about seven of these ops.
    let limits = crate::net::StreamLimits {
        bytes: 64 * 1024,
        ..crate::net::StreamLimits::default()
    };
    let bob_net = Arc::new(
        net::IrohNet::new(bob_endpoint, bob.clone())
            .unwrap()
            .with_stream_limits(limits),
    );
    bob_net.start_accept_loop().unwrap();
    let bob_addr = super::iroh::ready_addr(bob_net.endpoint()).await;

    let topic = bob
        .create_topic::<Note>(TopicConfig {
            initial_peers: [alice.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    for index in 0..1200 {
        topic
            .publish(Note {
                text: format!("{index:0>8192}"),
            })
            .unwrap();
    }
    let ops = oplog::topological(bob.storage(), &topic_id).unwrap();
    oplog::Oplog::with_storage(alice.storage().clone())
        .receive_ops(ops.clone())
        .unwrap();
    for op in ops[2..].iter().step_by(2) {
        alice.storage().drop_op_record(&op.id);
    }
    alice.recheck_topics().unwrap();
    let clock = alice.storage().actor_clock(&topic_id).unwrap();
    assert_eq!(clock, bob.storage().actor_clock(&topic_id).unwrap());

    let mut holes = alice.topic_unresolved(topic_id).unwrap().len();
    assert_eq!(holes, 600);
    let mut calls = 0;
    while holes > 0 {
        calls += 1;
        assert!(calls <= 32, "{holes} holes left after {calls} calls");
        let result = alice_net.sync_now(bob_addr.clone(), topic_id).await;
        let left = alice.topic_unresolved(topic_id).unwrap().len();
        if left > 0 {
            assert_eq!(
                result.as_ref().map_err(std::io::Error::kind),
                Err(std::io::ErrorKind::WouldBlock),
                "{left} holes left: {result:?}"
            );
            assert!(left < holes, "a call repaired nothing");
        } else {
            result.unwrap();
        }
        assert_eq!(alice.storage().actor_clock(&topic_id).unwrap(), clock);
        holes = left;
    }
    assert!(calls > 1, "the repair was not sliced");
    alice.recheck_topics().unwrap();
    assert!(alice.topic_unresolved(topic_id).unwrap().is_empty());
    assert_eq!(
        alice.sync_fingerprint(topic_id).unwrap().fingerprint,
        bob.sync_fingerprint(topic_id).unwrap().fingerprint
    );

    alice_net.shutdown().await;
    bob_net.shutdown().await;
}
