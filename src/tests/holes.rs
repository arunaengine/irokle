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

/// Stored operations of the topics the reuse tests scan.
const REUSE_OPS: usize = 2000;

/// A topic of [`REUSE_OPS`] notes seeded into `storage` with one body lost,
/// held by a node over that storage. Returns the holder, the source and the ops.
fn reuse_topic<S: Corrupt>(storage: &S) -> (Irokle<S>, Irokle, Vec<Op>) {
    let (holder, source, ops) = whole_topic(storage, REUSE_OPS);
    storage.drop_op_record(&ops[REUSE_OPS / 2].id);
    (holder, source, ops)
}

/// A topic of `events` notes seeded whole into `storage`, held by a node over it.
fn whole_topic<S: Storage>(storage: &S, events: usize) -> (Irokle<S>, Irokle, Vec<Op>) {
    let source = node(42);
    let holder_signer = Ed25519Signer::from_bytes(&[44; 32]);
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [holder_signer.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for index in 0..events {
        topic
            .publish(Note {
                text: index.to_string(),
            })
            .unwrap();
    }
    let ops = oplog::topological(source.storage(), &topic.id()).unwrap();
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(ops.clone())
        .unwrap();
    let holder = Irokle::builder()
        .with_storage(storage.clone())
        .with_signer(holder_signer)
        .build()
        .unwrap();
    (holder, source, ops)
}

/// Payload and position reads `work` makes in `storage`.
fn reads<S>(
    storage: &S,
    counters: fn(&S) -> crate::CounterSnapshot,
    work: impl FnOnce(),
) -> (u64, u64) {
    let before = counters(storage);
    work();
    let after = counters(storage);
    (
        after.op_reads - before.op_reads,
        after.meta_reads - before.meta_reads,
    )
}

/// Integrity questions about a topic with a lost body read no payload, and once
/// the topic was scanned, summaries, fingerprints and request plans of the
/// unchanged store read no stored record again.
fn assert_holes_reused<S: Corrupt>(storage: S, counters: fn(&S) -> crate::CounterSnapshot) {
    let (holder, source, ops) = reuse_topic(&storage);
    let topic_id = ops[0].signed.body.topic_id;
    let remote = source.sync_summary(topic_id).unwrap();
    let (payloads, _) = reads(&storage, counters, || {
        holder.sync_fingerprint(topic_id).unwrap();
    });
    assert_eq!(payloads, 0, "the first scan decoded payloads");
    for _ in 0..3 {
        let (payloads, positions) = reads(&storage, counters, || {
            holder.sync_summary(topic_id).unwrap();
            holder.sync_fingerprint(topic_id).unwrap();
            let request = holder.plan_sync_request(source.peer_id(), &remote).unwrap();
            assert!(request.wants.contains(&ops[REUSE_OPS / 2].id));
        });
        assert_eq!(payloads, 0, "a repeated answer decoded payloads");
        assert!(positions < 64, "{positions} positions read again");
    }
    let hole = BTreeSet::from([ops[REUSE_OPS / 2].id]);
    assert_eq!(holder.topic_unresolved(topic_id).unwrap(), hole);
}

#[test]
fn memory_holes_reused() {
    assert_holes_reused(MemoryStorage::new(), MemoryStorage::counters);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_holes_reused() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    assert_holes_reused(storage, crate::storage::FjallStorage::counters);
}

/// Once a topic was found whole, appending and receiving one op and answering
/// for the topic again read a few records, not its history.
fn assert_warm_cheap<S: Corrupt>(storage: S, counters: fn(&S) -> crate::CounterSnapshot) {
    let (holder, source, ops) = whole_topic(&storage, REUSE_OPS);
    let topic_id = ops[0].signed.body.topic_id;
    holder.sync_fingerprint(topic_id).unwrap();
    let topic = source.open_topic::<Note>(topic_id).unwrap();
    let op_id = topic
        .publish(Note {
            text: "one more".into(),
        })
        .unwrap()
        .meta
        .op_id;
    let op = source.storage().get_op(&op_id).unwrap().unwrap();
    let (payloads, positions) = reads(&storage, counters, || {
        let data = sync::SyncData {
            topic_id,
            ops: vec![op],
        };
        holder
            .receive_sync_data_from(source.peer_id(), data)
            .unwrap();
        let remote = source.sync_summary(topic_id).unwrap();
        assert_eq!(
            holder.sync_fingerprint(topic_id).unwrap().fingerprint,
            remote.fingerprint
        );
        holder.plan_sync_request(source.peer_id(), &remote).unwrap();
    });
    assert!(payloads < 8, "{payloads} payloads read");
    assert!(positions < 64, "{positions} positions read");
}

#[test]
fn memory_warm_cheap() {
    assert_warm_cheap(MemoryStorage::new(), MemoryStorage::counters);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_warm_cheap() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    assert_warm_cheap(storage, crate::storage::FjallStorage::counters);
}

/// An endpoint whose id is the peer id of `Ed25519Signer::from_bytes(&[seed; 32])`.
#[cfg(feature = "iroh")]
async fn keyed_endpoint(seed: u8) -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

/// A matching fingerprint is no evidence while the answering side has not
/// finished scanning: it answers with summaries and records no ack until then.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evidence_waits_scan() {
    let (holder, source, ops) = whole_topic(&MemoryStorage::new(), 40);
    let topic_id = ops[0].signed.body.topic_id;
    holder.set_step_reads(8);
    let holder_net = net::IrohNet::new(keyed_endpoint(44).await, holder.clone()).unwrap();
    let source_id = keyed_endpoint(42).await.id();
    let messages = vec![
        sync::SyncMessage::Open(source.sync_open(topic_id)),
        sync::SyncMessage::Fingerprint(source.sync_fingerprint(topic_id).unwrap()),
    ];
    let mut summaries = 0;
    loop {
        let replies = holder_net
            .handle_messages(source_id, messages.clone())
            .unwrap();
        let acked = holder
            .storage()
            .peer_ack(&source.peer_id(), &topic_id)
            .unwrap();
        if replies
            .iter()
            .any(|reply| matches!(reply, sync::SyncMessage::Fingerprint(_)))
        {
            assert!(acked.is_some(), "a matching answer recorded no ack");
            break;
        }
        assert!(acked.is_none(), "an unfinished scan recorded an ack");
        summaries += 1;
        assert!(summaries < 20, "the scan never finished");
    }
    assert!(
        summaries >= 2,
        "{summaries} answers before the scan finished"
    );
    holder_net.shutdown().await;
}

/// A member removed while the answering side still scans gets no ack from the
/// scan's verdict: its matching fingerprint is answered, but not recorded.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_records_nothing() {
    let (holder, source, ops) = whole_topic(&MemoryStorage::new(), 40);
    let topic_id = ops[0].signed.body.topic_id;
    source.set_step_reads(8);
    let source_net = net::IrohNet::new(keyed_endpoint(42).await, source.clone()).unwrap();
    let holder_id = keyed_endpoint(44).await.id();
    let answer = || {
        let messages = vec![
            sync::SyncMessage::Open(holder.sync_open(topic_id)),
            sync::SyncMessage::Fingerprint(holder.sync_fingerprint(topic_id).unwrap()),
        ];
        let replies = source_net.handle_messages(holder_id, messages).unwrap();
        let acked = source
            .storage()
            .peer_ack(&holder.peer_id(), &topic_id)
            .unwrap();
        assert!(acked.is_none(), "evidence recorded for a removed member");
        replies
            .iter()
            .any(|reply| matches!(reply, sync::SyncMessage::Fingerprint(_)))
    };
    assert!(!answer(), "an unfinished scan matched");
    let topic = source.open_topic::<Note>(topic_id).unwrap();
    topic.remove_peer(holder.peer_id()).unwrap();
    let history = oplog::topological(source.storage(), &topic_id).unwrap();
    let data = sync::SyncData {
        topic_id,
        ops: history,
    };
    holder
        .receive_sync_data_from(source.peer_id(), data)
        .unwrap();
    let mut answers = 1;
    while !answer() {
        answers += 1;
        assert!(answers < 20, "the scan never finished");
    }
    source_net.shutdown().await;
}

/// A damaged topic's scan paused between steps holds nothing: a healthy topic
/// publishes and answers a control exchange meanwhile, then the scan resumes.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthy_while_paused() {
    let storage = MemoryStorage::new();
    let (holder, source, ops) = reuse_topic(&storage);
    let damaged = ops[0].signed.body.topic_id;
    holder.set_step_reads(64);
    let holder_net = net::IrohNet::new(keyed_endpoint(44).await, holder.clone()).unwrap();
    let source_id = keyed_endpoint(42).await.id();
    let open = |topic_id| sync::SyncMessage::Open(source.sync_open(topic_id));
    let first = holder_net
        .handle_messages(source_id, vec![open(damaged)])
        .unwrap();
    assert_eq!(first.len(), 1);
    let healthy = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: [holder.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(source.storage(), &healthy.id()).unwrap();
    let data = sync::SyncData {
        topic_id: healthy.id(),
        ops: genesis,
    };
    holder
        .receive_sync_data_from(source.peer_id(), data)
        .unwrap();
    let local = holder.open_topic::<Note>(healthy.id()).unwrap();
    local
        .publish(Note {
            text: "healthy".into(),
        })
        .unwrap();
    let fingerprint = holder.sync_fingerprint(healthy.id()).unwrap();
    let messages = vec![
        open(healthy.id()),
        sync::SyncMessage::Fingerprint(fingerprint),
    ];
    let replies = holder_net.handle_messages(source_id, messages).unwrap();
    assert!(
        replies
            .iter()
            .any(|reply| matches!(reply, sync::SyncMessage::Fingerprint(_)))
    );
    let hole = BTreeSet::from([ops[REUSE_OPS / 2].id]);
    assert_eq!(holder.topic_unresolved(damaged).unwrap(), hole);
    holder_net.shutdown().await;
}

/// One fingerprint answer of a damaged topic scans it at most once and decodes
/// no payload; the next answer of the unchanged store scans nothing.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fingerprint_scans_once() {
    let storage = MemoryStorage::new();
    let (holder, source, ops) = reuse_topic(&storage);
    let topic_id = ops[0].signed.body.topic_id;
    let holder_net = net::IrohNet::new(keyed_endpoint(44).await, holder.clone()).unwrap();
    let source_id = keyed_endpoint(42).await.id();
    let messages = vec![
        sync::SyncMessage::Open(source.sync_open(topic_id)),
        sync::SyncMessage::Fingerprint(source.sync_fingerprint(topic_id).unwrap()),
    ];
    let answer = || {
        let replies = holder_net
            .handle_messages(source_id, messages.clone())
            .unwrap();
        assert!(
            replies
                .iter()
                .any(|reply| matches!(reply, sync::SyncMessage::Summary(_)))
        );
    };
    let (payloads, positions) = reads(&storage, MemoryStorage::counters, answer);
    assert_eq!(payloads, 0, "the answer decoded payloads");
    assert!(
        positions <= 2 * REUSE_OPS as u64 + 64,
        "{positions} positions read"
    );
    let (payloads, positions) = reads(&storage, MemoryStorage::counters, answer);
    assert_eq!(payloads, 0);
    assert!(positions < 64, "{positions} positions read again");
    holder_net.shutdown().await;
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
