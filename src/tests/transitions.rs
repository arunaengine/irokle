//! A captured network pull crosses a winning branch at controlled boundaries.

use crate::tests::support::*;
use std::collections::BTreeMap;
use std::time::Duration;

struct Side<S: Storage> {
    node: Irokle<StaleReadStorage<S>>,
    net: Arc<net::IrohNet<StaleReadStorage<S>>>,
    address: iroh::EndpointAddr,
}

async fn side<S: Storage>(store: S, seed: [u8; 32]) -> Side<S> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&seed))
        .alpns(vec![net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let node = Irokle::with_storage(
        StaleReadStorage::new(store),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&seed),
            peer_whitelist: None,
            ..NodeConfig::default()
        },
    )
    .unwrap()
    .with_request_items(3)
    .with_page_visits(1);
    let net = Arc::new(
        net::IrohNet::new(endpoint, node.clone())
            .unwrap()
            .with_stream_limits(net::StreamLimits {
                bytes: 16384,
                messages: 8,
                ..Default::default()
            }),
    );
    net.start_accept_loop().unwrap();
    let address = crate::tests::iroh::ready_addr(net.endpoint()).await;
    Side { node, net, address }
}

fn branches(provisional: bool) -> (Vec<Op>, Vec<Op>) {
    let source = crate::tests::progress::reverse_chain(MemoryStorage::new(), 40);
    let original = oplog::topological(source.log.storage(), &source.topic_id).unwrap();
    let owner = Ed25519Signer::from_bytes(&[230; 32]);
    let mut signers = BTreeMap::from([(owner.peer_id(), owner.clone())]);
    for index in 0..40_u64 {
        let mut seed = [11; 32];
        seed[..8].copy_from_slice(&index.to_le_bytes());
        let signer = Ed25519Signer::from_bytes(&seed);
        signers.insert(signer.peer_id(), signer);
    }
    let mut histories = Vec::new();
    for variant in [3, 4] {
        let mut ids = BTreeMap::new();
        let mut ops = Vec::new();
        for op in &original {
            let mut body = op.signed.body.clone();
            body.deps = body.deps.iter().map(|id| ids[id]).collect();
            body.actor_prev = body.actor_prev.map(|id| ids[&id]);
            if let TopicPayload::Genesis(genesis) = &mut body.payload {
                genesis.replication_policy = ReplicationPolicy::all().with_max_sync_peers(variant);
                if provisional {
                    genesis.initial_peers.remove(&source.reader);
                }
            }
            let signed = Op::sign(body, &signers[&op.signed.body.author]).unwrap();
            ids.insert(op.id, signed.id);
            ops.push(signed);
        }
        let log = oplog::Oplog::new();
        log.receive_ops(ops.clone()).unwrap();
        if provisional {
            ops.push(
                log.create_control_op(
                    source.topic_id,
                    actor_id_for(source.topic_id, owner.peer_id()),
                    TopicControl::AddPeer {
                        peer: source.reader,
                    },
                    &owner,
                )
                .unwrap(),
            );
        }
        histories.push(ops);
    }
    histories.sort_by_key(|ops| std::cmp::Reverse(ops[0].id));
    let new = histories.pop().unwrap();
    let old = histories.pop().unwrap();
    assert!(new[0].id < old[0].id);
    for (before, after) in old.iter().zip(&new) {
        assert_eq!(before.signed.body.actor_id, after.signed.body.actor_id);
        assert_eq!(before.signed.body.actor_seq, after.signed.body.actor_seq);
        assert_ne!(before.id, after.id);
    }
    (old, new)
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(120), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("transition lost observable progress");
}

fn install<S: Storage>(side: &Side<S>, ops: &[Op]) {
    oplog::Oplog::with_storage(side.node.storage().clone())
        .receive_ops(ops.to_vec())
        .unwrap();
}

async fn drain<S: Storage>(side: &Side<S>) {
    side.net.shutdown().await;
    let owned = side.net.owned_bytes();
    assert_eq!(owned.jobs, 0);
    assert!(owned.current.values().all(|bytes| *bytes == 0), "{owned:?}");
    assert_eq!(side.net.plan_counts(), (0, 0));
    assert_eq!(side.net.retained_goals(), 0);
}

fn exact<S: Storage>(side: &Side<S>, expected: &[Op]) {
    let reference = oplog::Oplog::new();
    reference.receive_ops(expected.to_vec()).unwrap();
    let topic = expected[0].signed.body.topic_id;
    let store = side.node.storage();
    let actual = store.topic_view(&topic, None).unwrap().unwrap();
    let view = reference
        .storage()
        .topic_view(&topic, None)
        .unwrap()
        .unwrap();
    assert_eq!(actual.state, view.state);
    assert_eq!(actual.clock, view.clock);
    assert_eq!(actual.tips, view.tips);
    assert_eq!(actual.fingerprint, view.fingerprint);
    assert_eq!(
        store.list_op_ids(&topic).unwrap(),
        expected.iter().map(|op| op.id).collect()
    );
    assert!(side.node.topic_unresolved(topic).unwrap().is_empty());
    for op in expected {
        assert_eq!(store.get_op(&op.id).unwrap().as_ref(), Some(op));
        assert_eq!(
            store.get_meta(&op.id).unwrap(),
            reference.storage().get_meta(&op.id).unwrap()
        );
    }
    assert_eq!(
        store
            .pending_puts
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(store.provisional_topics().unwrap().is_empty());
}

async fn transition<S: Storage>(source: S, receiver: S, boundary: &'static str, faults: u8) {
    println!("IROKLE_TRANSITION boundary={boundary} faults={faults}");
    let provisional = boundary == "activation";
    let (old, new) = branches(provisional);
    let topic = old[0].signed.body.topic_id;
    let alice = side(source, [230; 32]).await;
    let mut bob = side(receiver.clone(), [231; 32]).await;
    install(&alice, &old);
    let summary = sync::SyncMessage::Summary(alice.node.sync_summary(topic).unwrap());
    assert!(2 * net::framed_message_len(&summary).unwrap() <= 16384);
    if !provisional {
        install(&bob, &old[..1]);
    }
    alice
        .node
        .put_sync_obligation(bob.node.peer_id(), topic, [old.last().unwrap().id].into())
        .unwrap();
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    let server_boundary = matches!(boundary, "continuation" | "plan" | "ack");
    let gated = if server_boundary {
        alice.node.storage()
    } else {
        bob.node.storage()
    };
    let skip = if boundary == "activation" { 0 } else { 2 };
    gated.arm_read_after(GatePoint::Sync(topic, boundary), skip, Arc::clone(&gate));
    let pending = tokio::spawn({
        let net = Arc::clone(&bob.net);
        let address = alice.address.clone();
        async move {
            for attempt in 0..64 {
                match net.sync_now(address.clone(), topic).await {
                    Err(error)
                        if attempt < 63 && error.kind() == std::io::ErrorKind::WouldBlock => {}
                    result => return result,
                }
            }
            unreachable!()
        }
    });
    wait_for(|| gate.arrived() || pending.is_finished()).await;
    if pending.is_finished() {
        panic!("boundary {boundary} was not reached: {:?}", pending.await);
    }
    assert!(
        gate.arrived() && !gate.has_left() && !pending.is_finished(),
        "boundary {boundary} not held"
    );
    assert!(
        alice
            .node
            .storage()
            .sync_counts
            .lock()
            .unwrap()
            .get("continuation")
            .copied()
            .unwrap_or(0)
            > 0
    );
    if !matches!(boundary, "continuation" | "activation") {
        assert!(
            bob.node
                .storage()
                .sync_counts
                .lock()
                .unwrap()
                .get("request")
                .copied()
                .unwrap_or(0)
                >= 3
        );
    }
    if provisional {
        assert!(bob.node.storage().topic_state(&topic).unwrap().is_none());
    }

    let held = if faults & 4 != 0 {
        Some(alice.net.hold_planners().await)
    } else {
        None
    };
    let mut extras = Vec::new();
    let mut queued = Vec::new();
    if held.is_some() {
        let summary = alice.node.sync_summary(topic).unwrap();
        for index in 0..17_u64 {
            let mut seed = [11; 32];
            seed[..8].copy_from_slice(&index.to_le_bytes());
            let extra = side(MemoryStorage::new(), seed).await;
            install(&extra, &old[..1]);
            let mut request = extra
                .node
                .plan_sync_request(alice.node.peer_id(), &summary)
                .unwrap();
            request.credit = sync::SyncCredit {
                ops: 1,
                bytes: 16 * 1024,
            };
            queued.push(tokio::spawn({
                let net = Arc::clone(&extra.net);
                let address = alice.address.clone();
                let open = extra.node.sync_open(topic);
                async move {
                    net.sync_with(
                        address,
                        &[
                            sync::SyncMessage::Open(open),
                            sync::SyncMessage::Request(request),
                        ],
                    )
                    .await
                }
            }));
            extras.push(extra);
            wait_for(|| {
                alice.net.plan_counts() == (((index + 1) as usize).min(16), (index + 1) as usize)
            })
            .await;
        }
        let heartbeat = extras[0]
            .net
            .sync_with(
                alice.address.clone(),
                &[sync::SyncMessage::Open(extras[0].node.sync_open(topic))],
            )
            .await
            .unwrap();
        assert!(!heartbeat.is_empty());
    }
    if faults & 1 != 0 {
        pending.abort();
    }
    install(&alice, &new);
    install(&bob, if provisional { &new } else { &new[..1] });
    assert!(
        alice
            .node
            .storage()
            .topic_view(&topic, None)
            .unwrap()
            .unwrap()
            .epoch
            > 0
    );
    if !provisional {
        assert!(
            bob.node
                .storage()
                .topic_view(&topic, None)
                .unwrap()
                .unwrap()
                .epoch
                > 0
        );
    }
    alice
        .node
        .put_sync_obligation(bob.node.peer_id(), topic, [new.last().unwrap().id].into())
        .unwrap();
    assert!(
        alice
            .node
            .storage()
            .has_sync_obligations(&bob.node.peer_id(), &topic)
            .unwrap()
    );
    assert!(!gate.has_left());
    drop(release);
    drop(held);
    let result = pending.await;
    if faults & 1 != 0 {
        assert!(result.unwrap_err().is_cancelled());
    } else {
        if let Err(error) = result.unwrap() {
            assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::InvalidData
                ),
                "{error}"
            );
        }
    }
    for queued in queued {
        let response = queued.await.unwrap().unwrap();
        assert!(
            response
                .messages()
                .iter()
                .any(|message| matches!(message, sync::SyncMessage::Failure(_)))
        );
    }
    for extra in &extras {
        drain(extra).await;
    }
    if faults & 2 != 0 {
        drain(&bob).await;
        bob = side(receiver, [231; 32]).await;
    }
    for attempt in 0..64 {
        match bob.net.sync_now(alice.address.clone(), topic).await {
            Ok(()) => break,
            Err(error) => assert!(
                attempt < 63 && error.kind() == std::io::ErrorKind::WouldBlock,
                "{boundary}: {error}; held={} unresolved={:?} requests={:?} source={:?}",
                bob.node.storage().list_op_ids(&topic).unwrap().len(),
                bob.node.topic_unresolved(topic).unwrap(),
                bob.node.storage().sync_counts.lock().unwrap(),
                alice.node.storage().sync_counts.lock().unwrap(),
            ),
        }
    }
    exact(&alice, &new);
    exact(&bob, &new);
    let ack = alice
        .node
        .storage()
        .peer_ack(&bob.node.peer_id(), &topic)
        .unwrap()
        .unwrap();
    assert_eq!(ack.genesis, Some(new[0].id));
    assert_eq!(ack.clock, bob.node.storage().actor_clock(&topic).unwrap());
    assert!(
        !alice
            .node
            .storage()
            .has_sync_obligations(&bob.node.peer_id(), &topic)
            .unwrap()
    );
    drain(&bob).await;
    drain(&alice).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_boundaries() {
    for boundary in [
        "request",
        "continuation",
        "plan",
        "page",
        "ack",
        "activation",
    ] {
        transition(MemoryStorage::new(), MemoryStorage::new(), boundary, 0).await;
    }
}

async fn append_case<S: Storage>(source: S, receiver: S, boundary: &'static str) {
    let (mut history, _) = branches(false);
    let topic = history[0].signed.body.topic_id;
    let alice = side(source, [230; 32]).await;
    let bob = side(receiver, [231; 32]).await;
    install(&alice, &history);
    install(&bob, &history[..1]);
    let captured = alice.node.storage().actor_clock(&topic).unwrap();
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    let gated = if matches!(boundary, "continuation" | "plan") {
        alice.node.storage()
    } else {
        bob.node.storage()
    };
    gated.arm_read_after(GatePoint::Sync(topic, boundary), 2, Arc::clone(&gate));
    let pending = tokio::spawn({
        let net = Arc::clone(&bob.net);
        let address = alice.address.clone();
        async move {
            for attempt in 0..64 {
                match net.sync_now(address.clone(), topic).await {
                    Err(error)
                        if attempt < 63 && error.kind() == std::io::ErrorKind::WouldBlock => {}
                    result => return result,
                }
            }
            unreachable!()
        }
    });
    wait_for(|| gate.arrived() || pending.is_finished()).await;
    assert!(gate.arrived() && !pending.is_finished());
    let resumed = alice.node.sync_engine().page_work().resumed;
    let appended = oplog::Oplog::with_storage(alice.node.storage().clone())
        .create_event_op(
            topic,
            actor_id_for(topic, alice.node.peer_id()),
            EventEnvelope::encode_event(&Note {
                text: "same branch append".into(),
            })
            .unwrap(),
            alice.node.signer(),
        )
        .unwrap();
    history.push(appended.clone());
    alice
        .node
        .put_sync_obligation(bob.node.peer_id(), topic, [appended.id].into())
        .unwrap();
    assert_eq!(
        alice
            .node
            .storage()
            .topic_view(&topic, None)
            .unwrap()
            .unwrap()
            .epoch,
        0
    );
    drop(release);
    pending.await.unwrap().unwrap();
    assert!(
        bob.node
            .storage()
            .actor_clock(&topic)
            .unwrap()
            .dominates(&captured)
    );
    assert!(alice.node.sync_engine().page_work().resumed > resumed);
    if bob.node.storage().get_op(&appended.id).unwrap().is_none() {
        assert!(
            alice
                .node
                .storage()
                .has_sync_obligations(&bob.node.peer_id(), &topic)
                .unwrap()
        );
    }
    for attempt in 0..64 {
        match bob.net.sync_now(alice.address.clone(), topic).await {
            Ok(()) => break,
            Err(error) => assert!(
                attempt < 63 && error.kind() == std::io::ErrorKind::WouldBlock,
                "{error}"
            ),
        }
    }
    exact(&alice, &history);
    exact(&bob, &history);
    assert!(
        !alice
            .node
            .storage()
            .has_sync_obligations(&bob.node.peer_id(), &topic)
            .unwrap()
    );
    drain(&bob).await;
    drain(&alice).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_append_boundaries() {
    for boundary in ["request", "continuation", "plan", "page"] {
        append_case(MemoryStorage::new(), MemoryStorage::new(), boundary).await;
    }
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_append_boundaries() {
    for boundary in ["request", "continuation", "plan", "page"] {
        let source = tempfile::tempdir().unwrap();
        let receiver = tempfile::tempdir().unwrap();
        append_case(
            crate::storage::FjallStorage::open(source.path()).unwrap(),
            crate::storage::FjallStorage::open(receiver.path()).unwrap(),
            boundary,
        )
        .await;
    }
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_boundaries() {
    for boundary in [
        "request",
        "continuation",
        "plan",
        "page",
        "ack",
        "activation",
    ] {
        let source = tempfile::tempdir().unwrap();
        let receiver = tempfile::tempdir().unwrap();
        transition(
            crate::storage::FjallStorage::open(source.path()).unwrap(),
            crate::storage::FjallStorage::open(receiver.path()).unwrap(),
            boundary,
            0,
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_faults() {
    for faults in [1, 2, 4, 7] {
        transition(
            MemoryStorage::new(),
            MemoryStorage::new(),
            "request",
            faults,
        )
        .await;
    }
}

#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_faults() {
    for faults in [1, 2, 4, 7] {
        let source = tempfile::tempdir().unwrap();
        let receiver = tempfile::tempdir().unwrap();
        transition(
            crate::storage::FjallStorage::open(source.path()).unwrap(),
            crate::storage::FjallStorage::open(receiver.path()).unwrap(),
            "request",
            faults,
        )
        .await;
    }
}
