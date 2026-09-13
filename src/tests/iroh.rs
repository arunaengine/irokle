use super::support::*;

#[cfg(feature = "iroh")]
#[tokio::test]
async fn builder_sets_net() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let expected_peer = PeerId::from_bytes(*endpoint.id().as_bytes());
    let irokle = Irokle::builder()
        .with_net(endpoint)
        .without_auto_accept()
        .build()
        .unwrap();

    assert_eq!(irokle.peer_id(), expected_peer);
    assert!(irokle.endpoint().is_some());
    assert!(irokle.list_topics().unwrap().is_empty());
}

#[cfg(feature = "iroh")]
#[tokio::test]
async fn builder_sets_runtime_config() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: std::time::Duration::from_secs(7),
        sync_io_timeout: std::time::Duration::from_secs(8),
        resync_interval: std::time::Duration::from_secs(9),
        ..net::IrohRuntimeConfig::default()
    };
    let irokle = Irokle::builder()
        .with_net(endpoint)
        .with_iroh_runtime_config(runtime)
        .without_auto_accept()
        .build()
        .unwrap();

    assert_eq!(irokle.iroh_runtime_config(), Some(runtime));
}

#[cfg(feature = "iroh")]
#[test]
fn runtime_defaults_use_dirty_sync_and_daily_sweep() {
    let runtime = net::IrohRuntimeConfig::default();

    assert_eq!(runtime.resync_interval, std::time::Duration::from_secs(5));
    assert_eq!(
        runtime.resync_initial_backoff,
        std::time::Duration::from_secs(1)
    );
    assert_eq!(
        runtime.resync_max_backoff,
        std::time::Duration::from_secs(10 * 60)
    );
    assert_eq!(
        runtime.full_sweep_interval,
        std::time::Duration::from_secs(24 * 60 * 60)
    );
    assert_eq!(
        runtime.full_sweep_time_of_day,
        std::time::Duration::from_secs(3 * 60 * 60)
    );
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resync_runs_without_auto_accept_and_without_obligations() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_peer = PeerId::from_bytes(*bob_endpoint.id().as_bytes());
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: std::time::Duration::from_millis(10),
        sync_io_timeout: std::time::Duration::from_millis(10),
        resync_interval: std::time::Duration::from_millis(10),
        resync_initial_backoff: std::time::Duration::from_millis(10),
        resync_max_backoff: std::time::Duration::from_millis(20),
        ..net::IrohRuntimeConfig::default()
    };
    let alice = Irokle::builder()
        .with_net(alice_endpoint)
        .with_write_concern(WriteConcern::Local)
        .with_iroh_runtime_config(runtime)
        .without_auto_accept()
        .build()
        .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let status = alice.sync_status(topic.id()).unwrap();
            if status
                .iter()
                .any(|status| status.peer_id == bob_peer && status.failed_attempts > 0)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    alice.shutdown_iroh().await;
    bob_endpoint.close().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn iroh_defaults_to_async_replication() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_peer = PeerId::from_bytes(*bob_endpoint.id().as_bytes());
    let alice = Irokle::builder()
        .with_net(alice_endpoint)
        .without_auto_accept()
        .build()
        .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_peer].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();

    let report = alice.sync_report(bob_peer, topic.id()).unwrap();
    assert!(obligation_covers(
        alice.storage(),
        &report.obligations,
        &genesis.id
    ));

    alice.shutdown_iroh().await;
    bob_endpoint.close().await;
}

#[cfg(feature = "iroh")]
#[tokio::test]
async fn resync_and_accept_loops_start_once() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: std::time::Duration::from_millis(20),
        sync_io_timeout: std::time::Duration::from_millis(20),
        resync_interval: std::time::Duration::from_secs(60),
        ..net::IrohRuntimeConfig::default()
    };
    let node = Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let net = Arc::new(net::IrohNet::new_with_config(endpoint, node, runtime).unwrap());

    let accept = net.spawn_accept_loop().unwrap();
    let duplicate_accept = net.spawn_accept_loop().unwrap();
    let resync = net.spawn_resync_loop(runtime.resync_interval).unwrap();
    let duplicate_resync = net.spawn_resync_loop(runtime.resync_interval).unwrap();

    assert!(accept.is_some());
    assert!(duplicate_accept.is_none());
    assert!(resync.is_some());
    assert!(duplicate_resync.is_none());
    assert_eq!(net.runtime_config(), runtime);

    net.shutdown().await;
}

#[cfg(feature = "iroh")]
#[tokio::test]
async fn abort_allows_replacement() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: std::time::Duration::from_millis(20),
        sync_io_timeout: std::time::Duration::from_millis(20),
        resync_interval: std::time::Duration::from_secs(60),
        ..net::IrohRuntimeConfig::default()
    };
    let node = Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let net = Arc::new(net::IrohNet::new_with_config(endpoint, node, runtime).unwrap());

    let first = net
        .spawn_resync_loop(runtime.resync_interval)
        .unwrap()
        .expect("the first resync loop starts");
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    // The start-once latch clears on actual task exit, so the aborted loop can
    // be replaced instead of leaving the node without one.
    let replacement = net.spawn_resync_loop(runtime.resync_interval).unwrap();

    assert!(replacement.is_some());
    net.shutdown().await;
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[tokio::test]
async fn builder_selects_fjall() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let irokle = Irokle::builder()
        .with_net(endpoint)
        .with_fjall_path(dir.path())
        .unwrap()
        .without_auto_accept()
        .build()
        .unwrap();

    assert!(irokle.endpoint().is_some());
    assert!(irokle.list_topics().unwrap().is_empty());
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_now_records_ack() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder().with_net(alice_endpoint).build().unwrap();
    let bob = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_net(bob_endpoint)
        .build()
        .unwrap();
    let bob_addr = ready_addr(bob.endpoint().unwrap()).await;

    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let record = topic
        .publish(Note {
            text: "iroh".into(),
        })
        .unwrap();
    alice
        .put_sync_obligation(bob.peer_id(), topic.id(), [record.meta.op_id].into())
        .unwrap();

    alice.sync_addr_now(bob_addr, topic.id()).await.unwrap();

    assert_eq!(
        bob.open_topic::<Note>(topic.id())
            .unwrap()
            .history(history::HistoryOrder::OldestFirst)
            .unwrap()
            .len(),
        1
    );
    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &topic.id())
            .unwrap()
            .is_some()
    );
    assert!(
        alice
            .storage()
            .peer_ack(&alice.peer_id(), &topic.id())
            .unwrap()
            .is_none()
    );
    assert!(
        alice
            .sync_report(bob.peer_id(), topic.id())
            .unwrap()
            .obligations
            .is_empty()
    );
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_replication_records_scheduled_status() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_net(alice_endpoint)
        .with_write_concern(WriteConcern::Local)
        .without_auto_accept()
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();

    topic
        .publish_with(
            Note {
                text: "scheduled".into(),
            },
            crate::PublishOptions {
                write_concern: WriteConcern::AsyncReplication,
            },
        )
        .unwrap();

    let status = alice.sync_status(topic.id()).unwrap();
    assert_eq!(status.len(), 1);
    assert_eq!(status[0].peer_id, bob.peer_id());
    assert!(matches!(
        status[0].state,
        crate::SyncPeerState::Behind | crate::SyncPeerState::Failed
    ));
    assert_eq!(status[0].pending_obligations, 1);
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_replication_schedules_genesis_and_control_obligations() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_net(alice_endpoint)
        .with_write_concern(WriteConcern::AsyncReplication)
        .without_auto_accept()
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();

    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            replication_policy: ReplicationPolicy::all().with_max_sync_peers(1),
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();

    let report = alice.sync_report(bob.peer_id(), topic.id()).unwrap();
    assert!(
        obligation_covers(alice.storage(), &report.obligations, &genesis.id),
        "genesis op should be scheduled for async replication"
    );

    topic
        .set_replication_policy(ReplicationPolicy::all().with_max_sync_peers(1))
        .unwrap();
    let control = oplog::topological(alice.storage(), &topic.id())
        .unwrap()
        .into_iter()
        .find(|op| matches!(op.signed.body.payload, TopicPayload::Control(_)))
        .expect("control op");

    let report = alice.sync_report(bob.peer_id(), topic.id()).unwrap();
    assert!(
        obligation_covers(alice.storage(), &report.obligations, &control.id),
        "control op should be scheduled for async replication"
    );
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_replication_persists_genesis_obligation_with_fjall() {
    let dir = tempfile::tempdir().unwrap();
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_peer = PeerId::from_bytes(*bob_endpoint.id().as_bytes());

    let (topic_id, genesis_id) = {
        let alice = Irokle::builder()
            .with_net(alice_endpoint)
            .with_write_concern(WriteConcern::AsyncReplication)
            .with_fjall_path(dir.path())
            .unwrap()
            .without_auto_accept()
            .build()
            .unwrap();
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob_peer].into(),
                replication_policy: ReplicationPolicy::all().with_max_sync_peers(1),
            })
            .unwrap();
        let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap()[0].clone();
        alice.shutdown_iroh().await;
        bob_endpoint.close().await;
        (topic.id(), genesis.id)
    };

    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    let obligations = storage.sync_obligations(&bob_peer, &topic_id).unwrap();
    assert!(
        obligation_covers(&storage, &obligations, &genesis_id),
        "genesis obligation should be durably committed with the op"
    );
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_hides_non_member_summary() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let outsider_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .build()
        .unwrap();
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    let net = net::IrohNet::new(alice_endpoint, alice.clone()).unwrap();
    let outsider_peer = PeerId::from_bytes(*outsider_endpoint.id().as_bytes());

    let responses = net
        .handle_messages(
            outsider_endpoint.id(),
            vec![sync::SyncMessage::Open(
                sync::SyncEngine::<MemoryStorage>::open(topic.id(), outsider_peer, None),
            )],
        )
        .unwrap();

    assert!(responses.is_empty());
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn former_member_can_confirm_matching_fingerprint() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .build()
        .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    bob.receive_sync_data_from(
        alice.peer_id(),
        sync::SyncData {
            topic_id: topic.id(),
            ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
        },
    )
    .unwrap();
    bob.open_topic::<Note>(topic.id()).unwrap().leave().unwrap();
    alice
        .receive_sync_data_from(
            bob.peer_id(),
            sync::SyncData {
                topic_id: topic.id(),
                ops: oplog::topological(bob.storage(), &topic.id()).unwrap(),
            },
        )
        .unwrap();
    let net = net::IrohNet::new(alice_endpoint, alice.clone()).unwrap();

    let responses = net
        .handle_messages(
            bob_endpoint.id(),
            vec![
                sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                    topic.id(),
                    bob.peer_id(),
                    Some(Note::TYPE_ID.into()),
                )),
                sync::SyncMessage::Fingerprint(bob.sync_fingerprint(topic.id()).unwrap()),
            ],
        )
        .unwrap();

    assert!(responses.iter().any(|response| {
        matches!(response, sync::SyncMessage::Fingerprint(fingerprint) if fingerprint.topic_id == topic.id())
    }));
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whitelist_controls_bootstrap() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .build()
        .unwrap();
    let net = net::IrohNet::new(bob_endpoint, bob.clone()).unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let data = sync::SyncData {
        topic_id: topic.id(),
        ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
    };

    // A non-whitelisted Data message is skipped (no ack, nothing admitted)
    // without aborting the stream.
    let responses = net
        .handle_messages(
            alice_endpoint.id(),
            vec![
                sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                    topic.id(),
                    alice.peer_id(),
                    None,
                )),
                sync::SyncMessage::Data(data.clone()),
            ],
        )
        .unwrap();

    assert!(
        !responses
            .iter()
            .any(|response| matches!(response, sync::SyncMessage::Ack(_)))
    );
    assert!(bob.storage().topic_state(&topic.id()).unwrap().is_none());

    bob.add_peer_to_whitelist(alice.peer_id()).unwrap();
    let charlie = node(106);
    let excluded_topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [charlie.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let excluded_data = sync::SyncData {
        topic_id: excluded_topic.id(),
        ops: oplog::topological(alice.storage(), &excluded_topic.id()).unwrap(),
    };
    let responses = net
        .handle_messages(
            alice_endpoint.id(),
            vec![
                sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                    excluded_topic.id(),
                    alice.peer_id(),
                    None,
                )),
                sync::SyncMessage::Data(excluded_data),
            ],
        )
        .unwrap();

    assert!(
        !responses
            .iter()
            .any(|response| matches!(response, sync::SyncMessage::Ack(_)))
    );
    assert!(
        bob.storage()
            .topic_state(&excluded_topic.id())
            .unwrap()
            .is_none()
    );

    let responses = net
        .handle_messages(
            alice_endpoint.id(),
            vec![
                sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                    topic.id(),
                    alice.peer_id(),
                    None,
                )),
                sync::SyncMessage::Data(data),
            ],
        )
        .unwrap();

    assert!(
        responses
            .iter()
            .any(|response| matches!(response, sync::SyncMessage::Ack(_)))
    );
    assert!(bob.storage().topic_state(&topic.id()).unwrap().is_some());
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handle_messages_accepts_ack_heads_that_arrive_before_data() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let net = net::IrohNet::new(alice_endpoint, alice.clone()).unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let bootstrap = sync::SyncData {
        topic_id: topic.id(),
        ops: oplog::topological(alice.storage(), &topic.id()).unwrap(),
    };
    bob.receive_sync_data_from(alice.peer_id(), bootstrap)
        .unwrap();

    let alice_record = topic
        .publish(Note {
            text: "alice".into(),
        })
        .unwrap();
    let bob_topic = bob.open_topic::<Note>(topic.id()).unwrap();
    let bob_record = bob_topic.publish(Note { text: "bob".into() }).unwrap();
    let mut ack = sync::SyncAck {
        topic_id: topic.id(),
        peer_id: bob.peer_id(),
        genesis: genesis_of(bob.storage(), &topic.id()),
        accepted: [alice_record.meta.op_id].into(),
        heads: bob.storage().heads(&topic.id()).unwrap(),
        clock: bob.storage().actor_clock(&topic.id()).unwrap(),
        signature: None,
    };
    ack.sign(bob.signer()).unwrap();
    let data = sync::SyncData {
        topic_id: topic.id(),
        ops: vec![
            bob.storage()
                .get_op(&bob_record.meta.op_id)
                .unwrap()
                .unwrap(),
        ],
    };

    net.handle_messages(
        bob_endpoint.id(),
        vec![
            sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                topic.id(),
                bob.peer_id(),
                Some(Note::TYPE_ID.into()),
            )),
            sync::SyncMessage::Ack(ack),
            sync::SyncMessage::Data(data),
        ],
    )
    .unwrap();

    assert!(
        alice
            .storage()
            .get_meta(&bob_record.meta.op_id)
            .unwrap()
            .is_some()
    );
    let peer_ack = alice
        .storage()
        .peer_ack(&bob.peer_id(), &topic.id())
        .unwrap()
        .unwrap();
    assert!(peer_ack.heads.contains(&bob_record.meta.op_id));
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_resync_drains_topic_backlog_with_few_streams() {
    const TOPICS: usize = 1000;
    let lookup = iroh::address_lookup::memory::MemoryLookup::new();
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .address_lookup(lookup.clone())
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .address_lookup(lookup.clone())
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();

    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .with_write_concern(WriteConcern::Local)
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_net(bob_endpoint)
        .build()
        .unwrap();
    let bob_peer = bob.peer_id();
    lookup.add_endpoint_info(ready_addr(bob.endpoint().unwrap()).await);

    for index in 0..TOPICS {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob_peer].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        let record = topic
            .publish(Note {
                text: format!("doc-{index}"),
            })
            .unwrap();
        alice
            .put_sync_obligation(bob_peer, topic.id(), [record.meta.op_id].into())
            .unwrap();
    }

    let net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());
    let started = std::time::Instant::now();
    net.start_configured_resync_loop().unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        loop {
            if bob.list_topics().unwrap().len() == TOPICS
                && alice.storage().all_sync_obligations().unwrap().is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("bob did not receive the full topic backlog in time");
    let elapsed = started.elapsed();
    let streams = net.outbound_sync_streams();
    println!(
        "drained {TOPICS} single-op topics in {elapsed:?} using {streams} outbound sync streams"
    );
    assert!(
        streams <= 40,
        "per-topic round-trip amplification: {TOPICS} topics used {streams} outbound sync streams"
    );

    net.shutdown().await;
    bob.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genesis_tiebreak_eviction_reaches_sink_via_builder_net() {
    use crate::TopicEviction;

    let topic_id = TopicId::hash(b"iroh-genesis-fork-topic");
    let seed_x = [11u8; 32];
    let seed_y = [22u8; 32];
    // Both sides list each other as members so each can admit the other's chain.
    let members: BTreeSet<PeerId> = [
        Ed25519Signer::from_bytes(&seed_x).peer_id(),
        Ed25519Signer::from_bytes(&seed_y).peer_id(),
    ]
    .into();

    // Two independently created genesis chains for the same deterministic topic.
    let build_chain = |seed: &[u8; 32]| {
        let signer = Ed25519Signer::from_bytes(seed);
        let chain = oplog::Oplog::with_storage(MemoryStorage::new());
        let actor = actor_id_for(topic_id, signer.peer_id());
        let genesis = chain
            .create_topic_genesis(
                topic_id,
                actor,
                TopicGenesis {
                    event_type_id: Note::TYPE_ID.into(),
                    initial_peers: members.clone(),
                    replication_policy: ReplicationPolicy::default(),
                },
                &signer,
            )
            .unwrap();
        let event = chain
            .create_event_op(
                topic_id,
                actor,
                EventEnvelope::encode_event(&Note {
                    text: "forked".into(),
                })
                .unwrap(),
                &signer,
            )
            .unwrap();
        (signer, genesis, event)
    };

    let (signer_x, genesis_x, event_x) = build_chain(&seed_x);
    let (signer_y, genesis_y, event_y) = build_chain(&seed_y);

    // alice hosts the larger genesis (the loser that gets reset); the incoming
    // smaller genesis wins the tie-break.
    let (
        alice_seed,
        peer_seed,
        loser_signer,
        loser_genesis,
        loser_event,
        winner_genesis,
        winner_event,
    ) = if genesis_x.id > genesis_y.id {
        (
            seed_x, seed_y, signer_x, genesis_x, event_x, genesis_y, event_y,
        )
    } else {
        (
            seed_y, seed_x, signer_y, genesis_y, event_y, genesis_x, event_x,
        )
    };

    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&alice_seed))
        .bind()
        .await
        .unwrap();
    let peer_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&peer_seed))
        .bind()
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TopicEviction>();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .with_net(alice_endpoint)
        .with_eviction_sink(tx)
        .without_auto_accept()
        .build()
        .unwrap();
    // Seed alice with the losing chain so the incoming winning genesis collides.
    alice
        .receive_sync_data_from(
            loser_signer.peer_id(),
            sync::SyncData {
                topic_id,
                ops: vec![loser_genesis.clone(), loser_event.clone()],
            },
        )
        .unwrap();

    let peer_peer_id = PeerId::from_bytes(*peer_endpoint.id().as_bytes());
    let peer = Irokle::builder()
        .with_iroh_secret_key(peer_endpoint.secret_key())
        .build()
        .unwrap();
    let peer_net = net::IrohNet::new(peer_endpoint, peer).unwrap();
    alice.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice.endpoint().unwrap()).await;
    let messages = vec![
        sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
            topic_id,
            peer_peer_id,
            Some(Note::TYPE_ID.into()),
        )),
        sync::SyncMessage::Data(sync::SyncData {
            topic_id,
            ops: vec![winner_genesis.clone(), winner_event.clone()],
        }),
    ];
    let responses = peer_net.sync_with(alice_addr, &messages).await.unwrap();

    assert!(responses.iter().any(|response| {
        matches!(response, sync::SyncMessage::Ack(ack) if ack.topic_id == topic_id)
    }));

    let eviction = rx.try_recv().expect("eviction delivered to sink");
    assert_eq!(eviction.topic_id, topic_id);
    assert_eq!(eviction.losing_genesis, loser_genesis.id);
    assert_eq!(eviction.winning_genesis, winner_genesis.id);
    assert_eq!(eviction.evicted.len(), 1);
    assert_eq!(eviction.evicted[0].op_id, loser_event.id);
    assert_eq!(eviction.evicted[0].author, loser_signer.peer_id());
    assert!(rx.try_recv().is_err());

    assert_eq!(
        alice
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        winner_genesis.id
    );

    peer_net.shutdown().await;
    alice.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
pub(super) async fn ready_addr(endpoint: &iroh::Endpoint) -> iroh::EndpointAddr {
    use futures::StreamExt;
    use iroh::Watcher;

    let addr = endpoint.addr();
    if !addr.addrs.is_empty() {
        return addr;
    }
    let mut stream = endpoint.watch_addr().stream();
    tokio::time::timeout(std::time::Duration::from_secs(5), async move {
        loop {
            let addr = stream.next().await.expect("iroh endpoint address stream");
            if !addr.addrs.is_empty() {
                return addr;
            }
        }
    })
    .await
    .expect("iroh endpoint produced a dialable address")
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_ack_spares_other_topics() {
    // A rejected ack must not discard the other acks batched into the same
    // stream, or their obligations never clear and the peer resends forever.
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let net = net::IrohNet::new(alice_endpoint, alice.clone()).unwrap();

    let mut topics = Vec::new();
    for text in ["poisoned", "healthy"] {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        topic.publish(Note { text: text.into() }).unwrap();
        topics.push(topic.id());
    }
    let (poisoned, healthy) = (topics[0], topics[1]);

    // A clock claiming more of alice's own actor than alice has is the shape a
    // stale ack takes after a topic reset rewinds the local chain.
    let mut ahead = alice.storage().actor_clock(&poisoned).unwrap();
    ahead.observe(actor_id_for(poisoned, alice.peer_id()), 99);
    let mut bad = sync::SyncAck {
        topic_id: poisoned,
        peer_id: bob.peer_id(),
        genesis: genesis_of(bob.storage(), &poisoned),
        accepted: BTreeSet::new(),
        heads: BTreeSet::new(),
        clock: ahead,
        signature: None,
    };
    bad.sign(bob.signer()).unwrap();
    let mut good = sync::SyncAck {
        topic_id: healthy,
        peer_id: bob.peer_id(),
        genesis: genesis_of(alice.storage(), &healthy),
        accepted: BTreeSet::new(),
        heads: alice.storage().heads(&healthy).unwrap(),
        clock: alice.storage().actor_clock(&healthy).unwrap(),
        signature: None,
    };
    good.sign(bob.signer()).unwrap();

    let responses = net
        .handle_messages(
            bob_endpoint.id(),
            vec![
                sync::SyncMessage::Open(bob.sync_open(poisoned)),
                sync::SyncMessage::Ack(bad),
                sync::SyncMessage::Open(bob.sync_open(healthy)),
                sync::SyncMessage::Ack(good),
            ],
        )
        .expect("one rejected ack must not fail the stream");

    assert!(
        responses
            .iter()
            .all(|m| !matches!(m, sync::SyncMessage::Ack(_)))
    );
    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &healthy)
            .unwrap()
            .is_some(),
        "the valid ack must still be applied"
    );
    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &poisoned)
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn equal_fingerprint_repairs() {
    // Two stores with identical heads and clocks, one missing a non-head
    // record: neither side may take the matched-fingerprint path, and the
    // damaged side must pull the record back over a real exchange.
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_signer = Ed25519Signer::from_iroh_secret_key(bob_endpoint.secret_key());
    let alice = Irokle::with_storage(
        MemoryStorage::new(),
        NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(alice_endpoint.secret_key()),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob_signer.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    topic.publish(Note { text: "one".into() }).unwrap();
    topic.publish(Note { text: "two".into() }).unwrap();
    let topic_id = topic.id();
    let ops = oplog::topological(alice.storage(), &topic_id).unwrap();

    let storage = MemoryStorage::new();
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops(ops.clone())
        .unwrap();
    damage_op(&storage, &ops[1].id, Damage::Both);
    let bob = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: bob_signer,
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let alice_net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());
    alice_net.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice_net.endpoint()).await;
    let bob_net = net::IrohNet::new(bob_endpoint, bob.clone()).unwrap();

    assert_eq!(
        storage.topic_fingerprint(&topic_id).unwrap(),
        alice.storage().topic_fingerprint(&topic_id).unwrap()
    );
    let bob_fingerprint = bob.sync_fingerprint(topic_id).unwrap();
    assert_ne!(
        bob_fingerprint.fingerprint,
        alice.sync_fingerprint(topic_id).unwrap().fingerprint
    );
    // The damaged responder must answer its own digest with a summary.
    let responses = bob_net
        .handle_messages(
            alice_net.endpoint().id(),
            vec![
                sync::SyncMessage::Open(alice.sync_open(topic_id)),
                sync::SyncMessage::Fingerprint(bob_fingerprint),
            ],
        )
        .unwrap();
    assert!(matches!(
        responses.last(),
        Some(sync::SyncMessage::Summary(_))
    ));

    bob_net.sync_now(alice_addr, topic_id).await.unwrap();

    assert!(bob.topic_unresolved(topic_id).unwrap().is_empty());
    assert_eq!(storage.get_op(&ops[1].id).unwrap().as_ref(), Some(&ops[1]));
    assert_eq!(
        oplog::topological(&storage, &topic_id).unwrap().len(),
        ops.len()
    );
    alice.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_topic_retries() {
    // A topic whose data the peer could not admit must come back as an explicit
    // failure, never as silence the requester reads as success, and must not
    // take the topics batched alongside it down with it.
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_signer = Ed25519Signer::from_iroh_secret_key(bob_endpoint.secret_key());
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(alice_endpoint.secret_key()),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let bob = Irokle::with_storage(
        MemoryStorage::new(),
        NodeConfig {
            signer: bob_signer.clone(),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();

    let mut topic_ids = Vec::new();
    for text in ["broken", "healthy"] {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob_signer.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        topic.publish(Note { text: text.into() }).unwrap();
        let ops = oplog::topological(alice.storage(), &topic.id()).unwrap();
        bob.receive_sync_data_from(
            alice.peer_id(),
            sync::SyncData {
                topic_id: topic.id(),
                ops,
            },
        )
        .unwrap();
        bob.open_topic::<Note>(topic.id())
            .unwrap()
            .publish(Note {
                text: "reply".into(),
            })
            .unwrap();
        topic_ids.push(topic.id());
    }
    let (broken, healthy) = (topic_ids[0], topic_ids[1]);
    storage.fail_writes(broken);

    let alice_net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());
    alice_net.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice_net.endpoint()).await;
    let bob_net = net::IrohNet::new(bob_endpoint, bob.clone()).unwrap();

    // One stream carrying both topics: the broken one is reported, the healthy
    // one still gets its ack.
    let mut messages = Vec::new();
    for topic_id in [broken, healthy] {
        let plan = bob
            .negotiate_sync(alice.peer_id(), &alice.sync_summary(topic_id).unwrap())
            .unwrap();
        messages.push(sync::SyncMessage::Open(bob.sync_open(topic_id)));
        messages.push(sync::SyncMessage::Data(sync::SyncData {
            topic_id,
            ops: plan.send,
        }));
    }
    let responses = bob_net
        .sync_with(alice_addr.clone(), &messages)
        .await
        .unwrap();
    assert!(responses.iter().any(|message| matches!(
        message,
        sync::SyncMessage::Failure(failure) if failure.topic_id == broken
    )));
    assert!(responses.iter().any(|message| matches!(
        message,
        sync::SyncMessage::Ack(ack) if ack.topic_id == healthy
    )));

    assert!(bob_net.sync_now(alice_addr.clone(), broken).await.is_err());
    bob_net.sync_now(alice_addr, healthy).await.unwrap();
    assert_eq!(alice.storage().list_op_ids(&broken).unwrap().len(), 2);
    assert_eq!(alice.storage().list_op_ids(&healthy).unwrap().len(), 3);
    alice.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_peer_ack() {
    // An ack bound to another peer must fail its own topic only; the valid ack
    // batched behind it still has to clear its obligations.
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_iroh_secret_key(bob_endpoint.secret_key())
        .without_auto_accept()
        .build()
        .unwrap();
    let carol = node(77);
    let net = net::IrohNet::new(alice_endpoint, alice.clone()).unwrap();

    let mut topics = Vec::new();
    for text in ["unbound", "healthy"] {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [bob.peer_id(), carol.peer_id()].into(),
                ..TopicConfig::default()
            })
            .unwrap();
        topic.publish(Note { text: text.into() }).unwrap();
        topics.push(topic.id());
    }
    let (unbound, healthy) = (topics[0], topics[1]);

    // Signed by carol, so only the session's peer binding can reject it.
    let mut stray = sync::SyncAck {
        topic_id: unbound,
        peer_id: carol.peer_id(),
        genesis: genesis_of(alice.storage(), &unbound),
        accepted: BTreeSet::new(),
        heads: alice.storage().heads(&unbound).unwrap(),
        clock: alice.storage().actor_clock(&unbound).unwrap(),
        signature: None,
    };
    stray.sign(carol.signer()).unwrap();
    let mut good = sync::SyncAck {
        topic_id: healthy,
        peer_id: bob.peer_id(),
        genesis: genesis_of(alice.storage(), &healthy),
        accepted: BTreeSet::new(),
        heads: alice.storage().heads(&healthy).unwrap(),
        clock: alice.storage().actor_clock(&healthy).unwrap(),
        signature: None,
    };
    good.sign(bob.signer()).unwrap();

    let responses = net
        .handle_messages(
            bob_endpoint.id(),
            vec![
                sync::SyncMessage::Open(bob.sync_open(unbound)),
                sync::SyncMessage::Ack(stray),
                sync::SyncMessage::Open(bob.sync_open(healthy)),
                sync::SyncMessage::Ack(good),
            ],
        )
        .expect("an unbound ack must not fail the stream");

    assert!(responses.iter().any(|message| matches!(
        message,
        sync::SyncMessage::Failure(failure) if failure.topic_id == unbound
    )));
    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &healthy)
            .unwrap()
            .is_some(),
        "the validly bound ack must still be applied"
    );
    assert!(
        alice
            .storage()
            .peer_ack(&carol.peer_id(), &unbound)
            .unwrap()
            .is_none()
    );
}

/// Shutdown owns the stream tasks it spawned: while one is held inside a
/// storage read the timed variant reports it running, and once released the
/// plain shutdown completes and stays complete.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_awaits_tasks() {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let storage = StaleReadStorage::new(MemoryStorage::new());
    let alice = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(alice_endpoint.secret_key()),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let bob = Irokle::with_storage(
        MemoryStorage::new(),
        NodeConfig {
            signer: Ed25519Signer::from_iroh_secret_key(bob_endpoint.secret_key()),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let topic_id = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap()
        .id();
    let alice_net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());
    alice_net.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice_net.endpoint()).await;
    let bob_net = Arc::new(net::IrohNet::new(bob_endpoint, bob.clone()).unwrap());

    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read(GatePoint::View(topic_id), Arc::clone(&gate));
    let open = vec![sync::SyncMessage::Open(bob.sync_open(topic_id))];
    let requester = Arc::clone(&bob_net);
    let request = tokio::spawn(async move { requester.sync_with(alice_addr, &open).await });
    let arrival = Arc::clone(&gate);
    tokio::task::spawn_blocking(move || arrival.wait_arrival())
        .await
        .unwrap();

    // The held task cannot end, so the timed shutdown must report it.
    let outcome = alice_net
        .shutdown_with_timeout(std::time::Duration::from_millis(200))
        .await;
    assert!(
        matches!(outcome, net::ShutdownOutcome::Incomplete { running } if running >= 1),
        "{outcome:?}"
    );

    drop(release);
    tokio::time::timeout(std::time::Duration::from_secs(60), alice_net.shutdown())
        .await
        .expect("shutdown completes once the held task ends");
    assert_eq!(
        alice_net
            .shutdown_with_timeout(std::time::Duration::from_secs(60))
            .await,
        net::ShutdownOutcome::Complete
    );
    request.abort();
    let _ = request.await;
    bob_net.shutdown().await;
}

/// With sweeps off and no new publish, a failing preferred peer must hand its
/// topic to an allowed alternate: the health change itself schedules the work.
/// The preferred peer's own obligation stays outstanding for when it returns.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fallback_gets_work() {
    let lookup = iroh::address_lookup::memory::MemoryLookup::new();
    let keys = [iroh::SecretKey::generate(), iroh::SecretKey::generate()];
    let peers = keys
        .each_ref()
        .map(|key| crate::Ed25519Signer::from_iroh_secret_key(key).peer_id());
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .address_lookup(lookup.clone())
        .bind()
        .await
        .unwrap();
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: std::time::Duration::from_secs(2),
        sync_io_timeout: std::time::Duration::from_secs(10),
        resync_interval: std::time::Duration::from_millis(50),
        resync_initial_backoff: std::time::Duration::from_millis(10),
        resync_max_backoff: std::time::Duration::from_millis(50),
        full_sweep_interval: std::time::Duration::ZERO,
        ..net::IrohRuntimeConfig::default()
    };
    let alice = Irokle::builder()
        .with_iroh_runtime_config(runtime)
        .with_net(alice_endpoint)
        .build()
        .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: peers.into(),
            replication_policy: ReplicationPolicy::all().with_max_sync_peers(1),
        })
        .unwrap();
    let state = alice.storage().topic_state(&topic.id()).unwrap().unwrap();
    let preferred = node::select_sync_peers(topic.id(), alice.peer_id(), &state)[0];
    let alternate_index = usize::from(peers[0] == preferred);
    let alternate_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(keys[alternate_index].clone())
        .address_lookup(lookup.clone())
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let alternate = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_net(alternate_endpoint)
        .build()
        .unwrap();
    lookup.add_endpoint_info(ready_addr(alternate.endpoint().unwrap()).await);

    // The preferred peer never runs. Only this publish creates work.
    topic
        .publish(Note {
            text: "fall back".into(),
        })
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while alternate.list_topics().unwrap().is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the alternate was never contacted: {:?} failures {}",
            alice.sync_status(topic.id()).unwrap(),
            alice.peer_health().failures(&preferred)
        )
    });
    assert!(
        alice
            .storage()
            .has_sync_obligations(&preferred, &topic.id())
            .unwrap(),
        "the failing peer's own work must stay outstanding"
    );

    alice.shutdown_iroh().await;
    alternate.shutdown_iroh().await;
}
/// Offline async publishes of N and then 2N events keep one clock record per
/// peer, not one per publish.
#[cfg(feature = "iroh")]
async fn assert_publishes_coalesce<S: Storage>(storage: S) {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .bind()
        .await
        .unwrap();
    let peers = [182, 183].map(|seed| Ed25519Signer::from_bytes(&[seed; 32]).peer_id());
    let alice = Irokle::builder()
        .with_storage(storage.clone())
        .with_net(endpoint)
        .with_write_concern(WriteConcern::AsyncReplication)
        .without_auto_accept()
        .build()
        .unwrap();
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: peers.into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let actor = actor_id_for(topic.id(), alice.peer_id());
    let rounds = 8;

    for round in 1..=2 {
        for index in 0..rounds {
            topic
                .publish(Note {
                    text: format!("{round}-{index}"),
                })
                .unwrap();
        }
        assert_eq!(
            storage.topic_obligation_counts(&topic.id()).unwrap(),
            peers.map(|peer| (peer, 1)).into(),
            "round {round}"
        );
        for peer in peers {
            let records = storage.sync_obligations(&peer, &topic.id()).unwrap();
            assert!(matches!(
                &records[..],
                [crate::storage::SyncObligation {
                    target: crate::storage::ObligationTarget::Clock(clock),
                    ..
                }] if clock.get(&actor) == 1 + round * rounds
            ));
        }
    }
    alice.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_publishes_coalesce() {
    assert_publishes_coalesce(MemoryStorage::new()).await;
}

#[cfg(all(feature = "iroh", feature = "fjall"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fjall_publishes_coalesce() {
    let dir = tempfile::tempdir().unwrap();
    assert_publishes_coalesce(crate::storage::FjallStorage::open(dir.path()).unwrap()).await;
}
/// An inviter net without background loops, and a receiver built with its net
/// that trusts `whitelist`.
async fn bootstrap_pair(
    whitelist: bool,
) -> (
    Irokle,
    Arc<net::IrohNet<MemoryStorage>>,
    Irokle,
    iroh::EndpointAddr,
) {
    let alice_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let bob_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
        .build()
        .unwrap();
    let alice_net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());
    let trusted = if whitelist {
        vec![alice.peer_id()]
    } else {
        Vec::new()
    };
    let bob = Irokle::builder()
        .with_peer_whitelist(trusted)
        .with_net(bob_endpoint)
        .build()
        .unwrap();
    let bob_addr = ready_addr(bob.endpoint().unwrap()).await;
    (alice, alice_net, bob, bob_addr)
}

/// A topic of `alice` with `events` notes and then the invitation of `bob`.
fn invite_last(alice: &Irokle, bob: PeerId, events: usize) -> (TopicId, Vec<Op>) {
    let topic = alice.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..events {
        topic
            .publish(Note {
                text: index.to_string(),
            })
            .unwrap();
    }
    topic.add_peer(bob).unwrap();
    (
        topic.id(),
        oplog::topological(alice.storage(), &topic.id()).unwrap(),
    )
}

/// The receiver holds the whole history and the inviter holds its certified ack.
fn assert_bootstrapped(alice: &Irokle, bob: &Irokle, topic_id: TopicId) {
    assert_eq!(
        bob.storage().list_op_ids(&topic_id).unwrap(),
        alice.storage().list_op_ids(&topic_id).unwrap()
    );
    let ack = alice
        .storage()
        .peer_ack(&bob.peer_id(), &topic_id)
        .unwrap()
        .expect("certified ack");
    assert_eq!(ack.genesis, genesis_of(alice.storage(), &topic_id));
    assert_eq!(ack.clock, alice.storage().actor_clock(&topic_id).unwrap());
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_spans_frames() {
    let (alice, alice_net, bob, bob_addr) = bootstrap_pair(true).await;
    let (topic_id, ops) = invite_last(&alice, bob.peer_id(), 300);
    assert!(ops.len() > crate::net::MAX_SYNC_DATA_OPS_PER_MESSAGE);

    // One data frame without the invitation is staged and only receipted.
    let first = vec![
        sync::SyncMessage::Open(alice.sync_open(topic_id)),
        sync::SyncMessage::Data(sync::SyncData {
            topic_id,
            ops: ops[..crate::net::MAX_SYNC_DATA_OPS_PER_MESSAGE].to_vec(),
        }),
    ];
    let responses = alice_net.sync_with(bob_addr.clone(), &first).await.unwrap();
    let receipt = responses.iter().find_map(|response| match response {
        sync::SyncMessage::Receipt(receipt) => Some(receipt.clock.clone()),
        _ => None,
    });
    let actor = actor_id_for(topic_id, alice.peer_id());
    assert_eq!(receipt.map(|clock| clock.get(&actor)), Some(256));
    assert!(
        !responses
            .iter()
            .any(|response| matches!(response, sync::SyncMessage::Ack(_)))
    );
    assert!(bob.storage().topic_state(&topic_id).unwrap().is_none());
    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &topic_id)
            .unwrap()
            .is_none()
    );

    alice_net.sync_now(bob_addr, topic_id).await.unwrap();
    assert_bootstrapped(&alice, &bob, topic_id);
    alice_net.shutdown().await;
    bob.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_spans_pages() {
    let (alice, alice_net, bob, bob_addr) = bootstrap_pair(true).await;
    let (topic_id, _) = invite_last(&alice, bob.peer_id(), 4200);

    // Each staged page is progress, so a manual sync either completes or asks
    // to be called again; it never reports the staged pages as a failure.
    let mut attempts = 0;
    loop {
        attempts += 1;
        assert!(attempts <= 8, "bootstrap did not finish");
        match alice_net.sync_now(bob_addr.clone(), topic_id).await {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("bootstrap sync failed: {error}"),
        }
    }
    assert_bootstrapped(&alice, &bob, topic_id);
    alice_net.shutdown().await;
    bob.shutdown_iroh().await;
}

/// A member invited after the history pulls the topic it does not hold, page
/// by page. A topic neither side holds leaves nothing to do.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invited_pulls_topic() {
    let (alice, alice_net, bob, _) = bootstrap_pair(true).await;
    alice_net.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice_net.endpoint()).await;
    let nowhere = TopicId::hash("pull-nowhere");
    bob.sync_addr_now(alice_addr.clone(), nowhere)
        .await
        .unwrap();
    assert!(bob.storage().topic_state(&nowhere).unwrap().is_none());

    let (topic_id, _) = invite_last(&alice, bob.peer_id(), 4200);
    let mut attempts = 0;
    loop {
        attempts += 1;
        assert!(attempts <= 8, "pull did not finish");
        match bob.sync_addr_now(alice_addr.clone(), topic_id).await {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("pull failed: {error}"),
        }
    }
    assert_bootstrapped(&alice, &bob, topic_id);
    alice_net.shutdown().await;
    bob.shutdown_iroh().await;
}

/// A source outside the whitelist is never asked for a topic this node lacks.
#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlisted_never_pulls() {
    let (alice, alice_net, bob, _) = bootstrap_pair(false).await;
    alice_net.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice_net.endpoint()).await;
    let (topic_id, _) = invite_last(&alice, bob.peer_id(), 3);

    let error = bob.sync_addr_now(alice_addr, topic_id).await.unwrap_err();
    assert!(error.to_string().contains("whitelist"), "{error}");
    assert!(bob.storage().topic_state(&topic_id).unwrap().is_none());
    assert!(
        bob.storage()
            .staged_bootstrap_ops(&alice.peer_id(), &topic_id)
            .unwrap()
            .is_empty()
    );
    alice_net.shutdown().await;
    bob.shutdown_iroh().await;
}

#[cfg(feature = "iroh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlisted_never_stages() {
    let (alice, alice_net, bob, bob_addr) = bootstrap_pair(false).await;
    let (topic_id, _) = invite_last(&alice, bob.peer_id(), 3);

    assert!(alice_net.sync_now(bob_addr, topic_id).await.is_err());
    assert!(bob.storage().topic_state(&topic_id).unwrap().is_none());
    assert!(
        bob.storage()
            .staged_bootstrap_ops(&alice.peer_id(), &topic_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        alice
            .storage()
            .peer_ack(&bob.peer_id(), &topic_id)
            .unwrap()
            .is_none()
    );
    alice_net.shutdown().await;
    bob.shutdown_iroh().await;
}
