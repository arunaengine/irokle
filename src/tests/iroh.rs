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
    assert!(
        report
            .obligations
            .iter()
            .any(|obligation| obligation.op_ids.contains(&genesis.id))
    );

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
        report
            .obligations
            .iter()
            .any(|obligation| obligation.op_ids.contains(&genesis.id)),
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
        report
            .obligations
            .iter()
            .any(|obligation| obligation.op_ids.contains(&control.id)),
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
        obligations
            .iter()
            .any(|obligation| obligation.op_ids.contains(&genesis_id)),
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
async fn genesis_tiebreak_eviction_reaches_sink_via_handle_messages() {
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
    let alice = Irokle::builder()
        .with_iroh_secret_key(alice_endpoint.secret_key())
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

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TopicEviction>();
    let net = net::IrohNet::new_with_alpns_config_and_sink(
        alice_endpoint,
        alice.clone(),
        Vec::new(),
        net::IrohRuntimeConfig::default(),
        Some(tx),
    )
    .unwrap();

    let peer_peer_id = PeerId::from_bytes(*peer_endpoint.id().as_bytes());
    let responses = net
        .handle_messages(
            peer_endpoint.id(),
            vec![
                sync::SyncMessage::Open(sync::SyncEngine::<MemoryStorage>::open(
                    topic_id,
                    peer_peer_id,
                    Some(Note::TYPE_ID.into()),
                )),
                sync::SyncMessage::Data(sync::SyncData {
                    topic_id,
                    ops: vec![winner_genesis.clone(), winner_event.clone()],
                }),
            ],
        )
        .unwrap();

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

    net.shutdown().await;
}

#[cfg(feature = "iroh")]
async fn ready_addr(endpoint: &iroh::Endpoint) -> iroh::EndpointAddr {
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
