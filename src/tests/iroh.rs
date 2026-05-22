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

    let err = net
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
        .unwrap_err();

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
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
    let err = net
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
        .unwrap_err();

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
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
