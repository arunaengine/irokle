//! A preferred peer that is down hands its topics to an allowed alternate
//! without a sweep or a new publish, for several fanouts and batch sizes, and
//! is served again once it returns.

use super::support::*;

use std::time::Duration;

type Lookup = iroh::address_lookup::memory::MemoryLookup;

async fn bind(lookup: &Lookup, key: &iroh::SecretKey) -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(key.clone())
        .address_lookup(lookup.clone())
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

/// A member that trusts `alice`, reachable through `lookup`.
async fn member(lookup: &Lookup, key: &iroh::SecretKey, alice: PeerId) -> Irokle {
    let node = Irokle::builder()
        .with_peer_whitelist([alice])
        .with_net(bind(lookup, key).await)
        .build()
        .unwrap();
    lookup.add_endpoint_info(super::iroh::ready_addr(node.endpoint().unwrap()).await);
    node
}

/// Waits until `done` holds, polling observable state under a generous cap.
async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(120), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

fn holds(node: &Irokle, alice: &Irokle, topic_id: TopicId) -> bool {
    node.storage().list_op_ids(&topic_id).unwrap()
        == alice.storage().list_op_ids(&topic_id).unwrap()
}

/// `topics` topics select the down peer among `fanout` peers. Each unselected
/// member receives its topics while the down peer's work stays owed, and the
/// down peer is probed and served once it returns.
async fn assert_fallback(fanout: usize, topics: usize) {
    let lookup = Lookup::new();
    let alice_key = iroh::SecretKey::generate();
    let alice_signer = Ed25519Signer::from_iroh_secret_key(&alice_key);
    let alice_peer = alice_signer.peer_id();
    let keys = (0..=fanout)
        .map(|_| iroh::SecretKey::generate())
        .collect::<Vec<_>>();
    let peers = keys
        .iter()
        .map(|key| Ed25519Signer::from_iroh_secret_key(key).peer_id())
        .collect::<Vec<_>>();
    let down = peers[0];

    // Topics are drawn on a scratch store and only the chosen ones installed.
    let storage = MemoryStorage::new();
    let config = || NodeConfig {
        signer: alice_signer.clone(),
        ..NodeConfig::default()
    };
    let offline = Irokle::with_storage(storage.clone(), config()).unwrap();
    let scratch = Irokle::with_storage(MemoryStorage::new(), config()).unwrap();
    let mut chosen = Vec::new();
    while chosen.len() < topics {
        let topic = scratch
            .create_topic::<Note>(TopicConfig {
                initial_peers: peers.iter().copied().collect(),
                replication_policy: ReplicationPolicy::all().with_max_sync_peers(fanout),
            })
            .unwrap();
        let state = scratch.storage().topic_state(&topic.id()).unwrap().unwrap();
        let selected = node::select_sync_peers(topic.id(), alice_peer, &state);
        if !selected.contains(&down) {
            continue;
        }
        let spare = peers
            .iter()
            .position(|peer| !selected.contains(peer))
            .unwrap();
        let ops = oplog::topological(scratch.storage(), &topic.id()).unwrap();
        oplog::Oplog::with_storage(storage.clone())
            .receive_ops(ops)
            .unwrap();
        let record = offline
            .open_topic::<Note>(topic.id())
            .unwrap()
            .publish(Note {
                text: "fall back".into(),
            })
            .unwrap();
        offline
            .put_sync_obligation(down, topic.id(), [record.meta.op_id].into())
            .unwrap();
        chosen.push((topic.id(), spare));
    }

    let up = {
        let mut up = Vec::new();
        for key in &keys[1..] {
            up.push(member(&lookup, key, alice_peer).await);
        }
        up
    };
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: Duration::from_secs(2),
        sync_io_timeout: Duration::from_secs(20),
        resync_interval: Duration::from_millis(50),
        resync_initial_backoff: Duration::from_millis(100),
        resync_max_backoff: Duration::from_millis(400),
        full_sweep_interval: Duration::ZERO,
        ..net::IrohRuntimeConfig::default()
    };
    let alice = Irokle::builder()
        .with_storage(storage.clone())
        .with_iroh_secret_key(&alice_key)
        .with_iroh_runtime_config(runtime)
        .with_net(bind(&lookup, &alice_key).await)
        .without_auto_accept()
        .build()
        .unwrap();

    wait_until("an unselected member never received its topics", || {
        chosen
            .iter()
            .all(|(topic_id, spare)| holds(&up[spare - 1], &alice, *topic_id))
    })
    .await;
    // Targets of the down peer found by discovery after its first failed
    // batch may be served by the alternate before their own attempt fails.
    wait_until("the down peer was not attempted for every topic", || {
        chosen.iter().all(|(topic_id, _)| {
            alice
                .sync_status(*topic_id)
                .unwrap()
                .into_iter()
                .any(|status| {
                    status.peer_id == down
                        && status.failed_attempts > 0
                        && status.last_error.is_some()
                })
        })
    })
    .await;
    for (topic_id, _) in &chosen {
        assert!(
            storage.has_sync_obligations(&down, topic_id).unwrap(),
            "the down peer's own work must stay owed"
        );
    }
    assert!(alice.peer_health().failures(&down) > 0);

    let returned = member(&lookup, &keys[0], alice_peer).await;
    wait_until("the returning peer was never served", || {
        chosen.iter().all(|(topic_id, _)| {
            holds(&returned, &alice, *topic_id)
                && !storage.has_sync_obligations(&down, topic_id).unwrap()
        })
    })
    .await;
    wait_until("the returning peer stayed failed", || {
        alice.peer_health().failures(&down) == 0
    })
    .await;

    alice.shutdown_iroh().await;
    returned.shutdown_iroh().await;
    for node in up {
        node.shutdown_iroh().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_one_small() {
    assert_fallback(1, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_one_large() {
    assert_fallback(1, 200).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_two_small() {
    assert_fallback(2, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_two_large() {
    assert_fallback(2, 200).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_three_small() {
    assert_fallback(3, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_three_large() {
    assert_fallback(3, 200).await;
}
