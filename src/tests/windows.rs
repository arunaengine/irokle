//! Both peers push and pull more than one page at once over tiny QUIC receive
//! windows. Each side keeps serving acks and controls, and a spent credit
//! continues instead of ending in a mutual wait or a silent omission.

use crate::tests::support::*;

use std::time::Duration;

type Lookup = iroh::address_lookup::memory::MemoryLookup;

async fn bind(lookup: &Lookup, key: &iroh::SecretKey) -> iroh::Endpoint {
    let transport = iroh::endpoint::QuicTransportConfig::builder()
        .stream_receive_window(iroh::endpoint::VarInt::from_u32(16 * 1024))
        .receive_window(iroh::endpoint::VarInt::from_u32(64 * 1024))
        .build();
    iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(key.clone())
        .transport_config(transport)
        .address_lookup(lookup.clone())
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

fn publish(node: &Irokle, topic_id: TopicId, count: usize) {
    let topic = node.open_topic::<Note>(topic_id).unwrap();
    for index in 0..count {
        topic
            .publish(Note {
                text: format!("{index:0>256}"),
            })
            .unwrap();
    }
}

/// The ack `holder` stores from `peer` certifies `holder`'s whole clock.
fn certified(holder: &Irokle, peer: &Irokle, topic_id: TopicId) -> bool {
    let clock = holder.storage().actor_clock(&topic_id).unwrap();
    holder
        .storage()
        .peer_ack(&peer.peer_id(), &topic_id)
        .unwrap()
        .is_some_and(|ack| ack.genesis.is_some() && ack.clock.dominates(&clock))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplex_small_windows() {
    let io_timeout = Duration::from_secs(120);
    let runtime = net::IrohRuntimeConfig {
        sync_io_timeout: io_timeout,
        resync_interval: Duration::from_millis(50),
        resync_initial_backoff: Duration::from_millis(50),
        resync_max_backoff: Duration::from_millis(400),
        full_sweep_interval: Duration::ZERO,
        ..net::IrohRuntimeConfig::default()
    };
    let lookup = Lookup::new();
    let keys = [iroh::SecretKey::generate(), iroh::SecretKey::generate()];
    let signers = keys.each_ref().map(Ed25519Signer::from_iroh_secret_key);
    let storages = [MemoryStorage::new(), MemoryStorage::new()];
    let offline = [0, 1].map(|index| {
        Irokle::with_storage(
            storages[index].clone(),
            NodeConfig {
                signer: signers[index].clone(),
                ..NodeConfig::default()
            },
        )
        .unwrap()
    });
    let topic_id = offline[0]
        .create_topic::<Note>(TopicConfig {
            initial_peers: [signers[1].peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap()
        .id();
    offline[1]
        .receive_sync_data_from(
            signers[0].peer_id(),
            sync::SyncData {
                topic_id,
                ops: oplog::topological(&storages[0], &topic_id).unwrap(),
            },
        )
        .unwrap();
    // More than one credit of ops owed in each direction before either net runs.
    let count = sync::SyncCredit::default().ops as usize + 300;
    for node in &offline {
        publish(node, topic_id, count);
    }

    let mut nodes = Vec::new();
    for index in 0..2 {
        let node = Irokle::builder()
            .with_storage(storages[index].clone())
            .with_iroh_secret_key(&keys[index])
            .with_iroh_runtime_config(runtime)
            .with_peer_whitelist([signers[1 - index].peer_id()])
            .with_net(bind(&lookup, &keys[index]).await)
            .build()
            .unwrap();
        lookup.add_endpoint_info(crate::tests::iroh::ready_addr(node.endpoint().unwrap()).await);
        nodes.push(node);
    }
    let (alice, bob) = (nodes[0].clone(), nodes[1].clone());
    let writer = {
        let alice = alice.clone();
        tokio::task::spawn_blocking(move || publish(&alice, topic_id, 300))
    };
    let (to_bob, to_alice) = tokio::join!(
        alice.sync_now(bob.peer_id(), topic_id),
        bob.sync_now(alice.peer_id(), topic_id)
    );
    for result in [to_bob, to_alice] {
        // A crossing exchange may find its page already delivered by the
        // other direction; a mutual wait would surface as a timeout.
        assert!(
            result.is_ok()
                || result.as_ref().is_err_and(|error| matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::InvalidData
                )),
            "{result:?}"
        );
    }
    writer.await.unwrap();

    let converged = tokio::time::timeout(io_timeout / 2, async {
        loop {
            if certified(&alice, &bob, topic_id)
                && certified(&bob, &alice, topic_id)
                && alice
                    .storage()
                    .topic_obligation_counts(&topic_id)
                    .unwrap()
                    .is_empty()
                && bob
                    .storage()
                    .topic_obligation_counts(&topic_id)
                    .unwrap()
                    .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        converged.is_ok(),
        "no convergence before a stalled stream could time out: alice {:?} bob {:?}",
        alice.sync_status(topic_id).unwrap(),
        bob.sync_status(topic_id).unwrap()
    );
    assert_eq!(
        alice.storage().list_op_ids(&topic_id).unwrap(),
        bob.storage().list_op_ids(&topic_id).unwrap()
    );
    assert_eq!(
        alice.storage().list_op_ids(&topic_id).unwrap().len(),
        1 + 2 * count + 300
    );

    alice.shutdown_iroh().await;
    bob.shutdown_iroh().await;
}
