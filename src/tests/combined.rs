//! Faults combined in one run: the captured goal still finishes or reports a
//! real block, never a false success.

use super::support::*;

use std::time::Duration;

async fn ready_addr(endpoint: &iroh::Endpoint) -> iroh::EndpointAddr {
    use futures::StreamExt;
    use iroh::Watcher;
    let addr = endpoint.addr();
    if !addr.addrs.is_empty() {
        return addr;
    }
    let mut stream = endpoint.watch_addr().stream();
    tokio::time::timeout(Duration::from_secs(5), async move {
        loop {
            let addr = stream.next().await.expect("address stream");
            if !addr.addrs.is_empty() {
                return addr;
            }
        }
    })
    .await
    .expect("dialable address")
}

/// A member invited after a page of history, with a writer publishing, the preferred
/// replica down and a cancelled manual sync, is still brought to the writer's final
/// frontier by the resync loop through the alternate path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_invite_faults() {
    let lookup = iroh::address_lookup::memory::MemoryLookup::new();
    let runtime = net::IrohRuntimeConfig {
        connect_timeout: Duration::from_secs(2),
        sync_io_timeout: Duration::from_secs(20),
        resync_interval: Duration::from_millis(50),
        resync_initial_backoff: Duration::from_millis(10),
        resync_max_backoff: Duration::from_millis(100),
        full_sweep_interval: Duration::ZERO,
        ..net::IrohRuntimeConfig::default()
    };
    let alice = Irokle::builder()
        .with_iroh_runtime_config(runtime)
        .with_net(
            iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
                .address_lookup(lookup.clone())
                .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
                .bind()
                .await
                .unwrap(),
        )
        .build()
        .unwrap();
    let down = Ed25519Signer::generate().peer_id();
    let carol_key = iroh::SecretKey::generate();
    let carol_peer = Ed25519Signer::from_iroh_secret_key(&carol_key).peer_id();

    // Prefer the replica that never runs once the new member is invited.
    let mut chosen = None;
    for _ in 0..32 {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: [down].into(),
                replication_policy: ReplicationPolicy::all().with_max_sync_peers(1),
            })
            .unwrap();
        let mut state = alice.storage().topic_state(&topic.id()).unwrap().unwrap();
        state.members.insert(carol_peer);
        if node::select_sync_peers(topic.id(), alice.peer_id(), &state) == vec![down] {
            chosen = Some(topic);
            break;
        }
    }
    let topic = chosen.expect("a topic preferring the unreachable replica");
    let topic_id = topic.id();
    let mut before_invite = None;
    for index in 0..4200 {
        let record = topic
            .publish(Note {
                text: format!("before {index}"),
            })
            .unwrap();
        before_invite = Some(record.meta.op_id);
    }
    let before_invite = before_invite.unwrap();
    topic.add_peer(carol_peer).unwrap();

    let carol = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_net(
            iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
                .secret_key(carol_key)
                .address_lookup(lookup.clone())
                .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
                .bind()
                .await
                .unwrap(),
        )
        .build()
        .unwrap();
    let carol_addr = ready_addr(carol.endpoint().unwrap()).await;
    lookup.add_endpoint_info(carol_addr.clone());

    let writer = {
        let topic = alice.open_topic::<Note>(topic_id).unwrap();
        tokio::task::spawn_blocking(move || {
            for index in 0..300 {
                topic
                    .publish(Note {
                        text: format!("during {index}"),
                    })
                    .unwrap();
            }
        })
    };
    let cancelled = tokio::time::timeout(
        Duration::from_millis(30),
        alice.sync_addr_now(carol_addr, topic_id),
    )
    .await;
    assert!(cancelled.is_err() || cancelled.is_ok_and(|result| result.is_ok() || result.is_err()));
    writer.await.unwrap();
    let final_clock = alice.storage().actor_clock(&topic_id).unwrap();

    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let caught_up = carol
                .storage()
                .actor_clock(&topic_id)
                .unwrap()
                .dominates(&final_clock);
            if caught_up {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the new member never caught up; carol has topic: {}, statuses {:?}",
            carol.storage().topic_state(&topic_id).unwrap().is_some(),
            alice.sync_status(topic_id).unwrap()
        )
    });
    assert!(
        alice
            .storage()
            .has_sync_obligations(&down, &topic_id)
            .unwrap(),
        "work owed to the unreachable replica stays outstanding"
    );
    assert!(carol.topic_unresolved(topic_id).unwrap().is_empty());
    // Obligations keep their peer: the replica's still names what was written
    // while it was the target, while the member that caught up owes nothing
    // once its ack is applied. Writes after the replica ran out of retries go to
    // the member instead, so which later writes it is owed depends on timing.
    let last = oplog::topological(alice.storage(), &topic_id)
        .unwrap()
        .pop()
        .unwrap()
        .id;
    let covers = |peer: &PeerId, op_id: &OpId| {
        let obligations = alice.storage().sync_obligations(peer, &topic_id).unwrap();
        obligation_covers(alice.storage(), &obligations, op_id)
    };
    let owed = |peer: &PeerId| covers(peer, &last);
    assert!(covers(&down, &before_invite));
    tokio::time::timeout(Duration::from_secs(120), async {
        while owed(&carol_peer) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the caught-up member's ack never settled what it was owed");
    assert!(
        covers(&down, &before_invite),
        "another peer's ack settles nothing of the replica"
    );

    alice.shutdown_iroh().await;
    carol.shutdown_iroh().await;
}
