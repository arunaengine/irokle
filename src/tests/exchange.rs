//! Whole exchanges over real endpoints: pages in either direction, both at
//! once, under concurrent writes, and a peer that cannot deliver what it claims.

use super::support::*;

use std::time::Duration;

struct Pair {
    alice: Irokle,
    bob: Irokle,
    bob_addr: iroh::EndpointAddr,
    topic_id: TopicId,
}

async fn endpoint() -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

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

/// Two local-writing nodes that both hold the topic's genesis.
async fn pair() -> Pair {
    let alice = Irokle::builder()
        .with_write_concern(WriteConcern::Local)
        .with_net(endpoint().await)
        .without_auto_accept()
        .build()
        .unwrap();
    let bob = Irokle::builder()
        .with_peer_whitelist([alice.peer_id()])
        .with_write_concern(WriteConcern::Local)
        .with_net(endpoint().await)
        .build()
        .unwrap();
    let bob_addr = ready_addr(bob.endpoint().unwrap()).await;
    let topic = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let genesis = oplog::topological(alice.storage(), &topic.id()).unwrap();
    bob.receive_sync_data_from(
        alice.peer_id(),
        sync::SyncData {
            topic_id: topic.id(),
            ops: genesis,
        },
    )
    .unwrap();
    Pair {
        alice,
        bob,
        bob_addr,
        topic_id: topic.id(),
    }
}

fn publish(node: &Irokle, topic_id: TopicId, count: usize) {
    let topic = node.open_topic::<Note>(topic_id).unwrap();
    for index in 0..count {
        topic
            .publish(Note {
                text: format!("{index}"),
            })
            .unwrap();
    }
}

fn frontier(node: &Irokle, topic_id: TopicId) -> BTreeSet<OpId> {
    node.storage().heads(&topic_id).unwrap()
}

/// Sync until `sync_addr_now` completes, treating an exhausted page budget as
/// progress. Each call either completes, advances or fails; none may hang.
async fn sync_until_done(pair: &Pair) {
    for _ in 0..16 {
        match pair
            .alice
            .sync_addr_now(pair.bob_addr.clone(), pair.topic_id)
            .await
        {
            Ok(()) => return,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("sync failed: {error}"),
        }
    }
    panic!("sync never completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pull_pages() {
    let pair = pair().await;
    publish(&pair.bob, pair.topic_id, 5000);
    sync_until_done(&pair).await;
    assert_eq!(
        frontier(&pair.alice, pair.topic_id),
        frontier(&pair.bob, pair.topic_id)
    );
    pair.alice.shutdown_iroh().await;
    pair.bob.shutdown_iroh().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_pages() {
    let pair = pair().await;
    publish(&pair.alice, pair.topic_id, 5000);
    sync_until_done(&pair).await;
    assert_eq!(
        frontier(&pair.alice, pair.topic_id),
        frontier(&pair.bob, pair.topic_id)
    );
    assert!(
        pair.alice
            .storage()
            .peer_reached_op(
                &pair.bob.peer_id(),
                &frontier(&pair.alice, pair.topic_id)
                    .into_iter()
                    .next()
                    .unwrap()
            )
            .unwrap(),
        "the pushed frontier is certified by the peer"
    );
    pair.alice.shutdown_iroh().await;
    pair.bob.shutdown_iroh().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_pages() {
    let pair = pair().await;
    publish(&pair.alice, pair.topic_id, 3000);
    publish(&pair.bob, pair.topic_id, 3000);
    sync_until_done(&pair).await;
    let alice_clock = pair.alice.storage().actor_clock(&pair.topic_id).unwrap();
    let bob_clock = pair.bob.storage().actor_clock(&pair.topic_id).unwrap();
    assert!(alice_clock.dominates(&bob_clock) && bob_clock.dominates(&alice_clock));
    pair.alice.shutdown_iroh().await;
    pair.bob.shutdown_iroh().await;
}

/// Writes that keep arriving while pages move are later work: every call
/// returns, and a final sync after the writes stop completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_during_pages() {
    let pair = pair().await;
    publish(&pair.bob, pair.topic_id, 4500);
    let writer = {
        let bob = pair.bob.clone();
        let topic_id = pair.topic_id;
        tokio::task::spawn_blocking(move || publish(&bob, topic_id, 2000))
    };
    for _ in 0..3 {
        let result = pair
            .alice
            .sync_addr_now(pair.bob_addr.clone(), pair.topic_id)
            .await;
        assert!(
            result.is_ok()
                || result
                    .as_ref()
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock),
            "a sync under concurrent writes failed: {result:?}"
        );
    }
    writer.await.unwrap();
    sync_until_done(&pair).await;
    assert_eq!(
        frontier(&pair.alice, pair.topic_id),
        frontier(&pair.bob, pair.topic_id)
    );
    pair.alice.shutdown_iroh().await;
    pair.bob.shutdown_iroh().await;
}

/// A peer whose clock claims ops it cannot serve yields a truthful failure,
/// not success and not a spin: each call returns an error promptly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unserved_claim_blocks() {
    let pair = pair().await;
    publish(&pair.bob, pair.topic_id, 20);
    let ops = oplog::topological(pair.bob.storage(), &pair.topic_id).unwrap();
    pair.bob.storage().drop_op_record(&ops[3].id);
    pair.bob.recheck_topics().unwrap();
    for _ in 0..3 {
        let result = pair
            .alice
            .sync_addr_now(pair.bob_addr.clone(), pair.topic_id)
            .await;
        assert!(result.is_err(), "an incomplete peer must not look synced");
    }
    assert!(
        pair.alice.storage().actor_clock(&pair.topic_id).unwrap()
            != pair.bob.storage().actor_clock(&pair.topic_id).unwrap()
    );
    pair.alice.shutdown_iroh().await;
    pair.bob.shutdown_iroh().await;
}
