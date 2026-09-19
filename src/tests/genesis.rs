//! Two stores holding different genesis operations of one topic converge on
//! the deterministic winner through ordinary sync, whichever side starts.

use crate::tests::iroh::ready_addr;
use crate::tests::support::*;

struct Side {
    node: Irokle,
    net: Arc<net::IrohNet<MemoryStorage>>,
    addr: iroh::EndpointAddr,
}

/// A node whose net only answers, so nothing but the tested call syncs.
async fn side() -> Side {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    let node = Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .build()
        .unwrap();
    let runtime = net::IrohRuntimeConfig {
        full_sweep_interval: std::time::Duration::ZERO,
        ..net::IrohRuntimeConfig::default()
    };
    let net = Arc::new(net::IrohNet::new_with_config(endpoint, node.clone(), runtime).unwrap());
    net.start_accept_loop().unwrap();
    let addr = ready_addr(net.endpoint()).await;
    Side { node, net, addr }
}

/// A genesis of `topic_id` by `author` for `members` plus `events` notes,
/// written straight into `side`'s store. `max_sync_peers` makes two geneses of
/// the same author and members differ.
fn branch(
    side: &Side,
    topic_id: TopicId,
    author: &Ed25519Signer,
    members: &BTreeSet<PeerId>,
    max_sync_peers: usize,
    events: usize,
) -> Vec<Op> {
    let log = oplog::Oplog::with_storage(side.node.storage().clone());
    let actor = actor_id_for(topic_id, author.peer_id());
    let genesis = TopicGenesis {
        event_type_id: Note::TYPE_ID.into(),
        initial_peers: members.clone(),
        replication_policy: ReplicationPolicy::all().with_max_sync_peers(max_sync_peers),
    };
    let mut ops = vec![
        log.create_topic_genesis(topic_id, actor, genesis, author)
            .unwrap(),
    ];
    for index in 0..events {
        let note = EventEnvelope::encode_event(&Note {
            text: format!("{max_sync_peers}-{index}"),
        })
        .unwrap();
        ops.push(log.create_event_op(topic_id, actor, note, author).unwrap());
    }
    ops
}

/// Syncs manually until the call reports completion.
async fn sync_until_done(from: &Side, to: &Side, topic_id: TopicId) {
    let mut attempts = 0;
    loop {
        attempts += 1;
        assert!(attempts <= 8, "branch conflict did not converge");
        match from.net.sync_now(to.addr.clone(), topic_id).await {
            Ok(()) => return,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("branch conflict sync failed: {error}"),
        }
    }
}

/// Both sides hold exactly the winning branch, and the losing side journaled
/// its replaced events for its application.
fn assert_converged(a: &Side, b: &Side, topic_id: TopicId, winner: &[Op], loser: &Side) {
    let ids = winner.iter().map(|op| op.id).collect::<BTreeSet<_>>();
    for side in [a, b] {
        assert_eq!(
            genesis_of(side.node.storage(), &topic_id),
            Some(winner[0].id)
        );
        assert_eq!(side.node.storage().list_op_ids(&topic_id).unwrap(), ids);
    }
    let evictions = loser.node.pending_evictions().unwrap();
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0].winning_genesis, winner[0].id);
}

/// Runs one conflict where `a` and `b` hold branches by one author or by two,
/// started by `a` (`Some(true)`), by `b` (`Some(false)`) or by both at once.
async fn converge(same_author: bool, start: Option<bool>, events: usize) {
    let (a, b) = (side().await, side().await);
    let topic_id = TopicId::hash(format!("branch-{same_author}-{start:?}-{events}").as_bytes());
    let first = Ed25519Signer::from_bytes(&[201; 32]);
    let second = Ed25519Signer::from_bytes(&[if same_author { 201 } else { 202 }; 32]);
    // Each branch lists both authors, so either genesis may win the reset.
    let members = [
        a.node.peer_id(),
        b.node.peer_id(),
        first.peer_id(),
        second.peer_id(),
    ]
    .into();
    let a_ops = branch(&a, topic_id, &first, &members, 3, events);
    let b_ops = branch(&b, topic_id, &second, &members, 4, events);
    assert_ne!(a_ops[0].id, b_ops[0].id);
    match start {
        Some(true) => sync_until_done(&a, &b, topic_id).await,
        Some(false) => sync_until_done(&b, &a, topic_id).await,
        None => {
            tokio::join!(
                sync_until_done(&a, &b, topic_id),
                sync_until_done(&b, &a, topic_id)
            );
        }
    }
    let (winner, loser) = if a_ops[0].id < b_ops[0].id {
        (&a_ops, &b)
    } else {
        (&b_ops, &a)
    };
    assert_converged(&a, &b, topic_id, winner, loser);
    a.net.shutdown().await;
    b.net.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_author_start() {
    converge(true, Some(true), 3).await;
    converge(true, Some(false), 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn other_author_start() {
    converge(false, Some(true), 3).await;
    converge(false, Some(false), 3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_starts_converge() {
    converge(true, None, 3).await;
    converge(false, None, 3).await;
}

/// A winning branch longer than one page replaces the loser across pages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_branch_converges() {
    converge(true, Some(true), 5000).await;
    converge(true, Some(false), 5000).await;
}

/// A genesis whose author is not a member of the other side's branch never
/// resets it: both keep their branch, nothing is evicted, and the sync reports
/// an error instead of completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_keeps_local() {
    let (a, b) = (side().await, side().await);
    let topic_id = TopicId::hash(b"branch-unauthorized");
    let a_author = Ed25519Signer::from_bytes(&[203; 32]);
    let b_author = Ed25519Signer::from_bytes(&[204; 32]);
    let sides = [a.node.peer_id(), b.node.peer_id()];
    let a_ops = branch(
        &a,
        topic_id,
        &a_author,
        &[sides[0], sides[1], a_author.peer_id()].into(),
        3,
        2,
    );
    let b_ops = branch(
        &b,
        topic_id,
        &b_author,
        &[sides[0], sides[1], b_author.peer_id()].into(),
        3,
        2,
    );
    for (from, to) in [(&a, &b), (&b, &a)] {
        for _ in 0..3 {
            assert!(from.net.sync_now(to.addr.clone(), topic_id).await.is_err());
        }
    }
    for (side, ops) in [(&a, &a_ops), (&b, &b_ops)] {
        assert_eq!(genesis_of(side.node.storage(), &topic_id), Some(ops[0].id));
        assert_eq!(
            side.node.storage().list_op_ids(&topic_id).unwrap(),
            ops.iter().map(|op| op.id).collect()
        );
        assert!(side.node.pending_evictions().unwrap().is_empty());
    }
    a.net.shutdown().await;
    b.net.shutdown().await;
}
