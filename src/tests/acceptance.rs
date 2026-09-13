//! Repairs combined through public APIs and real sessions: page races beside a
//! normal topic, and a behind peer catching up across deep work.

use std::time::Duration;

use super::iroh::ready_addr;
use super::support::*;

#[cfg(feature = "fjall")]
use crate::sync::{ActorRangeHint, SyncCredit, SyncData, SyncEngine, SyncMessage, SyncRequest};

async fn endpoint(lookup: &iroh::address_lookup::memory::MemoryLookup) -> iroh::Endpoint {
    iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .address_lookup(lookup.clone())
        .alpns(vec![crate::net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

fn config(signer: Ed25519Signer) -> NodeConfig {
    NodeConfig {
        signer,
        default_write_concern: WriteConcern::Local,
        peer_whitelist: None,
    }
}

/// A request for everything of `actor_id` on branch `genesis`.
#[cfg(feature = "fjall")]
fn request(topic_id: TopicId, actor_id: ActorId, genesis: OpId) -> SyncMessage {
    SyncMessage::Request(SyncRequest {
        topic_id,
        known: BTreeSet::new(),
        wants: BTreeSet::new(),
        actor_range_hints: vec![ActorRangeHint {
            actor_id,
            from_exclusive: 0,
            to_inclusive: u64::MAX,
        }],
        genesis: Some(genesis),
        credit: SyncCredit::default(),
    })
}

#[cfg(feature = "fjall")]
fn served_ids(replies: &[SyncMessage], topic_id: TopicId) -> BTreeSet<OpId> {
    replies
        .iter()
        .filter_map(|reply| match reply {
            SyncMessage::Data(data) if data.topic_id == topic_id => Some(&data.ops),
            _ => None,
        })
        .flatten()
        .map(|op| op.id)
        .collect()
}

/// A batch push races a membership removal on one topic while the other topic
/// of the batch completes. Then a served stream races a same-position genesis
/// replacement on one request while its other request is served in full.
#[cfg(feature = "fjall")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn races_beside_topic() {
    let lookup = iroh::address_lookup::memory::MemoryLookup::new();
    let dir = tempfile::tempdir().unwrap();
    let storage = StaleReadStorage::new(crate::storage::FjallStorage::open(dir.path()).unwrap());
    let alice_endpoint = endpoint(&lookup).await;
    let alice_signer = Ed25519Signer::from_iroh_secret_key(alice_endpoint.secret_key());
    let alice = Irokle::with_storage(storage.clone(), config(alice_signer)).unwrap();
    let bob_endpoint = endpoint(&lookup).await;
    let bob_signer = Ed25519Signer::from_iroh_secret_key(bob_endpoint.secret_key());
    let bob = Irokle::with_storage(MemoryStorage::new(), config(bob_signer)).unwrap();
    let bob_net = Arc::new(net::IrohNet::new(bob_endpoint, bob.clone()).unwrap());
    bob_net.start_accept_loop().unwrap();
    let bob_addr = ready_addr(bob_net.endpoint()).await;
    let alice_net = Arc::new(net::IrohNet::new(alice_endpoint, alice.clone()).unwrap());

    let reader = bob.peer_id();
    let shared = |peers: Vec<PeerId>| {
        let topic = alice
            .create_topic::<Note>(TopicConfig {
                initial_peers: peers.into_iter().collect(),
                ..TopicConfig::default()
            })
            .unwrap();
        let genesis = oplog::topological(&storage, &topic.id()).unwrap()[0].clone();
        let data = SyncData {
            topic_id: topic.id(),
            ops: vec![genesis],
        };
        bob.receive_sync_data_from(alice.peer_id(), data).unwrap();
        for text in ["one", "two"] {
            topic.publish(Note { text: text.into() }).unwrap();
        }
        topic
    };
    let raced = shared(vec![reader, node(96).peer_id()]);
    let steady = shared(vec![reader]);
    let (raced_id, steady_id) = (raced.id(), steady.id());
    let before = storage.list_op_ids(&raced_id).unwrap();

    // The digest's view and the open's state read come before the plan's view.
    let gate = Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read_after(GatePoint::Topic(raced_id), 2, Arc::clone(&gate));
    // Driven beside the race in this task: the batch future is too deep to spawn.
    let topics = [raced_id, steady_id];
    let syncing = alice_net.sync_topics_now(bob_addr, &topics);
    let racing = async {
        let arrival = Arc::clone(&gate);
        tokio::task::spawn_blocking(move || arrival.wait_arrival())
            .await
            .unwrap();
        assert!(gate.arrived(), "the push never planned the raced topic");
        tokio::task::spawn_blocking(move || {
            raced.remove_peer(reader).unwrap();
            raced
                .publish(Note {
                    text: "after".into(),
                })
                .unwrap();
        })
        .await
        .unwrap();
        drop(release);
    };
    let (results, ()) = tokio::join!(syncing, racing);
    assert!(results[&steady_id].is_ok(), "{results:?}");
    assert_eq!(
        bob.storage().list_op_ids(&steady_id).unwrap(),
        storage.list_op_ids(&steady_id).unwrap()
    );
    assert!(storage.list_op_ids(&raced_id).unwrap().len() > before.len());
    let received = bob.storage().list_op_ids(&raced_id).unwrap();
    assert!(
        received.is_subset(&before),
        "a removed member received later ops: {:?}",
        received.difference(&before).collect::<Vec<_>>()
    );

    let branches = super::branch::branches(97);
    let replaced = branches.topic_id;
    oplog::Oplog::with_storage(storage.clone())
        .receive_ops_from_peer(
            Some(branches.author.peer_id()),
            vec![branches.old.0.clone(), branches.old.1.clone()],
        )
        .unwrap();
    let old = BTreeSet::from([branches.old.0.id, branches.old.1.id]);
    let new_genesis = branches.new.0.id;
    let member = branches.member.peer_id();
    let normal = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [member].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    normal
        .publish(Note {
            text: "served".into(),
        })
        .unwrap();
    let open = |topic_id| {
        SyncMessage::Open(SyncEngine::<MemoryStorage>::open(
            topic_id,
            member,
            Some(Note::TYPE_ID.into()),
        ))
    };
    let messages = vec![
        open(replaced),
        request(
            replaced,
            branches.old.1.signed.body.actor_id,
            branches.old.0.id,
        ),
        open(normal.id()),
        request(
            normal.id(),
            actor_id_for(normal.id(), alice.peer_id()),
            genesis_of(&storage, &normal.id()).unwrap(),
        ),
    ];
    let member_id = iroh::EndpointId::from_bytes(member.as_bytes()).unwrap();
    let serving = Arc::clone(&alice_net);
    let writer = storage.clone();
    // The first view of the replaced topic is the page plan's own read.
    let replies = interleave(
        &storage,
        (GatePoint::View(replaced), 0),
        Isolation::Commits,
        move || {
            serving
                .handle_messages(member_id, messages)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>()
        },
        move || super::branch::reset_to_new(&writer, &branches),
    );
    assert_eq!(genesis_of(&storage, &replaced), Some(new_genesis));
    let refused = replies.iter().any(
        |reply| matches!(reply, SyncMessage::Failure(failure) if failure.topic_id == replaced),
    );
    assert!(
        refused || served_ids(&replies, replaced).is_subset(&old),
        "a request accepted on the old genesis served new-branch ops"
    );
    assert_eq!(
        served_ids(&replies, normal.id()),
        storage.list_op_ids(&normal.id()).unwrap(),
        "the other request of the stream was not served in full"
    );
    alice_net.shutdown().await;
    bob_net.shutdown().await;
}

/// Deep dependencies between two writers, a lost early record and a second
/// ready topic, served in small stream slices: the behind peer alone initiates,
/// repairs the hole and reaches both frontiers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn behind_peer_slices() {
    let lookup = iroh::address_lookup::memory::MemoryLookup::new();
    let alice_endpoint = endpoint(&lookup).await;
    let alice_signer = Ed25519Signer::from_iroh_secret_key(alice_endpoint.secret_key());
    let alice = Irokle::with_storage(MemoryStorage::new(), config(alice_signer)).unwrap();
    let bob_endpoint = endpoint(&lookup).await;
    let bob_signer = Ed25519Signer::from_iroh_secret_key(bob_endpoint.secret_key());
    let bob_storage = MemoryStorage::new();
    let bob = Irokle::with_storage(bob_storage.clone(), config(bob_signer)).unwrap();
    let writer = Irokle::with_storage(
        MemoryStorage::new(),
        config(Ed25519Signer::from_bytes(&[98; 32])),
    )
    .unwrap();

    let members = TopicConfig {
        initial_peers: [bob.peer_id(), writer.peer_id()].into(),
        ..TopicConfig::default()
    };
    let deep = alice.create_topic::<Note>(members).unwrap();
    let deep_id = deep.id();
    let copy = |from: &Irokle, to: &Irokle, id: OpId| {
        let op = from.storage().get_op(&id).unwrap().unwrap();
        oplog::Oplog::with_storage(to.storage().clone())
            .receive_ops(vec![op])
            .unwrap();
    };
    oplog::Oplog::with_storage(writer.storage().clone())
        .receive_ops(oplog::topological(alice.storage(), &deep_id).unwrap())
        .unwrap();
    let written = writer.open_topic::<Note>(deep_id).unwrap();
    // Each writer's op depends on the other's newest, so actors wait on each other.
    for index in 0..300 {
        let text = format!("alice {index}");
        let id = deep.publish(Note { text }).unwrap().meta.op_id;
        copy(&alice, &writer, id);
        let text = format!("writer {index}");
        let id = written.publish(Note { text }).unwrap().meta.op_id;
        copy(&writer, &alice, id);
    }
    let ready = alice
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    for index in 0..50 {
        ready
            .publish(Note {
                text: format!("ready {index}"),
            })
            .unwrap();
    }

    let history = oplog::topological(alice.storage(), &deep_id).unwrap();
    let log = oplog::Oplog::with_storage(bob_storage.clone());
    log.receive_ops(history[..history.len() * 2 / 5].to_vec())
        .unwrap();
    let lost = history[5].id;
    damage_op(&bob_storage, &lost, Damage::Both);
    let ready_genesis = oplog::topological(alice.storage(), &ready.id()).unwrap()[0].clone();
    log.receive_ops(vec![ready_genesis]).unwrap();
    assert!(bob.topic_unresolved(deep_id).unwrap().contains(&lost));

    let limits = net::StreamLimits {
        bytes: 16 * 1024,
        ..net::StreamLimits::default()
    };
    let alice_net = Arc::new(
        net::IrohNet::new(alice_endpoint, alice.clone())
            .unwrap()
            .with_stream_limits(limits),
    );
    alice_net.start_accept_loop().unwrap();
    let alice_addr = ready_addr(alice_net.endpoint()).await;
    let bob_net = net::IrohNet::new(bob_endpoint, bob.clone()).unwrap();
    let topics = [deep_id, ready.id()];
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds <= 32, "the behind peer stopped advancing");
        let results = tokio::time::timeout(
            Duration::from_secs(120),
            bob_net.sync_topics_now(alice_addr.clone(), &topics),
        )
        .await
        .expect("a sync round never finished");
        let mut done = true;
        for (topic_id, result) in results {
            match result {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => done = false,
                Err(error) => panic!("sync of {topic_id} failed: {error}"),
            }
        }
        if done {
            break;
        }
    }
    let streams = bob_net.outbound_sync_streams();
    assert!(streams >= 8, "{streams} streams carried the whole catch-up");
    for topic_id in topics {
        assert_eq!(
            bob_storage.list_op_ids(&topic_id).unwrap(),
            alice.storage().list_op_ids(&topic_id).unwrap()
        );
    }
    assert!(bob.topic_unresolved(deep_id).unwrap().is_empty());
    assert_eq!(
        bob_storage.get_op(&lost).unwrap().as_ref(),
        Some(&history[5])
    );
    assert_eq!(alice_net.outbound_sync_streams(), 0);
    alice_net.shutdown().await;
    bob_net.shutdown().await;
}
