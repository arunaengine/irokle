//! The same fixture driver runs in separately compiled Irokle revisions.

use crate::tests::support::*;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use sync::{ActorFilter, RequestKnowledge, SyncAck, SyncMessage};

fn path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).expect(name))
}

fn save(path: &Path, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn history(case: &str, suffix: &str) -> Vec<Op> {
    postcard::from_bytes(
        &std::fs::read(path("IROKLE_FIXTURES").join(format!("{case}-{suffix}.bin"))).unwrap(),
    )
    .unwrap()
}

#[test]
#[ignore = "writes shared signed fixtures for the two-process version campaign"]
fn write_fixtures() {
    let directory = path("IROKLE_FIXTURES");
    std::fs::create_dir(&directory).unwrap();
    let chain = crate::tests::progress::reverse_chain(MemoryStorage::new(), 40);
    let ops = oplog::topological(chain.log.storage(), &chain.topic_id).unwrap();
    for case in ["ordinary", "window", "reconnect", "collision", "fallback"] {
        for suffix in ["initial", "final"] {
            save(
                &directory.join(format!("{case}-{suffix}.bin")),
                &postcard::to_stdvec(&ops).unwrap(),
            );
        }
    }
    let owner = Irokle::new(NodeConfig {
        signer: Ed25519Signer::from_bytes(&[230; 32]),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap();
    let topic = owner.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..64 {
        topic
            .publish(Note {
                text: index.to_string(),
            })
            .unwrap();
    }
    topic.add_peer(chain.reader).unwrap();
    let ops = oplog::topological(owner.storage(), &topic.id()).unwrap();
    for suffix in ["initial", "final"] {
        save(
            &directory.join(format!("bootstrap-{suffix}.bin")),
            &postcard::to_stdvec(&ops).unwrap(),
        );
    }
    let branches = crate::tests::branch::branches(230);
    for (suffix, pair) in [("initial", branches.old), ("final", branches.new)] {
        save(
            &directory.join(format!("branch-{suffix}.bin")),
            &postcard::to_stdvec(&vec![pair.0, pair.1]).unwrap(),
        );
    }
}

fn verify(node: &Irokle, expected: &[Op]) {
    let topic = expected[0].signed.body.topic_id;
    let reference = oplog::Oplog::new();
    reference.receive_ops(expected.to_vec()).unwrap();
    assert_eq!(
        node.storage().list_op_ids(&topic).unwrap(),
        reference.storage().list_op_ids(&topic).unwrap()
    );
    assert_eq!(
        node.storage().actor_clock(&topic).unwrap(),
        reference.storage().actor_clock(&topic).unwrap()
    );
    assert_eq!(
        node.storage().heads(&topic).unwrap(),
        reference.storage().heads(&topic).unwrap()
    );
    for op in expected {
        assert_eq!(node.storage().get_op(&op.id).unwrap().as_ref(), Some(op));
        assert!(node.storage().dep_resolvable(&op.id).unwrap());
        op.signed.verify().unwrap();
    }
    assert!(node.topic_unresolved(topic).unwrap().is_empty());
}

async fn network(node: Irokle) -> Arc<net::IrohNet> {
    let key = if node.peer_id() == Ed25519Signer::from_bytes(&[230; 32]).peer_id() {
        230
    } else {
        231
    };
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&[key; 32]))
        .alpns(vec![net::IROKLE_SYNC_ALPN.to_vec()])
        .bind()
        .await
        .unwrap();
    Arc::new(net::IrohNet::new(endpoint, node).unwrap())
}

fn control(address: SocketAddr, command: u8) {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(30)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    stream.write_all(&[command]).unwrap();
    let mut reply = [0];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(reply, [command]);
}

async fn serve(node: Irokle, initial: &[Op], final_ops: &[Op]) {
    let topic = initial[0].signed.body.topic_id;
    let reader = Ed25519Signer::from_bytes(&[231; 32]).peer_id();
    node.put_sync_obligation(reader, topic, [initial.last().unwrap().id].into())
        .unwrap();
    let net = network(node.clone()).await;
    net.start_accept_loop().unwrap();
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let ready = postcard::to_stdvec(&(
        crate::tests::iroh::ready_addr(net.endpoint()).await,
        listener.local_addr().unwrap(),
    ))
    .unwrap();
    let ready_path = path("IROKLE_PEER_READY");
    save(&ready_path.with_extension("partial"), &ready);
    std::fs::rename(ready_path.with_extension("partial"), ready_path).unwrap();
    loop {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(120)))
            .unwrap();
        let mut command = [0];
        stream.read_exact(&mut command).unwrap();
        match command[0] {
            b'a' => {
                node.receive_sync_data_from(
                    node.peer_id(),
                    sync::SyncData {
                        topic_id: topic,
                        ops: final_ops.to_vec(),
                    },
                )
                .unwrap();
                node.put_sync_obligation(reader, topic, [final_ops.last().unwrap().id].into())
                    .unwrap();
                verify(&node, final_ops);
            }
            b'd' => {
                let bytes = std::fs::read(path("IROKLE_PEER_ACK")).unwrap();
                let SyncMessage::Ack(ack) = net::decode_sync_message(&bytes).unwrap() else {
                    panic!("persisted fixture is not an ACK");
                };
                ack.verify_signature().unwrap();
                assert_eq!(
                    net::encode_sync_message(&SyncMessage::Ack(ack.clone())).unwrap(),
                    bytes
                );
                assert_eq!(ack.genesis, Some(final_ops[0].id));
                assert_eq!(ack.clock, node.storage().actor_clock(&topic).unwrap());
                node.apply_sync_ack(&ack).unwrap();
                assert!(
                    !node
                        .storage()
                        .has_sync_obligations(&reader, &topic)
                        .unwrap()
                );
                verify(&node, final_ops);
                stream.write_all(&command).unwrap();
                break;
            }
            other => panic!("unknown control {other}"),
        }
        stream.write_all(&command).unwrap();
    }
    net.shutdown().await;
    net.endpoint().close().await;
}

async fn pull(net: &net::IrohNet, address: &iroh::EndpointAddr, topic: TopicId) {
    for attempt in 0..256 {
        match net.sync_now(address.clone(), topic).await {
            Ok(()) => return,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(attempt < 255, "finite goal did not finish: {error}");
            }
            Err(error) => panic!("pull failed: {error}"),
        }
    }
}

async fn manual_pull(
    node: &Irokle,
    net: &net::IrohNet,
    address: &iroh::EndpointAddr,
    expected: &[Op],
    case: &str,
) {
    let topic = expected[0].signed.body.topic_id;
    let peer = Ed25519Signer::from_bytes(&[230; 32]).peer_id();
    let responses = net
        .sync_with(address.clone(), &[SyncMessage::Open(node.sync_open(topic))])
        .await
        .unwrap();
    let remote = responses
        .messages()
        .iter()
        .find_map(|message| match message {
            SyncMessage::Summary(summary) => Some(summary.clone()),
            _ => None,
        })
        .unwrap();
    drop(responses);
    let engine = sync::SyncEngine::new(
        oplog::Oplog::with_storage(node.storage().clone()),
        node.peer_id(),
    )
    .with_request_items(3);
    let mut knowledge = RequestKnowledge::with_capacity(2);
    let mut positions = 0;
    let mut held_collisions = 0;
    for round in 0..512 {
        let local = node.storage().actor_clock(&topic).unwrap();
        if local == remote.actor_clock {
            assert!(
                round > 1 && positions > 0,
                "no multi-window dependency discovery"
            );
            assert!(
                held_collisions > 0,
                "filter never collided with a held actor"
            );
            println!(
                "IROKLE_TRANSFER rounds={round} positions={positions} held_collisions={held_collisions}"
            );
            return;
        }
        let mut request = engine.plan_request_with(peer, &remote, &knowledge).unwrap();
        if case != "reconnect" && round == 0 {
            let bytes = if case == "fallback" {
                sync::MAX_FILTER_BYTES
            } else {
                1
            };
            request.window.behind = Some(ActorFilter {
                bits: vec![255; bytes],
            });
            let encoded = net::encode_sync_message(&SyncMessage::Request(request.clone())).unwrap();
            assert!(encoded.len() > bytes);
            println!(
                "IROKLE_FILTER bytes={bytes} request_bytes={}",
                encoded.len()
            );
        } else if case == "fallback" && round == 1 {
            request.window.behind = None;
        }
        request.credit.ops = 3;
        let window = request.window.clone();
        let responses = net
            .sync_with(
                address.clone(),
                &[
                    SyncMessage::Open(node.sync_open(topic)),
                    SyncMessage::Request(request),
                ],
            )
            .await
            .unwrap();
        let mut received = false;
        for message in responses.messages() {
            match message {
                SyncMessage::Data(data) => {
                    for op in &data.ops {
                        for dependency in &op.signed.body.deps {
                            assert!(node.storage().dep_resolvable(dependency).unwrap());
                        }
                        node.receive_sync_data_from(
                            peer,
                            sync::SyncData {
                                topic_id: topic,
                                ops: vec![op.clone()],
                            },
                        )
                        .unwrap();
                    }
                    received |= !data.ops.is_empty();
                }
                SyncMessage::Page(page) => {
                    assert!(page.missing.is_empty());
                    positions += page.positions.len();
                    held_collisions += page
                        .positions
                        .iter()
                        .filter(|actor| local.get(actor) > 0)
                        .count();
                    knowledge.settle(
                        &window,
                        (&page.positions, page.continued),
                        (received, remote.actor_clock.len()),
                    );
                }
                SyncMessage::Failure(failure) => panic!("page failed: {failure:?}"),
                _ => {}
            }
        }
        if case == "reconnect" && received {
            assert!(node.storage().list_op_ids(&topic).unwrap().len() < expected.len());
            return;
        }
    }
    panic!("finite manual goal did not finish");
}

async fn receive(node: Irokle, initial: &[Op], final_ops: &[Op], case: &str) {
    let topic = initial[0].signed.body.topic_id;
    let (address, control_addr): (iroh::EndpointAddr, SocketAddr) =
        postcard::from_bytes(&std::fs::read(path("IROKLE_PEER_READY")).unwrap()).unwrap();
    let mut net = network(node.clone()).await;
    if matches!(case, "collision" | "fallback" | "reconnect") {
        manual_pull(&node, &net, &address, initial, case).await;
    }
    if case == "reconnect" {
        net.shutdown().await;
        net.endpoint().close().await;
        net = network(node.clone()).await;
    }
    pull(&net, &address, topic).await;
    verify(&node, initial);
    if case == "branch" {
        control(control_addr, b'a');
        pull(&net, &address, topic).await;
    }
    verify(&node, final_ops);
    let summary = node.sync_summary(topic).unwrap();
    let mut ack = SyncAck {
        topic_id: topic,
        peer_id: node.peer_id(),
        genesis: summary.genesis,
        accepted: BTreeSet::new(),
        heads: summary.heads,
        clock: summary.actor_clock,
        signature: None,
    };
    ack.sign(node.signer()).unwrap();
    let message = SyncMessage::Ack(ack);
    save(
        &path("IROKLE_PEER_ACK"),
        &net::encode_sync_message(&message).unwrap(),
    );
    net.sync_with(
        address,
        &[SyncMessage::Open(node.sync_open(topic)), message],
    )
    .await
    .unwrap();
    control(control_addr, b'd');
    net.shutdown().await;
    net.endpoint().close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the version campaign's fixture and peer control paths"]
async fn peer_process() {
    let case = std::env::var("IROKLE_PEER_CASE").unwrap();
    let server = std::env::var("IROKLE_PEER_ROLE").unwrap() == "server";
    let initial = history(&case, "initial");
    let final_ops = history(&case, "final");
    let node = Irokle::new(NodeConfig {
        signer: Ed25519Signer::from_bytes(&[if server { 230 } else { 231 }; 32]),
        default_write_concern: WriteConcern::Local,
        peer_whitelist: None,
    })
    .unwrap()
    .with_request_items(if case == "ordinary" { 65536 } else { 3 });
    let preload = if server {
        initial.clone()
    } else if case == "bootstrap" {
        Vec::new()
    } else if matches!(case.as_str(), "collision" | "fallback") {
        initial[..initial.len() - 5].to_vec()
    } else {
        initial[..1].to_vec()
    };
    oplog::Oplog::with_storage(node.storage().clone())
        .receive_ops(preload)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(300), async {
        if server {
            serve(node, &initial, &final_ops).await;
        } else {
            receive(node, &initial, &final_ops, &case).await;
        }
    })
    .await
    .expect("version peer lost progress");
}
