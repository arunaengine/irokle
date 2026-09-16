// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;
use crate::tests::support::{forked_side, node};
use crate::{MemoryStorage, Op, TopicId};

fn send_ops(
    session: &mut SyncSession,
    net: &SharedNet<MemoryStorage>,
    topic_id: TopicId,
    ops: Vec<Op>,
) {
    session
        .handle(
            net,
            SyncMessage::Data(crate::sync::SyncData { topic_id, ops }),
        )
        .unwrap();
    session.hold(&net.budget).unwrap();
}

#[tokio::test]
async fn replies_follow_branch() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&[211; 32]))
        .bind()
        .await
        .unwrap();
    let receiver = crate::Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .without_peer_whitelist()
        .build()
        .unwrap();
    let net = super::super::IrohNet::new(endpoint, receiver).unwrap();
    let author = node(210);
    let topic_id = TopicId::hash(b"session-branches");
    let (_, _, left, left_event) = forked_side(
        MemoryStorage::new(),
        topic_id,
        210,
        [net.node.peer_id()],
        "left",
    );
    let (_, _, right, right_event) = forked_side(
        MemoryStorage::new(),
        topic_id,
        210,
        [net.node.peer_id(), node(212).peer_id()],
        "right",
    );
    let (old, new) = if left.id > right.id {
        ([left, left_event], [right, right_event])
    } else {
        ([right, right_event], [left, left_event])
    };
    let remote = super::super::endpoint_addr(author.peer_id()).unwrap().id;
    let mut session = SyncSession::new(remote);
    session
        .handle(&net, SyncMessage::Open(author.sync_open(topic_id)))
        .unwrap();
    for op in &old {
        send_ops(&mut session, &net, topic_id, vec![op.clone()]);
    }
    let previous = &session.replies[&topic_id];
    assert_eq!(previous.genesis, Some(old[0].id));
    assert_eq!(previous.accepted, old.iter().map(|op| op.id).collect());
    previous.verify_signature().unwrap();

    let unrelated = TopicId::hash(b"session-unrelated");
    let (_, _, genesis, event) = forked_side(
        MemoryStorage::new(),
        unrelated,
        210,
        [net.node.peer_id()],
        "unrelated",
    );
    session
        .handle(&net, SyncMessage::Open(author.sync_open(unrelated)))
        .unwrap();
    send_ops(
        &mut session,
        &net,
        unrelated,
        vec![genesis.clone(), event.clone()],
    );
    session
        .handle(&net, SyncMessage::Open(author.sync_open(topic_id)))
        .unwrap();
    send_ops(&mut session, &net, topic_id, new.to_vec());
    assert_eq!(
        net.node
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        new[0].id
    );
    let (responses, _) = session.finish(&net, 0).unwrap();
    let acks = responses
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Ack(ack) => Some((ack.topic_id, ack)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(acks.len(), 2);
    let ack = acks[&topic_id];
    ack.verify_signature().unwrap();
    assert_eq!(ack.genesis, Some(new[0].id));
    assert_eq!(ack.accepted, new.iter().map(|op| op.id).collect());
    assert!(old.iter().all(|op| !ack.accepted.contains(&op.id)));
    let other = acks[&unrelated];
    other.verify_signature().unwrap();
    assert_eq!(other.accepted, [genesis.id, event.id].into());
    net.shutdown().await;
}
#[tokio::test]
async fn positions_keep_share() {
    use crate::oplog::Oplog;
    use crate::sync::{ActorRangeHint, ActorWindow, SyncData, SyncPage, SyncRequest};
    use crate::tests::support::Note;
    use crate::{
        ActorId, Ed25519Signer, EventEnvelope, OpBody, Signer, TopicGenesis, TopicPayload,
        actor_id_for,
    };

    struct Case {
        request: SyncRequest,
        ready: Op,
    }

    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&[234; 32]))
        .bind()
        .await
        .unwrap();
    let author = node(234);
    let signer = Ed25519Signer::from_bytes(&[234; 32]);
    let writers = [235, 236].map(|seed| Ed25519Signer::from_bytes(&[seed; 32]));
    let reader = node(237);
    let source = Oplog::with_storage(author.storage().clone());
    let destination = Oplog::with_storage(reader.storage().clone());
    let quiet = TopicId::from_bytes([1; 32]);
    let unknown = TopicId::from_bytes([2; 32]);
    let mut cases = Vec::new();
    for topic_id in [quiet, unknown] {
        let actor = actor_id_for(topic_id, signer.peer_id());
        let genesis = source
            .create_topic_genesis(
                topic_id,
                actor,
                TopicGenesis::new(
                    "test.note",
                    [
                        signer.peer_id(),
                        reader.peer_id(),
                        writers[0].peer_id(),
                        writers[1].peer_id(),
                    ],
                ),
                &signer,
            )
            .unwrap();
        destination.receive_ops(vec![genesis.clone()]).unwrap();
        let root = |writer: &Ed25519Signer| {
            Op::sign(
                OpBody {
                    topic_id,
                    author: writer.peer_id(),
                    actor_id: actor_id_for(topic_id, writer.peer_id()),
                    actor_seq: 1,
                    actor_prev: None,
                    deps: [genesis.id].into(),
                    generation: 1,
                    payload: TopicPayload::Event(
                        EventEnvelope::encode_event(&Note {
                            text: "root".into(),
                        })
                        .unwrap(),
                    ),
                },
                writer,
            )
            .unwrap()
        };
        let ready = root(&writers[0]);
        source.receive_ops(vec![ready.clone()]).unwrap();
        if topic_id == unknown {
            source.receive_ops(vec![root(&writers[1])]).unwrap();
            source
                .create_event_op(
                    topic_id,
                    actor,
                    EventEnvelope::encode_event(&Note {
                        text: "join".into(),
                    })
                    .unwrap(),
                    &signer,
                )
                .unwrap();
        }
        cases.push(Case {
            request: SyncRequest {
                topic_id,
                known: [genesis.id].into(),
                wants: BTreeSet::new(),
                actor_range_hints: vec![
                    ActorRangeHint {
                        actor_id: actor,
                        from_exclusive: 1,
                        to_inclusive: if topic_id == unknown { 2 } else { 1 },
                    },
                    ActorRangeHint {
                        actor_id: ready.signed.body.actor_id,
                        from_exclusive: 0,
                        to_inclusive: 1,
                    },
                ],
                genesis: Some(genesis.id),
                credit: Default::default(),
                window: ActorWindow {
                    after: Some(ActorId::from_bytes([255; 32])),
                    through: None,
                    behind: None,
                },
            },
            ready,
        });
    }
    let net = super::super::IrohNet::new(endpoint, author).unwrap();
    let remote = super::super::endpoint_addr(reader.peer_id()).unwrap().id;
    let mut session = SyncSession::new(remote);
    for case in &cases {
        session
            .handle(
                &net,
                SyncMessage::Open(reader.sync_open(case.request.topic_id)),
            )
            .unwrap();
        session
            .handle(&net, SyncMessage::Request(case.request.clone()))
            .unwrap();
    }
    session.hold(&net.budget).unwrap();
    let control_bytes = session
        .controls
        .iter()
        .map(|message| crate::net::framed_message_len(message).unwrap())
        .sum::<usize>();
    let empty_page = SyncMessage::Page(SyncPage {
        topic_id: quiet,
        more: false,
        missing: BTreeSet::new(),
        positions: BTreeSet::new(),
        continued: false,
    });
    let page_bytes = crate::net::framed_message_len(&empty_page).unwrap();
    let data_bytes = |case: &Case| {
        crate::net::framed_message_len(&SyncMessage::Data(SyncData {
            topic_id: case.request.topic_id,
            ops: vec![case.ready.clone()],
        }))
        .unwrap()
    };
    assert_eq!(data_bytes(&cases[0]), data_bytes(&cases[1]));
    let limits = StreamLimits {
        bytes: control_bytes + 2 * page_bytes + 2 * data_bytes(&cases[0]),
        ..StreamLimits::default()
    };
    let grant = net
        .budget
        .try_take(
            Pool::Data,
            net.budget.output_bound(session.pages_bound(limits)),
            OwnedClass::Output,
        )
        .unwrap();
    let (responses, held) = session
        .finish_slice(&net, grant.bytes(), limits, false)
        .unwrap();
    assert!(held <= grant.bytes());
    assert!(
        responses
            .iter()
            .map(|message| crate::net::framed_message_len(message).unwrap())
            .sum::<usize>()
            <= limits.bytes
    );
    let needed = actor_id_for(unknown, writers[1].peer_id());
    let page = responses
        .iter()
        .find_map(|message| match message {
            SyncMessage::Page(page) if page.topic_id == unknown => Some(page),
            _ => None,
        })
        .unwrap();
    assert_eq!(page.positions, [needed].into());
    assert!(page.more);
    assert!(responses.iter().any(|message| matches!(message, SyncMessage::Data(data) if data.topic_id == quiet && data.ops == vec![cases[0].ready.clone()])));
    assert!(responses.iter().any(
        |message| matches!(message, SyncMessage::Page(page) if page.topic_id == quiet && !page.more)
    ));
    for message in responses {
        if let SyncMessage::Data(data) = message {
            destination.receive_ops(data.ops).unwrap();
        }
    }
    drop(grant);
    drop(session);

    let mut request = cases[1].request.clone();
    request.actor_range_hints.push(ActorRangeHint {
        actor_id: needed,
        from_exclusive: 0,
        to_inclusive: 1,
    });
    let mut complete = false;
    for _ in 0..16 {
        let clock = reader.storage().actor_clock(&unknown).unwrap();
        for hint in &mut request.actor_range_hints {
            hint.from_exclusive = clock.get(&hint.actor_id);
        }
        let mut session = SyncSession::new(remote);
        session
            .handle(&net, SyncMessage::Open(reader.sync_open(unknown)))
            .unwrap();
        session
            .handle(&net, SyncMessage::Request(request.clone()))
            .unwrap();
        session.hold(&net.budget).unwrap();
        let limits = StreamLimits::default();
        let grant = net
            .budget
            .try_take(
                Pool::Data,
                net.budget.output_bound(session.pages_bound(limits)),
                OwnedClass::Output,
            )
            .unwrap();
        let (responses, held) = session
            .finish_slice(&net, grant.bytes(), limits, false)
            .unwrap();
        assert!(held <= grant.bytes());
        let mut more = None;
        for message in responses {
            match message {
                SyncMessage::Data(data) => {
                    destination.receive_ops(data.ops).unwrap();
                }
                SyncMessage::Page(page) => {
                    assert!(page.positions.iter().all(|actor| {
                        request
                            .actor_range_hints
                            .iter()
                            .any(|hint| hint.actor_id == *actor)
                    }));
                    more = Some(page.more);
                }
                SyncMessage::Failure(failure) => panic!("unexpected topic failure: {failure:?}"),
                _ => {}
            }
        }
        if more == Some(false) {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert_eq!(
        reader.storage().list_op_ids(&unknown).unwrap(),
        net.node.storage().list_op_ids(&unknown).unwrap()
    );

    let mut session = SyncSession::new(remote);
    session
        .handle(&net, SyncMessage::Open(reader.sync_open(quiet)))
        .unwrap();
    session
        .handle(&net, SyncMessage::Open(reader.sync_open(unknown)))
        .unwrap();
    let mut request = cases[1].request.clone();
    request.actor_range_hints[1].from_exclusive = 1;
    session.handle(&net, SyncMessage::Request(request)).unwrap();
    session.hold(&net.budget).unwrap();
    let controls = session
        .controls
        .iter()
        .map(|message| crate::net::framed_message_len(message).unwrap())
        .sum::<usize>();
    let required = SyncMessage::Page(SyncPage {
        topic_id: unknown,
        more: true,
        missing: BTreeSet::new(),
        positions: [needed].into(),
        continued: false,
    });
    let extra = crate::net::framed_message_len(&required).unwrap() - page_bytes;
    let limits = StreamLimits {
        bytes: controls + page_bytes + extra - 1,
        ..StreamLimits::default()
    };
    let grant = net
        .budget
        .try_take(
            Pool::Data,
            net.budget.output_bound(session.pages_bound(limits)),
            OwnedClass::Output,
        )
        .unwrap();
    let (responses, held) = session
        .finish_slice(&net, grant.bytes(), limits, false)
        .unwrap();
    assert!(held <= grant.bytes());
    assert!(responses.iter().any(|message| matches!(message, SyncMessage::Failure(failure) if failure.topic_id == unknown && failure.code == crate::sync::SyncFailureCode::Request)));
    assert!(
        !responses
            .iter()
            .any(|message| matches!(message, SyncMessage::Page(page) if page.topic_id == unknown))
    );
    assert!(responses.iter().any(
        |message| matches!(message, SyncMessage::Summary(summary) if summary.topic_id == quiet)
    ));
    drop(responses);
    drop(grant);
    let missing = (0_u16..257)
        .map(|value| {
            let mut bytes = [0; 32];
            bytes[..2].copy_from_slice(&value.to_le_bytes());
            crate::OpId::from_bytes(bytes)
        })
        .collect::<BTreeSet<_>>();
    let mut request = cases[0].request.clone();
    request.actor_range_hints[1].from_exclusive = 1;
    request.wants = missing.clone();
    let mut reported = BTreeSet::new();
    for last in [false, true] {
        let mut session = SyncSession::new(remote);
        session
            .handle(&net, SyncMessage::Open(reader.sync_open(quiet)))
            .unwrap();
        session
            .handle(&net, SyncMessage::Request(request.clone()))
            .unwrap();
        session.hold(&net.budget).unwrap();
        let limits = StreamLimits::default();
        let grant = net
            .budget
            .try_take(
                Pool::Data,
                net.budget.output_bound(session.pages_bound(limits)),
                OwnedClass::Output,
            )
            .unwrap();
        let (responses, held) = session
            .finish_slice(&net, grant.bytes(), limits, true)
            .unwrap();
        assert!(held <= grant.bytes());
        let page = responses
            .iter()
            .find_map(|message| match message {
                SyncMessage::Page(page) => Some(page),
                _ => None,
            })
            .expect("required missing IDs must be published before resuming");
        assert_eq!(page.more, !last);
        assert_eq!(page.missing.len(), if last { 1 } else { 256 });
        assert!(page.missing.is_disjoint(&reported));
        reported.extend(page.missing.iter().copied());
    }
    assert_eq!(reported, missing);
    net.shutdown().await;
}
