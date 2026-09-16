// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};

use irokle::sync::{
    ActorFilter, ActorRangeHint, ActorWindow, SyncAck, SyncCredit, SyncMessage, SyncPage,
    SyncReceipt, SyncRequest, SyncSummary,
};
use irokle::{
    ActorClock, ActorId, Ed25519Signer, Op, OpBody, OpId, PeerId, ReplicationPolicy, Signer,
    TopicConfig, TopicControl, TopicId, TopicPayload,
};

const FIXTURE_ENV: &str = "IROKLE_CONTRACTS";

struct Values {
    op: Op,
    ack: SyncAck,
    summary: SyncSummary,
    request: SyncRequest,
    page: SyncPage,
    receipt: SyncReceipt,
    config: TopicConfig,
}

fn fixture_path(name: &str) -> PathBuf {
    std::env::var_os(FIXTURE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/contracts"))
        .join(name)
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

fn values() -> Values {
    let signer = Ed25519Signer::from_bytes(&[7; 32]);
    let peer = signer.peer_id();
    let other = PeerId::from_bytes([8; 32]);
    let topic = TopicId::from_bytes([1; 32]);
    let actor = ActorId::from_bytes([2; 32]);
    let other_actor = ActorId::from_bytes([9; 32]);
    let genesis = OpId::from_bytes([3; 32]);
    let head = OpId::from_bytes([4; 32]);
    let wanted = OpId::from_bytes([5; 32]);
    let config = TopicConfig {
        initial_peers: BTreeSet::from([peer, other]),
        replication_policy: ReplicationPolicy::selected([other]).with_max_sync_peers(3),
    };
    let op = Op::sign(
        OpBody {
            topic_id: topic,
            author: peer,
            actor_id: actor,
            actor_seq: 6,
            actor_prev: Some(genesis),
            deps: BTreeSet::from([genesis, head]),
            generation: 7,
            payload: TopicPayload::Control(TopicControl::SetReplicationPolicy {
                policy: config.replication_policy.clone(),
            }),
        },
        &signer,
    )
    .unwrap();
    let mut clock = ActorClock::new();
    clock.set(actor, 6);
    clock.set(other_actor, 2);
    let receipt = SyncReceipt {
        topic_id: topic,
        genesis,
        session: 11,
        clock: clock.clone(),
    };
    let summary = SyncSummary {
        topic_id: topic,
        event_type_id: Some("example.note.v1".to_owned()),
        genesis: Some(genesis),
        fingerprint: [12; 32],
        heads: BTreeSet::from([head]),
        actor_clock: clock.clone(),
        actor_tips: BTreeMap::from([(actor, (6, head)), (other_actor, (2, wanted))]),
        staged: Some(receipt.clone()),
    };
    let request = SyncRequest {
        topic_id: topic,
        known: BTreeSet::from([genesis]),
        wants: BTreeSet::from([head, wanted]),
        actor_range_hints: vec![ActorRangeHint {
            actor_id: actor,
            from_exclusive: 2,
            to_inclusive: 6,
        }],
        genesis: Some(genesis),
        credit: SyncCredit {
            ops: 13,
            bytes: 4096,
        },
        window: ActorWindow {
            after: Some(ActorId::from_bytes([1; 32])),
            through: Some(ActorId::from_bytes([10; 32])),
            behind: Some(ActorFilter {
                bits: vec![0x5a, 0xa5],
            }),
        },
    };
    let page = SyncPage {
        topic_id: topic,
        more: true,
        missing: BTreeSet::from([wanted]),
        positions: BTreeSet::from([other_actor]),
        continued: false,
    };
    let mut ack = SyncAck {
        topic_id: topic,
        peer_id: peer,
        genesis: Some(genesis),
        accepted: BTreeSet::from([op.id]),
        heads: BTreeSet::from([head]),
        clock,
        signature: None,
    };
    ack.sign(&signer).unwrap();
    Values {
        op,
        ack,
        summary,
        request,
        page,
        receipt,
        config,
    }
}

fn write_wire(name: &str, message: SyncMessage) {
    save(
        &fixture_path(name),
        &irokle::net::encode_sync_message(&message).unwrap(),
    );
}

fn assert_wire(name: &str, expected: SyncMessage) {
    let bytes = std::fs::read(fixture_path(name)).unwrap();
    let actual = irokle::net::decode_sync_message(&bytes).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(irokle::net::encode_sync_message(&actual).unwrap(), bytes);
}

#[test]
#[ignore = "writes frozen contracts from the reviewed baseline"]
fn write_contracts() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.4");
    let directory = PathBuf::from(std::env::var(FIXTURE_ENV).expect(FIXTURE_ENV));
    std::fs::create_dir(&directory).unwrap();
    let values = values();
    save(
        &fixture_path("operation.bin"),
        &postcard::to_stdvec(&values.op).unwrap(),
    );
    write_wire("ack.bin", SyncMessage::Ack(values.ack));
    write_wire("summary.bin", SyncMessage::Summary(values.summary));
    write_wire("request.bin", SyncMessage::Request(values.request));
    write_wire("page.bin", SyncMessage::Page(values.page));
    write_wire("receipt.bin", SyncMessage::Receipt(values.receipt));
    save(
        &fixture_path("configuration.bin"),
        &postcard::to_stdvec(&values.config).unwrap(),
    );
}

#[test]
fn read_contracts() {
    assert_eq!(irokle::sync::MAX_ACTOR_RANGE_HINT_SPAN, 65_536);
    assert_eq!(irokle::sync::MAX_ACTOR_FILTER_BYTES, 1024 * 1024);
    let values = values();
    let op_bytes = std::fs::read(fixture_path("operation.bin")).unwrap();
    let op: Op = postcard::from_bytes(&op_bytes).unwrap();
    assert_eq!(op, values.op);
    assert_eq!(postcard::to_stdvec(&op).unwrap(), op_bytes);
    op.validate().unwrap();

    assert_wire("ack.bin", SyncMessage::Ack(values.ack.clone()));
    values.ack.verify_signature().unwrap();
    assert_wire("summary.bin", SyncMessage::Summary(values.summary));
    assert_wire("request.bin", SyncMessage::Request(values.request));
    assert_wire("page.bin", SyncMessage::Page(values.page));
    assert_wire("receipt.bin", SyncMessage::Receipt(values.receipt));

    let config_bytes = std::fs::read(fixture_path("configuration.bin")).unwrap();
    let config: TopicConfig = postcard::from_bytes(&config_bytes).unwrap();
    assert_eq!(config, values.config);
    assert_eq!(postcard::to_stdvec(&config).unwrap(), config_bytes);
}

#[cfg(feature = "iroh")]
#[test]
fn net_items_compile() {
    fn consume(
        responses: irokle::net::SyncResponses,
    ) -> impl Iterator<Item = irokle::net::SyncResponse> {
        responses.into_iter()
    }
    fn borrow(item: &irokle::net::SyncResponse) -> &SyncMessage {
        item.as_ref()
    }
    let _ = (consume, borrow);
}
