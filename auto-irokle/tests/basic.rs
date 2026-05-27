// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use auto_irokle::{AutoEvent, AutoIrokle, AutoIrokleExt, PatchOp};
use irokle::{Ed25519Signer, Irokle, TopicConfig, TopicId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(type_id = "test.auto.note")]
struct Note {
    title: String,
    tags: BTreeSet<String>,
    attrs: BTreeMap<String, String>,
    lines: Vec<String>,
}

impl Note {
    fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            tags: BTreeSet::new(),
            attrs: BTreeMap::new(),
            lines: Vec::new(),
        }
    }
}

fn node(seed: u8) -> Irokle {
    Irokle::builder()
        .with_signer(Ed25519Signer::from_bytes(&[seed; 32]))
        .build()
        .unwrap()
}

fn sync_pair(a: &Irokle, b: &Irokle, topic_id: TopicId) {
    let b_summary = b.sync_summary(topic_id).unwrap();
    let data_for_b = a.plan_sync_data(b.peer_id(), &b_summary).unwrap();
    let request_for_a = a.plan_sync_request(b.peer_id(), &b_summary).unwrap();

    let b_ack = b.receive_sync_data_from(a.peer_id(), data_for_b).unwrap();
    let data_for_a = b
        .plan_sync_response_data(a.peer_id(), &request_for_a)
        .unwrap();
    let a_ack = a.receive_sync_data_from(b.peer_id(), data_for_a).unwrap();

    a.apply_sync_ack(&b_ack).unwrap();
    b.apply_sync_ack(&a_ack).unwrap();
}

#[test]
fn document_id_is_topic_id() {
    let node = node(1);
    let mut doc = node
        .create_doc(Note::new("draft"), TopicConfig::default())
        .unwrap();
    let id = doc.id();

    doc.change(|note| note.title = "final".into()).unwrap();

    let opened = node.open_doc::<Note>(id).unwrap();

    assert_eq!(opened.id(), id);
    assert_eq!(opened.state().title, "final");
}

#[test]
fn derive_supports_renamed_auto_irokle_crate() {
    use auto_irokle as renamed_auto;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, renamed_auto::AutoIrokle)]
    #[auto_irokle(type_id = "test.auto.renamed", crate = "renamed_auto")]
    struct Renamed {
        value: String,
    }

    let node = node(14);
    let mut doc = node
        .create_doc(
            Renamed {
                value: "before".into(),
            },
            TopicConfig::default(),
        )
        .unwrap();

    doc.change(|state| state.value = "after".into()).unwrap();

    assert_eq!(doc.state().value, "after");
}

#[test]
fn syncs_document_to_peer() {
    let alice = node(2);
    let bob = node(3);
    let mut doc = alice
        .create_doc(
            Note::new("draft"),
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();

    doc.change(|note| {
        note.title = "synced".into();
        note.tags.insert("irokle".into());
        note.attrs.insert("kind".into(), "note".into());
        note.lines = vec!["one".into(), "two".into()];
    })
    .unwrap();

    sync_pair(&alice, &bob, doc.id());
    let bob_doc = bob.open_doc::<Note>(doc.id()).unwrap();

    assert_eq!(bob_doc.state(), doc.state());
}

#[test]
fn concurrent_lww_registers_converge_with_op_id_tiebreak() {
    let alice = node(4);
    let bob = node(5);
    let mut alice_doc = alice
        .create_doc(
            Note::new("draft"),
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<Note>(alice_doc.id()).unwrap();

    let alice_record = alice_doc
        .change(|note| note.title = "alice".into())
        .unwrap()
        .unwrap();
    let bob_record = bob_doc
        .change(|note| note.title = "bob".into())
        .unwrap()
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    let expected = if alice_record.meta.op_id > bob_record.meta.op_id {
        "alice"
    } else {
        "bob"
    };

    assert_eq!(alice_doc.state().title, expected);
    assert_eq!(bob_doc.state().title, expected);
}

#[test]
fn vec_fields_are_lww_registers() {
    let alice = node(6);
    let bob = node(7);
    let mut alice_doc = alice
        .create_doc(
            Note::new("draft"),
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<Note>(alice_doc.id()).unwrap();

    let alice_record = alice_doc
        .change(|note| note.lines = vec!["a".into(), "b".into()])
        .unwrap()
        .unwrap();
    let bob_record = bob_doc
        .change(|note| note.lines = vec!["b".into(), "a".into()])
        .unwrap()
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    let expected = if alice_record.meta.op_id > bob_record.meta.op_id {
        vec!["a".to_owned(), "b".to_owned()]
    } else {
        vec!["b".to_owned(), "a".to_owned()]
    };

    assert_eq!(alice_doc.state().lines, expected);
    assert_eq!(bob_doc.state().lines, expected);
}

#[test]
fn observed_remove_set_removes_observed_add() {
    let alice = node(8);
    let bob = node(9);
    let mut initial = Note::new("draft");
    initial.tags.insert("irokle".into());
    let mut alice_doc = alice
        .create_doc(
            initial,
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<Note>(alice_doc.id()).unwrap();

    bob_doc
        .change(|note| {
            note.tags.remove("irokle");
        })
        .unwrap();

    sync_pair(&bob, &alice, alice_doc.id());
    alice_doc.refresh().unwrap();

    assert!(!alice_doc.state().tags.contains("irokle"));
}

#[test]
fn observed_remove_set_keeps_concurrent_readd() {
    let alice = node(10);
    let bob = node(11);
    let mut initial = Note::new("draft");
    initial.tags.insert("irokle".into());
    let mut alice_doc = alice
        .create_doc(
            initial,
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<Note>(alice_doc.id()).unwrap();

    alice_doc
        .change(|note| {
            note.tags.remove("irokle");
        })
        .unwrap();
    alice_doc
        .change(|note| {
            note.tags.insert("irokle".into());
        })
        .unwrap();
    bob_doc
        .change(|note| {
            note.tags.remove("irokle");
        })
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    assert!(alice_doc.state().tags.contains("irokle"));
    assert!(bob_doc.state().tags.contains("irokle"));
}

#[test]
fn empty_change_returns_none() {
    let node = node(20);
    let mut doc = node
        .create_doc(Note::new("draft"), TopicConfig::default())
        .unwrap();

    let record = doc.change(|_note| {}).unwrap();
    assert!(record.is_none());

    let same_value_record = doc
        .change(|note| {
            note.title = "draft".into();
        })
        .unwrap();
    assert!(same_value_record.is_none());
}

#[test]
fn rogue_init_is_ignored() {
    let node = node(21);
    let mut doc = node
        .create_doc(Note::new("first"), TopicConfig::default())
        .unwrap();

    doc.change(|note| note.title = "after-mutation".into())
        .unwrap();

    let mut wiped = Note::new("rogue-replacement");
    wiped.tags.insert("rogue-tag".into());
    let rogue_init = AutoEvent::init(&wiped).unwrap();
    doc.topic().publish(rogue_init).unwrap();

    doc.refresh().unwrap();

    assert_eq!(doc.state().title, "after-mutation");
    assert!(doc.state().tags.is_empty());
}

#[test]
fn explicit_kind_lww_overrides_set_detection() {
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
    #[auto_irokle(type_id = "test.auto.forced_lww")]
    struct Doc {
        #[auto_irokle(kind = "lww")]
        tags: BTreeSet<String>,
    }

    let alice = node(30);
    let bob = node(31);
    let mut alice_doc = alice
        .create_doc(
            Doc {
                tags: BTreeSet::new(),
            },
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<Doc>(alice_doc.id()).unwrap();

    let alice_record = alice_doc
        .change(|doc| {
            doc.tags.insert("alice".into());
        })
        .unwrap()
        .unwrap();
    let bob_record = bob_doc
        .change(|doc| {
            doc.tags.insert("bob".into());
        })
        .unwrap()
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    let winner: BTreeSet<String> = if alice_record.meta.op_id > bob_record.meta.op_id {
        ["alice".to_owned()].into()
    } else {
        ["bob".to_owned()].into()
    };
    assert_eq!(alice_doc.state().tags, winner);
    assert_eq!(bob_doc.state().tags, winner);
}

#[test]
fn malformed_patch_surfaces_decode_error() {
    let node = node(22);
    let mut doc = node
        .create_doc(Note::new("draft"), TopicConfig::default())
        .unwrap();

    let bogus = AutoEvent::patch(vec![PatchOp::set(
        vec!["definitely_not_a_field".into()],
        vec![0xff, 0xff, 0xff],
    )]);
    doc.topic().publish(bogus).unwrap();

    let err = doc.refresh().unwrap_err();
    match err {
        irokle::Error::Decode(msg) => {
            assert!(
                msg.contains("unsupported auto-irokle patch op"),
                "unexpected decode error: {msg}"
            );
        }
        other => panic!("expected Error::Decode, got {other:?}"),
    }
}

#[test]
fn map_fields_use_observed_remove_keys_and_lww_values() {
    let alice = node(12);
    let bob = node(13);
    let mut alice_doc = alice
        .create_doc(
            Note::new("draft"),
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<Note>(alice_doc.id()).unwrap();

    let alice_record = alice_doc
        .change(|note| {
            note.attrs.insert("status".into(), "alice".into());
        })
        .unwrap()
        .unwrap();
    let bob_record = bob_doc
        .change(|note| {
            note.attrs.insert("status".into(), "bob".into());
        })
        .unwrap()
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    let expected = if alice_record.meta.op_id > bob_record.meta.op_id {
        "alice"
    } else {
        "bob"
    };
    assert_eq!(alice_doc.state().attrs.get("status").unwrap(), expected);
    assert_eq!(bob_doc.state().attrs.get("status").unwrap(), expected);

    alice_doc
        .change(|note| {
            note.attrs.remove("status");
        })
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    bob_doc.refresh().unwrap();

    assert!(!bob_doc.state().attrs.contains_key("status"));
}
