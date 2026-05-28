// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use auto_irokle::{AutoDoc, AutoEvent, AutoIrokle, AutoIrokleExt, PatchOp};
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
fn change_refreshes_before_diffing() {
    let alice = node(58);
    let bob = node(59);
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

    bob_doc
        .change(|note| {
            note.tags.insert("from-bob".into());
        })
        .unwrap();
    sync_pair(&bob, &alice, alice_doc.id());

    alice_doc
        .change(|note| note.title = "from-alice".into())
        .unwrap();

    assert_eq!(alice_doc.state().title, "from-alice");
    assert!(alice_doc.state().tags.contains("from-bob"));
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
fn malformed_multi_op_patch_does_not_partially_apply_or_get_marked_applied() {
    let node = node(60);
    let mut doc = node
        .create_doc(Note::new("draft"), TopicConfig::default())
        .unwrap();

    let patch = AutoEvent::patch(vec![
        PatchOp::set(
            vec!["title".into()],
            postcard::to_allocvec(&"partial".to_owned()).unwrap(),
        ),
        PatchOp::set(vec!["definitely_not_a_field".into()], vec![0xff]),
    ]);
    doc.topic().publish(patch).unwrap();

    let first = doc.refresh().unwrap_err();
    assert!(matches!(first, irokle::Error::Decode(_)));
    assert_eq!(doc.state().title, "draft");

    let second = doc.refresh().unwrap_err();
    assert!(matches!(second, irokle::Error::Decode(_)));
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

#[test]
fn refresh_is_incremental() {
    let node = node(40);
    let mut doc = node
        .create_doc(Note::new("v0"), TopicConfig::default())
        .unwrap();

    doc.change(|note| note.title = "v1".into()).unwrap();
    doc.change(|note| note.title = "v2".into()).unwrap();
    doc.change(|note| {
        note.tags.insert("hot".into());
    })
    .unwrap();

    let before = doc.projection().clone();
    doc.refresh().unwrap();
    let after = doc.projection().clone();
    assert_eq!(before, after);
}

#[test]
fn snapshot_round_trip() {
    let node = node(41);
    let mut doc = node
        .create_doc(Note::new("v0"), TopicConfig::default())
        .unwrap();
    doc.change(|note| {
        note.title = "snap-state".into();
        note.tags.insert("a".into());
        note.tags.insert("b".into());
        note.attrs.insert("k".into(), "v".into());
    })
    .unwrap();
    let topic_id = doc.id();
    let snap = doc.snapshot().unwrap();
    drop(doc);

    let restored: AutoDoc<Note> = node.open_doc_from_snapshot(topic_id, &snap).unwrap();
    assert_eq!(restored.state().title, "snap-state");
    assert!(restored.state().tags.contains("a"));
    assert!(restored.state().tags.contains("b"));
    assert_eq!(
        restored.state().attrs.get("k").map(String::as_str),
        Some("v")
    );
}

#[test]
fn snapshot_then_refresh_picks_up_new_ops() {
    let node = node(42);
    let mut doc = node
        .create_doc(Note::new("v0"), TopicConfig::default())
        .unwrap();
    doc.change(|note| note.title = "snap".into()).unwrap();
    let topic_id = doc.id();
    let snap = doc.snapshot().unwrap();

    doc.change(|note| note.title = "after-snap".into()).unwrap();

    let restored: AutoDoc<Note> = node.open_doc_from_snapshot(topic_id, &snap).unwrap();
    assert_eq!(restored.state().title, "after-snap");
}

#[test]
fn snapshot_magic_mismatch_fails() {
    let node = node(43);
    let doc = node
        .create_doc(Note::new("v0"), TopicConfig::default())
        .unwrap();
    match node.open_doc_from_snapshot::<Note>(doc.id(), b"GARBAGE_BYTES") {
        Err(irokle::Error::Decode(_)) => {}
        Err(other) => panic!("expected Decode error, got {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn snapshot_topic_mismatch_fails() {
    let node = node(61);
    let doc = node
        .create_doc(Note::new("one"), TopicConfig::default())
        .unwrap();
    let other = node
        .create_doc(Note::new("two"), TopicConfig::default())
        .unwrap();
    let snap = doc.snapshot().unwrap();

    match node.open_doc_from_snapshot::<Note>(other.id(), &snap) {
        Err(irokle::Error::Decode(msg)) => assert!(msg.contains("topic mismatch")),
        Err(other) => panic!("expected Decode error, got {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn snapshot_event_type_mismatch_fails() {
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
    #[auto_irokle(type_id = "test.auto.other-note")]
    struct OtherNote {
        title: String,
        tags: BTreeSet<String>,
        attrs: BTreeMap<String, String>,
        lines: Vec<String>,
    }

    let node = node(62);
    let doc = node
        .create_doc(Note::new("v0"), TopicConfig::default())
        .unwrap();
    let snap = doc.snapshot().unwrap();

    match node.open_doc_from_snapshot::<OtherNote>(doc.id(), &snap) {
        Err(irokle::Error::Decode(msg)) => assert!(msg.contains("event type mismatch")),
        Err(other) => panic!("expected Decode error, got {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn snapshot_frontier_must_exist_locally() {
    let alice = node(63);
    let bob = node(64);
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

    alice_doc
        .change(|note| note.title = "not-on-bob-yet".into())
        .unwrap();
    let snap = alice_doc.snapshot().unwrap();

    match bob.open_doc_from_snapshot::<Note>(alice_doc.id(), &snap) {
        Err(irokle::Error::Decode(msg)) => assert!(msg.contains("frontier")),
        Err(other) => panic!("expected Decode error, got {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

#[test]
fn gc_prunes_stable_tombstone() {
    let alice = node(44);
    let bob = node(45);
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

    alice_doc
        .change(|note| {
            note.tags.insert("temp".into());
        })
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    bob_doc.refresh().unwrap();

    alice_doc
        .change(|note| {
            note.tags.remove("temp");
        })
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    bob_doc.refresh().unwrap();
    sync_pair(&bob, &alice, alice_doc.id());

    let pruned = alice_doc.gc().unwrap();
    assert!(
        pruned >= 1,
        "expected at least one pruned tombstone, got {pruned}"
    );
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(nested)]
struct Inner {
    title: String,
    tags: BTreeSet<String>,
}

impl Inner {
    fn empty() -> Self {
        Self {
            title: String::new(),
            tags: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(type_id = "test.auto.nested.doc")]
struct NestedDoc {
    label: String,
    #[auto_irokle(kind = "nested")]
    inner: Inner,
}

#[test]
fn nested_struct_field_diff_merges_concurrently() {
    let alice = node(50);
    let bob = node(51);
    let mut alice_doc = alice
        .create_doc(
            NestedDoc {
                label: "start".into(),
                inner: Inner::empty(),
            },
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<NestedDoc>(alice_doc.id()).unwrap();

    alice_doc
        .change(|doc| doc.inner.title = "from-alice".into())
        .unwrap();
    bob_doc
        .change(|doc| {
            doc.inner.tags.insert("from-bob".into());
        })
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    assert_eq!(alice_doc.state().inner.title, "from-alice");
    assert!(alice_doc.state().inner.tags.contains("from-bob"));
    assert_eq!(bob_doc.state().inner.title, "from-alice");
    assert!(bob_doc.state().inner.tags.contains("from-bob"));
}

#[test]
fn nested_set_field_merges_concurrent_inserts() {
    let alice = node(52);
    let bob = node(53);
    let mut alice_doc = alice
        .create_doc(
            NestedDoc {
                label: "draft".into(),
                inner: Inner::empty(),
            },
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<NestedDoc>(alice_doc.id()).unwrap();

    alice_doc
        .change(|doc| {
            doc.inner.tags.insert("alice".into());
        })
        .unwrap();
    bob_doc
        .change(|doc| {
            doc.inner.tags.insert("bob".into());
        })
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    assert!(alice_doc.state().inner.tags.contains("alice"));
    assert!(alice_doc.state().inner.tags.contains("bob"));
    assert_eq!(alice_doc.state(), bob_doc.state());
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(nested)]
struct Leaf {
    value: String,
    tags: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(nested)]
struct Mid {
    #[auto_irokle(kind = "nested")]
    leaf: Leaf,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(type_id = "test.auto.deep")]
struct Deep {
    #[auto_irokle(kind = "nested")]
    mid: Mid,
}

#[test]
fn deeply_nested_two_levels_merge() {
    let alice = node(54);
    let bob = node(55);
    let initial = Deep {
        mid: Mid {
            leaf: Leaf {
                value: String::new(),
                tags: BTreeSet::new(),
            },
        },
    };
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
    let mut bob_doc = bob.open_doc::<Deep>(alice_doc.id()).unwrap();

    alice_doc
        .change(|doc| doc.mid.leaf.value = "alice".into())
        .unwrap();
    bob_doc
        .change(|doc| {
            doc.mid.leaf.tags.insert("bob".into());
        })
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    assert_eq!(alice_doc.state().mid.leaf.value, "alice");
    assert!(alice_doc.state().mid.leaf.tags.contains("bob"));
    assert_eq!(bob_doc.state(), alice_doc.state());
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PlainStruct {
    name: String,
    count: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(type_id = "test.auto.legacy")]
struct LegacyDoc {
    title: String,
    plain: PlainStruct,
}

#[test]
fn nested_struct_without_kind_attr_stays_lww() {
    let alice = node(56);
    let bob = node(57);
    let mut alice_doc = alice
        .create_doc(
            LegacyDoc {
                title: "draft".into(),
                plain: PlainStruct {
                    name: "init".into(),
                    count: 0,
                },
            },
            TopicConfig {
                initial_peers: [bob.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let mut bob_doc = bob.open_doc::<LegacyDoc>(alice_doc.id()).unwrap();

    let alice_record = alice_doc
        .change(|doc| {
            doc.plain.name = "alice".into();
        })
        .unwrap()
        .unwrap();
    let bob_record = bob_doc
        .change(|doc| {
            doc.plain.count = 42;
        })
        .unwrap()
        .unwrap();

    sync_pair(&alice, &bob, alice_doc.id());
    alice_doc.refresh().unwrap();
    bob_doc.refresh().unwrap();

    let winner = if alice_record.meta.op_id > bob_record.meta.op_id {
        PlainStruct {
            name: "alice".into(),
            count: 0,
        }
    } else {
        PlainStruct {
            name: "init".into(),
            count: 42,
        }
    };
    assert_eq!(alice_doc.state().plain, winner);
    assert_eq!(bob_doc.state().plain, winner);
}

#[test]
fn gc_preserves_tombstone_with_lagging_member() {
    let alice = node(46);
    let bob = node(47);
    let carol = node(48);
    let mut alice_doc = alice
        .create_doc(
            Note::new("draft"),
            TopicConfig {
                initial_peers: [bob.peer_id(), carol.peer_id()].into(),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    let _bob_doc = bob.open_doc::<Note>(alice_doc.id()).unwrap();

    alice_doc
        .change(|note| {
            note.tags.insert("temp".into());
        })
        .unwrap();
    alice_doc
        .change(|note| {
            note.tags.remove("temp");
        })
        .unwrap();
    sync_pair(&alice, &bob, alice_doc.id());
    sync_pair(&bob, &alice, alice_doc.id());

    let pruned = alice_doc.gc().unwrap();
    assert_eq!(
        pruned, 0,
        "expected no prune while carol is lagging, got {pruned}"
    );
}
