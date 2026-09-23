//! Typed history reads: each record decoded on its own, and incremental pages
//! read from one snapshot with the cursor that covers them.

use crate::history::HistoryOrder::OldestFirst;
use crate::oplog::Oplog;
use crate::reducer::HistoryEntry;
use crate::tests::support::*;

/// An admitted event whose payload does not decode fails `history`, while the
/// per-record read decodes the others and names the failing op.
#[test]
fn entries_name_undecodable() {
    let signer = Ed25519Signer::from_bytes(&[200; 32]);
    let node = Irokle::builder()
        .with_signer(signer.clone())
        .build()
        .unwrap();
    let topic = node.create_topic::<Note>(TopicConfig::default()).unwrap();
    topic
        .publish(Note {
            text: "before".into(),
        })
        .unwrap();
    let malformed = EventEnvelope {
        type_id: Note::TYPE_ID.into(),
        payload: Vec::<u8>::new().into(),
    };
    let actor = actor_id_for(topic.id(), signer.peer_id());
    let bad = Oplog::with_storage(node.storage().clone())
        .create_event_op(topic.id(), actor, malformed, &signer)
        .unwrap();
    topic
        .publish(Note {
            text: "after".into(),
        })
        .unwrap();

    assert!(topic.history(OldestFirst).is_err());
    let entries = topic.history_entries(OldestFirst).unwrap();
    assert_eq!(entries.len(), 3);
    assert!(matches!(&entries[0], HistoryEntry::Event(record) if record.event.text == "before"));
    assert!(matches!(&entries[1], HistoryEntry::Undecodable { meta, .. } if meta.op_id == bad.id));
    assert!(matches!(&entries[2], HistoryEntry::Event(record) if record.event.text == "after"));
}

/// Two writers interleave events. Pages of one op each follow causal order,
/// cover every event once, and end at the topic's own cursor; a caught-up page
/// reads no op record.
#[test]
fn pages_follow_causality() {
    let alice = Ed25519Signer::from_bytes(&[201; 32]);
    let bob = Ed25519Signer::from_bytes(&[202; 32]);
    let storage = MemoryStorage::new();
    let node = Irokle::builder()
        .with_signer(alice)
        .with_storage(storage.clone())
        .build()
        .unwrap();
    let topic = node
        .create_topic::<Note>(TopicConfig {
            initial_peers: [bob.peer_id()].into(),
            ..TopicConfig::default()
        })
        .unwrap();
    let mut cursor = topic.history_cursor().unwrap();
    let log = Oplog::with_storage(storage.clone());
    let bob_actor = actor_id_for(topic.id(), bob.peer_id());
    for index in 0..4 {
        topic
            .publish(Note {
                text: format!("alice-{index}"),
            })
            .unwrap();
        let note = Note {
            text: format!("bob-{index}"),
        };
        log.create_event_op(
            topic.id(),
            bob_actor,
            EventEnvelope::encode_event(&note).unwrap(),
            &bob,
        )
        .unwrap();
    }

    let mut read = Vec::new();
    loop {
        let page = topic.history_page(&cursor, Some(1)).unwrap();
        if page.entries.is_empty() {
            assert_eq!(page.cursor, cursor);
            break;
        }
        assert_eq!(page.entries.len(), 1);
        for entry in page.entries {
            let id = entry.into_record().unwrap().meta.op_id;
            let deps = storage.get_meta(&id).unwrap().unwrap().deps;
            let genesis = storage.topic_state(&topic.id()).unwrap().unwrap().genesis;
            assert!(deps.iter().all(|dep| *dep == genesis || read.contains(dep)));
            read.push(id);
        }
        cursor = page.cursor;
    }
    let full = topic
        .history(OldestFirst)
        .unwrap()
        .into_iter()
        .map(|record| record.meta.op_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(read.iter().copied().collect::<BTreeSet<_>>(), full);
    assert_eq!(read.len(), 8);
    assert_eq!(cursor, topic.history_cursor().unwrap());

    let before = storage.counters();
    assert!(
        topic
            .history_page(&cursor, None)
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(storage.counters().op_reads, before.op_reads);
    assert!(
        topic
            .history_after(&cursor, OldestFirst)
            .unwrap()
            .is_empty()
    );
}
