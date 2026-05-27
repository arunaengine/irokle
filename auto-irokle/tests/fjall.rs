// SPDX-License-Identifier: MIT OR Apache-2.0

#![cfg(feature = "fjall")]

use std::collections::{BTreeMap, BTreeSet};

use auto_irokle::{AutoIrokle, AutoIrokleExt};
use irokle::{Ed25519Signer, Irokle, TopicConfig, TopicId};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AutoIrokle)]
#[auto_irokle(type_id = "test.auto.fjall.note")]
struct Note {
    title: String,
    tags: BTreeSet<String>,
    attrs: BTreeMap<String, String>,
}

impl Note {
    fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            tags: BTreeSet::new(),
            attrs: BTreeMap::new(),
        }
    }
}

fn open_node(path: &std::path::Path, seed: u8) -> Irokle<irokle::FjallStorage> {
    Irokle::builder()
        .with_signer(Ed25519Signer::from_bytes(&[seed; 32]))
        .with_fjall_path(path)
        .unwrap()
        .build()
        .unwrap()
}

#[test]
fn fjall_round_trip_survives_node_restart() {
    let dir = TempDir::new().unwrap();

    let topic_id: TopicId = {
        let node = open_node(dir.path(), 99);
        let mut doc = node
            .create_doc(Note::new("draft"), TopicConfig::default())
            .unwrap();
        doc.change(|note| {
            note.title = "after-restart".into();
            note.tags.insert("persisted".into());
            note.attrs.insert("kind".into(), "note".into());
        })
        .unwrap();
        doc.id()
    };

    let node = open_node(dir.path(), 99);
    let doc = node.open_doc::<Note>(topic_id).unwrap();
    assert_eq!(doc.state().title, "after-restart");
    assert!(doc.state().tags.contains("persisted"));
    assert_eq!(doc.state().attrs.get("kind").map(String::as_str), Some("note"));
}
