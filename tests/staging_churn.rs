// SPDX-License-Identifier: MIT OR Apache-2.0
//! Disk bytes of a Fjall store under bootstrap staging churn, alongside staged and clearing
//! bytes counted by its limits. Logical deletion does not return file space at once; run
//! explicitly: `cargo test --release --features fjall --test staging_churn -- --ignored --nocapture`.

#![cfg(feature = "fjall")]

use irokle::oplog::topological;
use irokle::storage::Storage;
use irokle::{
    Ed25519Signer, FjallStorage, Irokle, NodeConfig, Op, PeerId, Signer, TopicConfig, TopicId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, irokle::Event, Serialize, Deserialize)]
#[irokle(type_id = "test.staging_churn.note")]
struct Note {
    text: String,
}

/// A topic of `seed` with `events` notes of 4 KiB and an invitation for `reader`.
fn history(seed: u8, events: usize, reader: PeerId) -> (TopicId, PeerId, Vec<Op>) {
    let source = Irokle::with_storage(
        irokle::MemoryStorage::new(),
        NodeConfig {
            signer: Ed25519Signer::from_bytes(&[seed; 32]),
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..events {
        let text = format!("{index:0>4096}");
        topic.publish(Note { text }).unwrap();
    }
    topic.add_peer(reader).unwrap();
    let ops = topological(source.storage(), &topic.id()).unwrap();
    (topic.id(), source.peer_id(), ops)
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                directory_bytes(&entry.path())
            } else {
                meta.len()
            }
        })
        .sum()
}

/// Bytes of the journal files and of each keyspace directory.
fn breakdown(path: &std::path::Path) -> String {
    let mut parts = Vec::new();
    let journals = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".jnl"))
        .map(|entry| entry.metadata().unwrap().len())
        .sum::<u64>();
    parts.push(format!("journals={journals}"));
    let mut spaces = std::fs::read_dir(path.join("keyspaces"))
        .unwrap()
        .map(|entry| entry.unwrap())
        .map(|entry| {
            (
                entry.file_name().to_string_lossy().to_string(),
                directory_bytes(&entry.path()),
            )
        })
        .collect::<Vec<_>>();
    spaces.sort();
    for (name, bytes) in spaces {
        parts.push(format!("keyspace_{name}={bytes}"));
    }
    parts.join(" ")
}

/// Stage about 6 MiB of history, end its session, and open the next one to empty its slot
/// for `CHURN_CYCLES` cycles (200 by default). Every ten cycles report directory bytes and
/// staged bytes counted by the store.
#[test]
#[ignore = "writes gigabytes of staging churn, run explicitly"]
fn fjall_staging_churn() {
    let reader = Ed25519Signer::from_bytes(&[1; 32]).peer_id();
    let histories = (0..4)
        .map(|index| history(40 + index, 1_500, reader))
        .collect::<Vec<_>>();
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path()).unwrap();
    let started = std::time::Instant::now();
    let cycles = std::env::var("CHURN_CYCLES").map_or(200, |cycles| cycles.parse().unwrap());
    for cycle in 0..cycles {
        let (topic_id, source, ops) = &histories[cycle % histories.len()];
        let provisional = storage
            .open_provisional(*source, *topic_id, ops[0].id, 1_000)
            .unwrap();
        let store = storage.provisional_store(&provisional).unwrap().unwrap();
        irokle::oplog::Oplog::with_storage(store.clone())
            .receive_ops_from_peer(Some(*source), ops.clone())
            .unwrap();
        let staged = storage
            .provisional_topics()
            .unwrap()
            .iter()
            .map(|topic| topic.bytes)
            .sum::<u64>();
        let current = storage
            .provisional_topics()
            .unwrap()
            .into_iter()
            .find(|topic| topic.session == provisional.session)
            .unwrap();
        assert!(storage.discard_provisional(&current).unwrap());
        if cycle % 10 == 9 {
            println!(
                "cycle={} staged_bytes_before_discard={staged} directory_bytes={} {} elapsed_ms={}",
                cycle + 1,
                directory_bytes(dir.path()),
                breakdown(dir.path()),
                started.elapsed().as_millis()
            );
        }
    }
    drop(storage);
    println!("closed directory_bytes={}", directory_bytes(dir.path()));
    drop(FjallStorage::open(dir.path()).unwrap());
    println!("reopened directory_bytes={}", directory_bytes(dir.path()));
}
