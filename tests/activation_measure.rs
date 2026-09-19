//! Deterministic signed fixtures; only provisional activation is timed.
#![cfg(all(feature = "fjall", unix))]

use irokle::oplog::Oplog;
use irokle::storage::{AdmissionEffects, Storage};
use irokle::{
    Ed25519Signer, Event, EventEnvelope, FjallStorage, MemoryStorage, Op, OpBody, Signer,
    TopicGenesis, TopicId, TopicPayload, actor_id_for,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::io::{Read, Write};

#[derive(Clone, Debug, irokle::Event, Serialize, Deserialize)]
#[irokle(type_id = "test.activation.measure")]
struct Note {
    value: u64,
}

fn fixture(actors: usize, count: usize) -> (TopicId, Vec<Op>) {
    assert!(actors > 0 && count >= actors);
    let owner = Ed25519Signer::from_bytes(&[201; 32]);
    let topic = TopicId::hash(format!("activation-measure/{actors}/{count}").as_bytes());
    let writers = (0..actors)
        .map(|index| {
            let mut seed = [202; 32];
            seed[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Ed25519Signer::from_bytes(&seed)
        })
        .collect::<Vec<_>>();
    let members: BTreeSet<_> = writers
        .iter()
        .chain([&owner])
        .map(Signer::peer_id)
        .collect();
    let genesis = Op::sign(
        OpBody {
            topic_id: topic,
            author: owner.peer_id(),
            actor_id: actor_id_for(topic, owner.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new(Note::TYPE_ID, members)),
        },
        &owner,
    )
    .unwrap();
    let mut ops = vec![genesis];
    let mut previous = vec![None; actors];
    for index in 0..count {
        let writer = index % actors;
        let signer = &writers[writer];
        let mut deps = BTreeSet::from([ops.last().unwrap().id]);
        deps.extend(previous[writer]);
        let op = Op::sign(
            OpBody {
                topic_id: topic,
                author: signer.peer_id(),
                actor_id: actor_id_for(topic, signer.peer_id()),
                actor_seq: (index / actors + 1) as u64,
                actor_prev: previous[writer],
                deps,
                generation: (index + 1) as u64,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note {
                        value: index as u64,
                    })
                    .unwrap(),
                ),
            },
            signer,
        )
        .unwrap();
        previous[writer] = Some(op.id);
        ops.push(op);
    }
    (topic, ops)
}

#[test]
#[ignore = "paired release measurement, run explicitly"]
fn activation_costs() {
    let persist_mode = match std::env::var("IROKLE_BENCH_PERSIST").as_deref() {
        Ok("buffer") => fjall::PersistMode::Buffer,
        Ok("sync_all") | Err(std::env::VarError::NotPresent) => fjall::PersistMode::SyncAll,
        _ => panic!("invalid measurement persist mode"),
    };
    let actors = std::env::var("ACTIVATION_ACTORS").unwrap().parse().unwrap();
    let count = std::env::var("ACTIVATION_OPS").unwrap().parse().unwrap();
    let (topic, ops) = fixture(actors, count);
    let encoded = postcard::to_allocvec(&ops).unwrap();
    let fixture_hash = blake3::hash(&encoded);
    let reference = Oplog::with_storage(MemoryStorage::new());
    for batch in ops.chunks(4096) {
        reference.receive_ops(batch.to_vec()).unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .open()
        .unwrap();
    let storage = FjallStorage::from_database_with_persist_mode(db.clone(), persist_mode).unwrap();
    let session = storage
        .open_provisional(ops[0].signed.body.author, topic, ops[0].id, 1000)
        .unwrap();
    let view = storage.provisional_store(&session).unwrap().unwrap();
    let staged = Oplog::with_storage(view.clone());
    for batch in ops.chunks(4096) {
        staged.receive_ops(batch.to_vec()).unwrap();
    }
    let state = view.topic_state(&topic).unwrap().unwrap();
    let slot = db
        .keyspace("bootstrap-0", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let prefix = [b"cn".as_slice(), topic.as_ref()].concat();
    let nodes = fjall::Readable::prefix(&db.read_tx(), &slot, prefix).count();
    let before = storage.counters();
    let mut profile = std::env::var_os("ACTIVATION_PROFILE").map(|path| {
        let mut stream = std::os::unix::net::UnixStream::connect(path).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(60)))
            .unwrap();
        stream.write_all(b"R").unwrap();
        let mut reply = [0];
        stream.read_exact(&mut reply).unwrap();
        assert_eq!(reply, *b"S");
        stream
    });
    let started = std::time::Instant::now();
    storage
        .activate_provisional(&session, &state, AdmissionEffects::default())
        .unwrap();
    let elapsed = started.elapsed();
    if let Some(stream) = &mut profile {
        stream.write_all(b"D").unwrap();
        let mut reply = [0];
        stream.read_exact(&mut reply).unwrap();
        assert_eq!(reply, *b"E");
    }
    let attempts = storage.counters().transaction_attempts - before.transaction_attempts;
    assert_eq!(
        storage.actor_clock(&topic).unwrap(),
        reference.storage().actor_clock(&topic).unwrap()
    );
    assert_eq!(
        storage.heads(&topic).unwrap(),
        reference.storage().heads(&topic).unwrap()
    );
    assert_eq!(
        storage.list_op_ids(&topic).unwrap(),
        reference.storage().list_op_ids(&topic).unwrap()
    );
    for op in &ops {
        assert_eq!(
            storage.get_meta(&op.id).unwrap(),
            reference.storage().get_meta(&op.id).unwrap()
        );
    }
    println!(
        "activation actors={actors} ops={count} fixture_blake3={fixture_hash} fixture_bytes={} node_records={nodes} elapsed_ns={} transaction_attempts={attempts} durability={persist_mode:?}",
        encoded.len(),
        elapsed.as_nanos()
    );
}
