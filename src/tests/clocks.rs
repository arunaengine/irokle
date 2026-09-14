//! Observed clocks stored by Fjall as shared trie nodes equal the clocks the
//! Memory store computes for the same graph: forks, joins, ops that waited for
//! their dependencies, a cold reopen, and a reset that removes the nodes.

use super::support::*;

use crate::oplog::Oplog;
use crate::storage::FjallStorage;

/// A seeded generator, so every run builds the same graph.
struct Draw(u64);

impl Draw {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

/// A genesis and `count` events of six writers, each depending on its own
/// previous op and on up to three random earlier ops.
fn random_graph(count: usize) -> (TopicId, Vec<Op>) {
    let owner = Ed25519Signer::from_bytes(&[236; 32]);
    let writers = (0..6)
        .map(|index| Ed25519Signer::from_bytes(&[237 + index; 32]))
        .collect::<Vec<_>>();
    let topic_id = TopicId::hash(b"random-clock-graph");
    let members = writers
        .iter()
        .chain([&owner])
        .map(Signer::peer_id)
        .collect::<BTreeSet<_>>();
    let genesis = Op::sign(
        OpBody {
            topic_id,
            author: owner.peer_id(),
            actor_id: actor_id_for(topic_id, owner.peer_id()),
            actor_seq: 1,
            actor_prev: None,
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis::new(Note::TYPE_ID, members)),
        },
        &owner,
    )
    .unwrap();
    let mut draw = Draw(0x2545_f491_4f6c_dd1d);
    let mut ops = vec![genesis];
    let mut last = [None::<usize>; 6];
    for _ in 0..count {
        let writer = draw.below(6);
        let mut deps = BTreeSet::new();
        for _ in 0..1 + draw.below(3) {
            deps.insert(draw.below(ops.len()));
        }
        deps.extend(last[writer]);
        let generation = deps
            .iter()
            .map(|index| ops[*index].signed.body.generation + 1)
            .max()
            .unwrap();
        let signer = &writers[writer];
        let op = Op::sign(
            OpBody {
                topic_id,
                author: signer.peer_id(),
                actor_id: actor_id_for(topic_id, signer.peer_id()),
                actor_seq: last[writer].map_or(1, |index| ops[index].signed.body.actor_seq + 1),
                actor_prev: last[writer].map(|index| ops[index].id),
                deps: deps.iter().map(|index| ops[*index].id).collect(),
                generation,
                payload: TopicPayload::Event(
                    EventEnvelope::encode_event(&Note { text: "x".into() }).unwrap(),
                ),
            },
            signer,
        )
        .unwrap();
        last[writer] = Some(ops.len());
        ops.push(op);
    }
    (topic_id, ops)
}

fn assert_same_metas<S: Storage>(expected: &MemoryStorage, actual: &S, ops: &[Op]) {
    for op in ops {
        let meta = actual.get_meta(&op.id).unwrap();
        assert_eq!(meta, expected.get_meta(&op.id).unwrap(), "{}", op.id);
        let position = actual.get_position(&op.id).unwrap();
        assert_eq!(position, meta.as_ref().map(Into::into));
    }
}

/// Every stored clock of a random graph admitted out of order, in small
/// batches, equals the Memory store's, before and after a cold reopen; a
/// reset leaves no clock node of the topic and the topic admits again.
#[test]
fn fjall_clocks_match() {
    let (topic_id, ops) = random_graph(240);
    let memory = MemoryStorage::new();
    let dir = tempfile::tempdir().unwrap();
    let fjall = FjallStorage::open(dir.path()).unwrap();
    // Reversed batches wait for their dependencies before they admit.
    let mut draw = Draw(7);
    let mut batches = Vec::new();
    let mut rest = &ops[1..];
    while !rest.is_empty() {
        let (batch, tail) = rest.split_at((1 + draw.below(8)).min(rest.len()));
        batches.push(batch.to_vec());
        rest = tail;
    }
    for (index, batch) in batches.iter().enumerate() {
        let batch = if index % 3 == 1 {
            batch.iter().rev().cloned().collect()
        } else {
            batch.clone()
        };
        for storage_log in [
            &Oplog::with_storage(memory.clone()) as &dyn Receive,
            &Oplog::with_storage(fjall.clone()),
        ] {
            if index == 0 {
                storage_log.receive(vec![ops[0].clone()]);
            }
            storage_log.receive(batch.clone());
        }
    }
    assert_eq!(memory.list_op_ids(&topic_id).unwrap().len(), ops.len());
    assert_same_metas(&memory, &fjall, &ops);
    drop(fjall);

    let fjall = FjallStorage::open(dir.path()).unwrap();
    assert_same_metas(&memory, &fjall, &ops);
    assert_eq!(fjall.reset_topic(&topic_id).unwrap(), ops.len());
    assert!(fjall.get_meta(&ops[1].id).unwrap().is_none());
    drop(fjall);
    let db = fjall::OptimisticTxDatabase::builder(dir.path())
        .open()
        .unwrap();
    let records = db
        .keyspace("records", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let prefix = [b"cn".as_slice(), topic_id.as_ref()].concat();
    assert_eq!(
        fjall::Readable::prefix(&db.read_tx(), &records, prefix).count(),
        0
    );
    drop((records, db));

    let fjall = FjallStorage::open(dir.path()).unwrap();
    Oplog::with_storage(fjall.clone())
        .receive_ops(ops.clone())
        .unwrap();
    assert_same_metas(&memory, &fjall, &ops);
}

/// Receiving a batch through an oplog of any backend.
trait Receive {
    fn receive(&self, ops: Vec<Op>);
}

impl<S: Storage> Receive for Oplog<S> {
    fn receive(&self, ops: Vec<Op>) {
        self.receive_ops(ops).unwrap();
    }
}
