// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeSet;

use super::*;
use crate::MemoryStorage;
use crate::storage::{CounterSnapshot, Storage};
use crate::sync::SyncData;
use crate::tests::support::{Corrupt, Damage, Irokle, Note, TopicConfig, damage_op, node};
use crate::{Op, oplog::Oplog};

/// A topic of `events` notes whose first `stored` ops `storage` holds, with the
/// oplog that admitted them and every op of the source.
fn seeded<S: Storage>(storage: &S, events: usize, stored: usize) -> (Oplog<S>, TopicId, Vec<Op>) {
    let source = node(71);
    let topic = source.create_topic::<Note>(TopicConfig::default()).unwrap();
    for index in 0..events {
        let text = index.to_string();
        topic.publish(Note { text }).unwrap();
    }
    let ops = crate::oplog::topological(source.storage(), &topic.id()).unwrap();
    let log = Oplog::with_storage(storage.clone());
    log.receive_ops(ops[..stored].to_vec()).unwrap();
    (log, topic.id(), ops)
}

/// One question to `log` about `topic_id` in a fresh snapshot.
fn ask<S: Storage>(log: &Oplog<S>, topic_id: &TopicId) -> Integrity {
    log.storage()
        .read_snapshot(|read| {
            let view = read.topic_view(topic_id, None)?.unwrap();
            log.integrity_in(read, &view)
        })
        .unwrap()
}

/// Asks until a scan is complete. Returns the verdict, the steps taken and the
/// most positions one step read.
fn finish<S: Storage>(
    log: &Oplog<S>,
    topic_id: &TopicId,
    counters: fn(&S) -> CounterSnapshot,
) -> (Integrity, usize, u64) {
    let mut steps = 0;
    let mut widest = 0;
    loop {
        let before = counters(log.storage());
        let integrity = ask(log, topic_id);
        let after = counters(log.storage());
        assert_eq!(after.op_reads, before.op_reads, "a step decoded payloads");
        widest = widest.max(after.meta_reads - before.meta_reads);
        steps += 1;
        assert!(steps <= 1000, "the scan never completed");
        if integrity.is_complete() {
            return (integrity, steps, widest);
        }
    }
}

fn positions<S>(storage: &S, counters: fn(&S) -> CounterSnapshot) -> u64 {
    counters(storage).meta_reads
}

/// A cold scan in steps of eight reads: each step reads a bounded share, the
/// steps together read each position about once, and the verdict names both
/// kinds of loss. Asking again reads nothing, and admission heals the verdict.
fn assert_steps_resume<S: Corrupt>(storage: S, counters: fn(&S) -> CounterSnapshot) {
    let (log, topic_id, ops) = seeded(&storage, 40, 41);
    damage_op(&storage, &ops[10].id, Damage::Op);
    damage_op(&storage, &ops[20].id, Damage::Meta);
    log.set_step_reads(8);
    let start = positions(&storage, counters);
    let (integrity, steps, widest) = finish(&log, &topic_id, counters);
    let total = positions(&storage, counters) - start;
    assert!(steps >= 41 / 8, "{steps} steps");
    // A header and one dependency read per id, and one resumed read per step.
    assert!(widest <= 2 * 8 + 1, "{widest} positions in one step");
    assert!(
        total <= 2 * 41 + steps as u64,
        "{total} positions in {steps} steps"
    );
    let generation = ops[10].signed.body.generation;
    let expected = Holes::from([(ops[10].id, Some(generation)), (ops[20].id, None)]);
    assert_eq!(integrity, Integrity::Incomplete(expected));

    let again = positions(&storage, counters);
    assert!(matches!(ask(&log, &topic_id), Integrity::Incomplete(_)));
    assert_eq!(positions(&storage, counters), again);

    // A repair fills one hole; a copy another handle stored fills the other
    // once it is received again, without any recheck.
    log.receive_ops(vec![ops[10].clone()]).unwrap();
    let expected = Holes::from([(ops[20].id, None)]);
    assert_eq!(ask(&log, &topic_id), Integrity::Incomplete(expected));
    Oplog::with_storage(storage.clone())
        .receive_ops(vec![ops[20].clone()])
        .unwrap();
    assert!(matches!(ask(&log, &topic_id), Integrity::Incomplete(_)));
    log.receive_ops(vec![ops[20].clone()]).unwrap();
    assert_eq!(ask(&log, &topic_id), Integrity::Whole);
    assert!(log.topic_unresolved(&topic_id).unwrap().is_empty());
}

#[test]
fn memory_steps_resume() {
    assert_steps_resume(MemoryStorage::new(), MemoryStorage::counters);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_steps_resume() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    assert_steps_resume(storage, crate::storage::FjallStorage::counters);
}

/// A topic whose newest op depends on `width` concurrent ops of as many members.
fn wide_topic<S: Corrupt>(storage: &S, width: u8) -> (Oplog<S>, TopicId, Vec<Op>, Op) {
    let source = node(81);
    let members = (0..width).map(|seed| node(100 + seed)).collect::<Vec<_>>();
    let topic = source
        .create_topic::<Note>(TopicConfig {
            initial_peers: members.iter().map(Irokle::peer_id).collect(),
            ..TopicConfig::default()
        })
        .unwrap();
    let topic_id = topic.id();
    let genesis = crate::oplog::topological(source.storage(), &topic_id).unwrap();
    let mut concurrent = Vec::new();
    for member in &members {
        let data = SyncData {
            topic_id,
            ops: genesis.clone(),
        };
        member
            .receive_sync_data_from(source.peer_id(), data)
            .unwrap();
        let text = "side".to_string();
        let record = member.open_topic::<Note>(topic_id).unwrap();
        let op_id = record.publish(Note { text }).unwrap().meta.op_id;
        let op = member.storage().get_op(&op_id).unwrap().unwrap();
        let data = SyncData {
            topic_id,
            ops: vec![op.clone()],
        };
        source
            .receive_sync_data_from(member.peer_id(), data)
            .unwrap();
        concurrent.push(op);
    }
    let text = "merge".to_string();
    let op_id = topic.publish(Note { text }).unwrap().meta.op_id;
    let wide = source.storage().get_op(&op_id).unwrap().unwrap();
    assert!(wide.signed.body.deps.len() >= width as usize);
    let log = Oplog::with_storage(storage.clone());
    let ops = crate::oplog::topological(source.storage(), &topic_id).unwrap();
    log.receive_ops(ops).unwrap();
    (log, topic_id, concurrent, wide)
}

/// Steps of four reads end inside the dependencies of an op that has ten, and
/// the next step goes on from that offset. A lost dependency is still found.
fn assert_wide_resumes<S: Corrupt>(storage: S) {
    let (_, topic_id, concurrent, wide) = wide_topic(&storage, 10);
    damage_op(&storage, &concurrent[3].id, Damage::Both);
    let mut cursor = Cursor::default();
    let mut holes = Holes::new();
    let mut inside = 0;
    for _ in 0..100 {
        let step = storage
            .read_snapshot(|read| scan_step(read, &topic_id, cursor, 4))
            .unwrap();
        holes.extend(step.holes);
        if let Some((id, from)) = step.cursor.open
            && id == wide.id
        {
            inside += usize::from(from.offset > 0);
        }
        if step.done {
            assert!(inside > 0, "no step resumed inside the dependencies");
            assert_eq!(holes, Holes::from([(concurrent[3].id, None)]));
            return;
        }
        cursor = step.cursor;
    }
    panic!("the scan never completed");
}

#[test]
fn memory_wide_resumes() {
    assert_wide_resumes(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_wide_resumes() {
    let dir = tempfile::tempdir().unwrap();
    assert_wide_resumes(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// A buffered op waiting on a missing dependency keeps the topic uncertified
/// although its scan is whole, until the dependency arrives.
fn assert_pending_counts<S: Corrupt>(storage: S) {
    let (log, topic_id, ops) = seeded(&storage, 6, 4);
    log.receive_ops(vec![ops[5].clone()]).unwrap();
    let (view, integrity) = log.inspect(&topic_id).unwrap().unwrap();
    assert_eq!(integrity, Integrity::Whole);
    assert!(!integrity.certifies(&view));
    let missing = BTreeSet::from([ops[4].id]);
    assert_eq!(log.topic_unresolved(&topic_id).unwrap(), missing);
    assert!(!log.whole_view(&topic_id).unwrap().unwrap().1);
    log.receive_ops(vec![ops[4].clone()]).unwrap();
    assert!(log.topic_unresolved(&topic_id).unwrap().is_empty());
    assert!(log.whole_view(&topic_id).unwrap().unwrap().1);
}

#[test]
fn memory_pending_counts() {
    assert_pending_counts(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_pending_counts() {
    let dir = tempfile::tempdir().unwrap();
    assert_pending_counts(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// Ops admitted while a scan is under way do not restart it: it completes
/// after reading about as many positions as the topic holds at the end.
fn assert_appends_continue<S: Corrupt>(storage: S, counters: fn(&S) -> CounterSnapshot) {
    let (log, topic_id, ops) = seeded(&storage, 60, 41);
    log.set_step_reads(8);
    let start = positions(&storage, counters);
    assert!(matches!(ask(&log, &topic_id), Integrity::Scanning(_)));
    let first = positions(&storage, counters) - start;
    log.receive_ops(ops[41..].to_vec()).unwrap();
    let resumed = positions(&storage, counters);
    let (integrity, steps, _) = finish(&log, &topic_id, counters);
    assert_eq!(integrity, Integrity::Whole);
    // Admission reads positions too; only the scan's own reads are counted.
    let total = first + positions(&storage, counters) - resumed;
    assert!(total <= 2 * 61 + steps as u64 + 1, "{total} positions read");
}

#[test]
fn memory_appends_continue() {
    assert_appends_continue(MemoryStorage::new(), MemoryStorage::counters);
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_appends_continue() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    assert_appends_continue(storage, crate::storage::FjallStorage::counters);
}

/// A reset by another handle between steps changes the data epoch, so the next
/// step starts the scan over under the new epoch instead of resuming the old one.
fn assert_reset_restarts<S: Corrupt>(storage: S) {
    let (log, topic_id, ops) = seeded(&storage, 41, 41);
    log.set_step_reads(8);
    assert!(matches!(ask(&log, &topic_id), Integrity::Scanning(_)));
    let epoch = || storage.topic_view(&topic_id, None).unwrap().unwrap().epoch;
    let before = epoch();
    // The newest op, stored beside the chain but reached by no head, which
    // quarantine removes with a reset.
    let full = MemoryStorage::new();
    Oplog::with_storage(full.clone())
        .receive_ops(ops.clone())
        .unwrap();
    let orphan = ops.last().unwrap();
    storage.orphan_op(orphan, &full.get_meta(&orphan.id).unwrap().unwrap());
    let other = Oplog::with_storage(storage.clone());
    assert!(other.quarantine_orphans(&topic_id).unwrap().is_some());
    assert!(epoch() > before);

    assert!(matches!(ask(&log, &topic_id), Integrity::Scanning(_)));
    assert_eq!(log.integrity.topics().unwrap()[&topic_id].key.1, epoch());
    assert_eq!(log.inspect(&topic_id).unwrap().unwrap().1, Integrity::Whole);
}

#[test]
fn memory_reset_restarts() {
    assert_reset_restarts(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_reset_restarts() {
    let dir = tempfile::tempdir().unwrap();
    assert_reset_restarts(crate::storage::FjallStorage::open(dir.path()).unwrap());
}

/// The saved place of `topic_id`'s scan, which no step may hold now.
#[cfg(feature = "fjall")]
fn saved<S: Storage>(log: &Oplog<S>, topic_id: &TopicId) -> Cursor {
    match &log.integrity.topics().unwrap()[topic_id].state {
        State::Scanning {
            cursor,
            stepping: None,
            ..
        } => *cursor,
        State::Scanning { .. } => panic!("a step still holds the scan"),
        State::Incomplete(_) | State::Whole => panic!("the scan already ended"),
    }
}

/// Reads of a snapshot that panic at a header read once `left` reads passed.
#[cfg(feature = "fjall")]
struct Panicking<'a> {
    read: &'a dyn SnapshotRead,
    left: std::cell::Cell<usize>,
}

#[cfg(feature = "fjall")]
impl SnapshotRead for Panicking<'_> {
    fn topic_view(
        &self,
        topic_id: &TopicId,
        peer_id: Option<&crate::PeerId>,
    ) -> Result<Option<TopicView>> {
        self.read.topic_view(topic_id, peer_id)
    }
    fn get_op(&self, id: &OpId) -> Result<Option<Op>> {
        self.read.get_op(id)
    }
    fn get_meta(&self, id: &OpId) -> Result<Option<crate::storage::OpMeta>> {
        self.read.get_meta(id)
    }
    fn get_header(&self, id: &OpId) -> Result<Option<crate::storage::OpHeader>> {
        let left = self.left.get();
        assert!(left > 0, "injected panic inside a scan step");
        self.left.set(left - 1);
        self.read.get_header(id)
    }
    fn dependency_ids(
        &self,
        id: &OpId,
        cursor: DependencyCursor,
        limit: usize,
    ) -> Result<Option<Vec<OpId>>> {
        self.read.dependency_ids(id, cursor, limit)
    }
    fn dep_resolvable(&self, id: &OpId) -> Result<bool> {
        self.read.dep_resolvable(id)
    }
    fn actor_range(
        &self,
        topic_id: &TopicId,
        actor_id: &crate::ActorId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(u64, OpId)>> {
        self.read.actor_range(topic_id, actor_id, after, limit)
    }
    fn list_op_ids(&self, topic_id: &TopicId) -> Result<BTreeSet<OpId>> {
        self.read.list_op_ids(topic_id)
    }
    fn topic_ids_after(
        &self,
        topic_id: &TopicId,
        after: Option<&OpId>,
        limit: usize,
    ) -> Result<Vec<OpId>> {
        self.read.topic_ids_after(topic_id, after, limit)
    }
}

/// A step that panics ends its claim, and the scan goes on from the place the
/// last finished step saved. Fjall snapshots hold no lock a panic could poison.
#[cfg(feature = "fjall")]
#[test]
fn panic_releases_claim() {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    let (log, topic_id, _) = seeded(&storage, 40, 41);
    log.set_step_reads(8);
    assert!(matches!(ask(&log, &topic_id), Integrity::Scanning(_)));
    let place = saved(&log, &topic_id);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        log.storage().read_snapshot(|read| {
            let view = read.topic_view(&topic_id, None)?.unwrap();
            let left = std::cell::Cell::new(3);
            log.integrity_in(&Panicking { read, left }, &view)
        })
    }));
    assert!(panicked.is_err());
    assert_eq!(saved(&log, &topic_id), place);
    let counters = crate::storage::FjallStorage::counters;
    assert_eq!(finish(&log, &topic_id, counters).0, Integrity::Whole);
}

/// While one step is paused inside its snapshot, another question neither waits
/// for it nor reads the claimed scan; the scan resumes once the step ends.
#[cfg(feature = "fjall")]
#[test]
fn blocked_step_owns() {
    use crate::tests::support::{Gate, GatePoint, StaleReadStorage};
    let dir = tempfile::tempdir().unwrap();
    let storage = StaleReadStorage::new(crate::storage::FjallStorage::open(dir.path()).unwrap());
    let (log, topic_id, ops) = seeded(&storage, 40, 41);
    log.set_step_reads(8);
    let first = ops.iter().map(|op| op.id).min().unwrap();
    let gate = std::sync::Arc::new(Gate::default());
    let release = gate.releaser();
    storage.arm_read(GatePoint::Meta(first), std::sync::Arc::clone(&gate));
    let blocked = std::thread::spawn({
        let log = log.clone();
        move || ask(&log, &topic_id)
    });
    gate.wait_arrival();
    let held = matches!(
        log.integrity.topics().unwrap()[&topic_id].state,
        State::Scanning {
            stepping: Some(_),
            ..
        }
    );
    assert!(held, "the paused step holds no claim");
    assert_eq!(ask(&log, &topic_id), Integrity::Unknown);
    drop(release);
    assert!(matches!(blocked.join().unwrap(), Integrity::Scanning(_)));
    assert_eq!(log.inspect(&topic_id).unwrap().unwrap().1, Integrity::Whole);
}

/// A reopened store keeps no cursor or verdict: its scan starts over and finds
/// the loss the interrupted scan had not reached.
#[cfg(feature = "fjall")]
#[test]
fn reopen_scans_again() {
    let dir = tempfile::tempdir().unwrap();
    let (topic_id, ops) = {
        let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
        let (log, topic_id, ops) = seeded(&storage, 40, 41);
        damage_op(&storage, &ops[10].id, Damage::Op);
        log.set_step_reads(8);
        assert!(matches!(ask(&log, &topic_id), Integrity::Scanning(_)));
        (topic_id, ops)
    };
    let storage = crate::storage::FjallStorage::open(dir.path()).unwrap();
    let log = Oplog::with_storage(storage);
    let generation = ops[10].signed.body.generation;
    let expected = Holes::from([(ops[10].id, Some(generation))]);
    let integrity = log.inspect(&topic_id).unwrap().unwrap().1;
    assert_eq!(integrity, Integrity::Incomplete(expected));
}

/// A fixed topic for pure scan tests: each listed id with its generation and
/// dependencies when its position is stored, and the ids stored completely.
struct Fixed {
    listed: BTreeMap<OpId, Option<(u64, Vec<OpId>)>>,
    stored: BTreeSet<OpId>,
}

impl SnapshotRead for Fixed {
    fn topic_view(&self, _: &TopicId, _: Option<&crate::PeerId>) -> Result<Option<TopicView>> {
        unreachable!("a scan step reads no view")
    }
    fn get_op(&self, _: &OpId) -> Result<Option<Op>> {
        unreachable!("a scan step decodes no payload")
    }
    fn get_meta(&self, _: &OpId) -> Result<Option<crate::storage::OpMeta>> {
        unreachable!("a scan step reads headers only")
    }
    fn get_header(&self, id: &OpId) -> Result<Option<crate::storage::OpHeader>> {
        let position = self.listed.get(id).and_then(Option::as_ref);
        Ok(position.map(|(generation, _)| crate::storage::OpHeader {
            topic_id: TopicId::hash(b"fixed"),
            actor_id: crate::ActorId::hash(b"fixed"),
            actor_seq: 1,
            actor_prev: None,
            generation: *generation,
        }))
    }
    fn dependency_ids(
        &self,
        id: &OpId,
        cursor: DependencyCursor,
        limit: usize,
    ) -> Result<Option<Vec<OpId>>> {
        let position = self.listed.get(id).and_then(Option::as_ref);
        Ok(position.map(|(_, deps)| {
            deps.iter()
                .skip(cursor.offset)
                .take(limit)
                .copied()
                .collect()
        }))
    }
    fn dep_resolvable(&self, id: &OpId) -> Result<bool> {
        Ok(self.stored.contains(id))
    }
    fn actor_range(
        &self,
        _: &TopicId,
        _: &crate::ActorId,
        _: u64,
        _: usize,
    ) -> Result<Vec<(u64, OpId)>> {
        unreachable!("a scan step reads no actor index")
    }
    fn list_op_ids(&self, _: &TopicId) -> Result<BTreeSet<OpId>> {
        Ok(self.listed.keys().copied().collect())
    }
}

/// The listing ends within a step whose reads run out before its last id: the
/// step is not the end of the scan, and the next one reads that id.
#[test]
fn tail_ids_scanned() {
    let id = |byte: u8| OpId::from_bytes([byte; 32]);
    let (first, lost, last) = (id(1), id(2), id(3));
    let fixed = Fixed {
        listed: BTreeMap::from([
            (first, Some((2, vec![id(10), id(11)]))),
            (lost, None),
            (last, Some((3, vec![id(10)]))),
        ]),
        stored: BTreeSet::from([first, id(10), id(11)]),
    };
    let topic_id = TopicId::hash(b"fixed");
    let step = scan_step(&fixed, &topic_id, Cursor::default(), 4).unwrap();
    assert!(!step.done, "the step ended before reading every listed id");
    assert_eq!(step.holes, Holes::from([(lost, None)]));
    let step = scan_step(&fixed, &topic_id, step.cursor, 4).unwrap();
    assert!(step.done);
    assert_eq!(step.holes, Holes::from([(last, Some(3))]));
}
