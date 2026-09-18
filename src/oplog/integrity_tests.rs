// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeSet;

use super::*;
use crate::Op;
use crate::storage::TopicView;

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
