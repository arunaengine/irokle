// SPDX-License-Identifier: MIT OR Apache-2.0
//! Requests that name only part of the actors a requester is behind on: the one
//! bounded builder of their ranges, what a responder may take from them, and
//! what a requester carries from one page result to its next request.

use std::collections::BTreeSet;

use crate::{ActorClock, ActorId};

use super::{
    ActorFilter, ActorRangeHint, ActorWindow, MAX_ACTOR_FILTER_BYTES, MAX_ACTOR_RANGE_HINT_SPAN,
};

/// What a requester carries between requests for one peer, topic and branch:
/// where the next actor window starts and the actors whose positions the last
/// page needed. A new branch or staging session starts from the default.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RequestKnowledge {
    after: Option<ActorId>,
    positions: BTreeSet<ActorId>,
    /// Grows when a page result names positions not asked for before.
    revision: u64,
}

impl RequestKnowledge {
    /// Fold the result of a page served for a request with `window`. New
    /// positions keep the window for the next request, which names them first;
    /// otherwise the next window starts after this one, so positions no
    /// request could satisfy do not hold every other actor back.
    #[cfg(any(feature = "iroh", test))]
    pub(crate) fn settle(&mut self, window: &ActorWindow, positions: &BTreeSet<ActorId>) {
        let known = self.positions.len();
        self.positions.extend(positions.iter().copied());
        while self.positions.len() > super::MAX_PAGE_MISSING {
            self.positions.pop_last();
        }
        if self.positions.len() > known {
            self.after = window.after;
            self.revision += 1;
            return;
        }
        self.after = window.through;
        if positions.is_empty() {
            self.positions.clear();
        }
    }

    /// How many positions the next request names before anything else.
    pub(crate) fn positions(&self) -> usize {
        self.positions.len()
    }

    /// How often page results named new positions, so a page that only asked
    /// for them still counts as progress.
    #[cfg(any(feature = "iroh", test))]
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }
}

/// What a request says about the requester's positions: a named actor is at
/// its hint, an actor the window holds is held up to the responder's clock,
/// and any other actor is unknown.
pub(crate) struct ActorScope<'a> {
    named: BTreeSet<ActorId>,
    window: &'a ActorWindow,
}

impl<'a> ActorScope<'a> {
    pub(crate) fn new(hints: &[ActorRangeHint], window: &'a ActorWindow) -> Self {
        Self {
            named: hints.iter().map(|hint| hint.actor_id).collect(),
            window,
        }
    }

    /// A scope that knows every actor, as a summary's full clock does.
    pub(crate) fn whole() -> ActorScope<'static> {
        const WHOLE: ActorWindow = ActorWindow {
            after: None,
            through: None,
            behind: None,
        };
        ActorScope {
            named: BTreeSet::new(),
            window: &WHOLE,
        }
    }

    pub(crate) fn unknown(&self, actor_id: &ActorId) -> bool {
        !self.named.contains(actor_id) && !self.window.holds(actor_id)
    }
}

/// The ranges one request names in at most `items` hints and the window they
/// describe. When every actor `remote` is ahead of `local` on fits, all are
/// named and the window holds every actor. Otherwise the request first names
/// the positions `knowledge` carries, then a run of actors behind in id order
/// after its cursor; the window covers that run and filters the actors behind
/// it leaves out. Spans share [`MAX_ACTOR_RANGE_HINT_SPAN`]; an actor past it
/// gets a zero-span hint.
pub(crate) fn request_ranges(
    local: &ActorClock,
    remote: &ActorClock,
    items: usize,
    knowledge: &RequestKnowledge,
) -> (Vec<ActorRangeHint>, ActorWindow) {
    let behind = remote
        .iter()
        .filter(|(actor_id, seq)| **seq > local.get(actor_id))
        .map(|(actor_id, _)| *actor_id)
        .collect::<Vec<_>>();
    let mut span = MAX_ACTOR_RANGE_HINT_SPAN;
    let mut hint = |actor_id: ActorId| {
        let from = local.get(&actor_id);
        let to = remote
            .get(&actor_id)
            .saturating_sub(from)
            .min(span)
            .saturating_add(from);
        span -= to - from;
        ActorRangeHint {
            actor_id,
            from_exclusive: from,
            to_inclusive: to,
        }
    };
    if behind.len() <= items {
        let hints = behind.into_iter().map(&mut hint).collect();
        return (hints, ActorWindow::default());
    }
    if items == 0 {
        // A window from an actor to itself holds nothing.
        let empty = Some(behind[0]);
        let window = ActorWindow {
            after: empty,
            through: empty,
            behind: ActorFilter::new(&behind, MAX_ACTOR_FILTER_BYTES),
        };
        return (Vec::new(), window);
    }
    let named = knowledge
        .positions
        .iter()
        .take(items - 1)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut hints = named
        .iter()
        .map(|actor_id| hint(*actor_id))
        .collect::<Vec<_>>();
    let mut after = knowledge.after;
    let mut start = behind.partition_point(|actor_id| Some(*actor_id) <= after);
    if start == behind.len() {
        (after, start) = (None, 0);
    }
    let mut through = after;
    let mut end = start;
    for actor_id in &behind[start..] {
        if !named.contains(actor_id) {
            if hints.len() == items {
                break;
            }
            hints.push(hint(*actor_id));
        }
        through = Some(*actor_id);
        end += 1;
    }
    // A run that reached the last actor behind holds everything after it too.
    if end == behind.len() {
        through = None;
    }
    let mut window = ActorWindow {
        after,
        through,
        behind: None,
    };
    let omitted = behind
        .iter()
        .filter(|actor_id| !named.contains(*actor_id) && !window.contains(actor_id))
        .copied()
        .collect::<Vec<_>>();
    window.behind = ActorFilter::new(&omitted, MAX_ACTOR_FILTER_BYTES);
    (hints, window)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(byte: u8) -> ActorId {
        ActorId::from_bytes([byte; 32])
    }

    fn clock(entries: &[(u8, u64)]) -> ActorClock {
        let mut clock = ActorClock::new();
        for (byte, seq) in entries {
            clock.observe(actor(*byte), *seq);
        }
        clock
    }

    /// Every actor behind inside the window is named, and a named actor's
    /// hint starts at the requester's own position.
    fn assert_described(
        local: &ActorClock,
        remote: &ActorClock,
        (hints, window): &(Vec<ActorRangeHint>, ActorWindow),
    ) {
        let named = hints
            .iter()
            .map(|hint| (hint.actor_id, hint.from_exclusive))
            .collect::<std::collections::BTreeMap<_, _>>();
        for (actor_id, seq) in remote.iter() {
            if *seq > local.get(actor_id) && window.contains(actor_id) {
                assert!(named.contains_key(actor_id), "{actor_id} omitted");
            }
        }
        for (actor_id, from) in named {
            assert_eq!(from, local.get(&actor_id));
        }
    }

    /// Up to the item limit every actor behind is named and the window holds
    /// every actor; one past it names a run whose window leaves the rest unknown.
    #[test]
    fn ranges_fit_items() {
        let local = clock(&[(1, 1), (9, 5)]);
        let remote = clock(&[(1, 3), (2, 2), (3, 1), (9, 5)]);
        for items in [3, 4] {
            let ranges = request_ranges(&local, &remote, items, &RequestKnowledge::default());
            assert_eq!(ranges.0.len(), 3);
            assert_eq!(ranges.1, ActorWindow::default());
            assert_described(&local, &remote, &ranges);
        }
        let ranges = request_ranges(&local, &remote, 2, &RequestKnowledge::default());
        assert_eq!(ranges.0.len(), 2);
        assert_eq!((ranges.1.after, ranges.1.through), (None, Some(actor(2))));
        // The actor behind outside the window is filtered, held actors are not.
        let behind = ranges.1.behind.as_ref().unwrap();
        assert!(behind.contains(&actor(3)));
        assert!(ranges.1.holds(&actor(9)));
        assert!(!ranges.1.contains(&actor(3)));
        assert_described(&local, &remote, &ranges);
        let empty = request_ranges(&local, &remote, 0, &RequestKnowledge::default());
        assert!(empty.0.is_empty());
        assert!(
            remote
                .iter()
                .all(|(actor_id, _)| !empty.1.contains(actor_id))
        );
        let (none, window) = request_ranges(&remote, &remote, 1, &RequestKnowledge::default());
        assert!(none.is_empty());
        assert_eq!(window, ActorWindow::default());
    }

    /// A page naming positions keeps the window and the next request names
    /// them first, held or behind; a page naming none moves the window on and
    /// wraps after the last actor.
    #[test]
    fn knowledge_moves_window() {
        let local = clock(&[(9, 4)]);
        let remote = clock(&[(1, 1), (2, 1), (3, 1), (4, 1), (9, 4)]);
        let mut knowledge = RequestKnowledge::default();
        let first = request_ranges(&local, &remote, 2, &knowledge);
        assert_eq!(first.1.through, Some(actor(2)));
        knowledge.settle(&first.1, &[actor(9), actor(4)].into());
        assert_eq!(knowledge.revision(), 1);
        let asked = request_ranges(&local, &remote, 2, &knowledge);
        assert_described(&local, &remote, &asked);
        assert_eq!(asked.0[0].actor_id, actor(4));
        assert_eq!(asked.1.after, None);
        knowledge.settle(&asked.1, &[actor(4)].into());
        assert_eq!(
            knowledge.revision(),
            1,
            "a repeated position is no progress"
        );
        let named = request_ranges(&local, &remote, 3, &knowledge);
        let zero = named
            .0
            .iter()
            .find(|hint| hint.actor_id == actor(9))
            .unwrap();
        assert_eq!((zero.from_exclusive, zero.to_inclusive), (4, 4));
        knowledge.settle(&named.1, &BTreeSet::new());
        let next = request_ranges(&local, &remote, 2, &knowledge);
        assert_described(&local, &remote, &next);
        assert_eq!(next.1.after, named.1.through);
        knowledge.settle(&next.1, &BTreeSet::new());
        let wrapped = request_ranges(&local, &remote, 2, &knowledge);
        assert_eq!(wrapped.1, first.1, "after the last actor the window wraps");
    }

    /// A request at every real limit, hints and wants at their largest
    /// encoding and a full filter, fits one sync frame.
    #[test]
    fn largest_request_frames() {
        let items = super::super::MAX_REQUEST_ITEMS;
        let hints = (0..items as u32 / 2)
            .map(|index| {
                let mut bytes = [0xff_u8; 32];
                bytes[..4].copy_from_slice(&index.to_be_bytes());
                ActorRangeHint {
                    actor_id: ActorId::from_bytes(bytes),
                    from_exclusive: u64::MAX - 1,
                    to_inclusive: u64::MAX,
                }
            })
            .collect();
        let wants = (0..items as u32 / 2)
            .map(|index| crate::OpId::hash(index.to_be_bytes()))
            .collect();
        let request = crate::sync::SyncMessage::Request(crate::sync::SyncRequest {
            topic_id: crate::TopicId::hash(b"largest"),
            known: BTreeSet::new(),
            wants,
            actor_range_hints: hints,
            genesis: Some(crate::OpId::hash(b"genesis")),
            credit: Default::default(),
            window: ActorWindow {
                after: Some(ActorId::from_bytes([0; 32])),
                through: Some(ActorId::from_bytes([0xff; 32])),
                behind: Some(ActorFilter {
                    bits: vec![0xff; MAX_ACTOR_FILTER_BYTES],
                }),
            },
        });
        let framed = crate::net::framed_message_len(&request).unwrap();
        assert!(framed <= 16 * 1024 * 1024 + 4, "{framed} bytes");
    }

    /// A filter never misses an actor it holds, and at ten bits an actor
    /// seldom collides.
    #[test]
    fn filter_never_misses() {
        let id =
            |index: u32| crate::ActorId::from_bytes(*blake3::hash(&index.to_le_bytes()).as_bytes());
        let held = (0..4096).map(id).collect::<Vec<_>>();
        let filter = ActorFilter::new(&held, MAX_ACTOR_FILTER_BYTES).unwrap();
        assert!(held.iter().all(|actor_id| filter.contains(actor_id)));
        let collisions = (4096..8192)
            .filter(|index| filter.contains(&id(*index)))
            .count();
        assert!(collisions < 4096 / 20, "{collisions} collisions");
        assert!(ActorFilter::new(&held, 64).is_none());
        assert!(!ActorFilter::default().contains(&held[0]));
    }

    /// At the real item limit the builder names no more than it may, whatever
    /// the number of actors behind.
    #[test]
    fn ranges_real_limit() {
        let items = super::super::MAX_REQUEST_ITEMS;
        let mut remote = ActorClock::new();
        for index in 0..(items as u32 + 5) {
            let mut bytes = [0_u8; 32];
            bytes[..4].copy_from_slice(&index.to_be_bytes());
            remote.observe(ActorId::from_bytes(bytes), 2);
        }
        let local = ActorClock::new();
        let (hints, window) =
            request_ranges(&local, &remote, items - 7, &RequestKnowledge::default());
        assert_eq!(hints.len(), items - 7);
        assert!(window.through.is_some());
        assert_described(&local, &remote, &(hints, window));
    }
}
