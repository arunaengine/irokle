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
}

impl RequestKnowledge {
    /// How many positions the next request names before anything else.
    pub(crate) fn positions(&self) -> usize {
        self.positions.len()
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
