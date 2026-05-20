//! User-facing history and DAG traversal helpers.

use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

/// Ordering used when traversing linearized history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryOrder {
    OldestFirst,
    NewestFirst,
}

impl Default for HistoryOrder {
    fn default() -> Self {
        Self::OldestFirst
    }
}

/// Query options for DAG traversal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DagQuery<I> {
    pub heads: Vec<I>,
    pub order: HistoryOrder,
    pub limit: Option<usize>,
    pub include_heads: bool,
}

impl<I> Default for DagQuery<I> {
    fn default() -> Self {
        Self {
            heads: Vec::new(),
            order: HistoryOrder::OldestFirst,
            limit: None,
            include_heads: true,
        }
    }
}

impl<I> DagQuery<I> {
    pub fn from_heads(heads: impl IntoIterator<Item = I>) -> Self {
        Self {
            heads: heads.into_iter().collect(),
            ..Self::default()
        }
    }

    pub fn newest_first(mut self) -> Self {
        self.order = HistoryOrder::NewestFirst;
        self
    }

    pub fn oldest_first(mut self) -> Self {
        self.order = HistoryOrder::OldestFirst;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn include_heads(mut self, include_heads: bool) -> Self {
        self.include_heads = include_heads;
        self
    }
}

/// A simple Vec-backed stream. It is deliberately just an iterator so callers can
/// use it ergonomically without depending on an async runtime.
#[derive(Clone, Debug)]
pub struct VecStream<T> {
    inner: std::vec::IntoIter<T>,
}

impl<T> VecStream<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self {
            inner: items.into_iter(),
        }
    }

    pub fn collect_vec(self) -> Vec<T> {
        self.inner.collect()
    }
}

impl<T> Iterator for VecStream<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub fn ordered<T>(mut records: Vec<T>, order: HistoryOrder) -> Vec<T> {
    if order == HistoryOrder::NewestFirst {
        records.reverse();
    }
    records
}

pub fn limited<T>(mut records: Vec<T>, limit: Option<usize>) -> Vec<T> {
    if let Some(limit) = limit {
        records.truncate(limit);
    }
    records
}

/// Generic DAG walk over predecessor links supplied by the caller.
pub fn traverse_dag<I, F>(query: DagQuery<I>, mut parents: F) -> Vec<I>
where
    I: Clone + Eq + Hash,
    F: FnMut(&I) -> Vec<I>,
{
    let mut seen = HashSet::new();
    let mut queue: VecDeque<(I, bool)> = query.heads.into_iter().map(|head| (head, true)).collect();
    let mut out = Vec::new();

    while let Some((id, is_head)) = queue.pop_front() {
        if !seen.insert(id.clone()) {
            continue;
        }

        if query.include_heads || !is_head {
            out.push(id.clone());
            if query.limit.is_some_and(|limit| out.len() >= limit) {
                break;
            }
        }

        for parent in parents(&id) {
            queue.push_back((parent, false));
        }
    }

    ordered(out, query.order)
}
