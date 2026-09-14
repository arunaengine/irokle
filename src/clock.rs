// SPDX-License-Identifier: MIT OR Apache-2.0
//! Actor/vector-clock utilities used for admission, reducers, and sync.

use crate::ids::ActorId;
use serde::de::Deserializer;
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Positions per actor. Clones share structure: entries live in a persistent
/// trie over the nibbles of actor ids, so a clock derived from another by a
/// few changes copies only the paths to them. Iteration is in id order and
/// the serialized form is a map of every entry, zero positions included.
#[derive(Clone, Default)]
pub struct ActorClock {
    root: Option<Arc<Node>>,
}

#[derive(Clone)]
enum Node {
    Leaf {
        actor: ActorId,
        seq: u64,
    },
    /// Entries sharing the nibbles of `key` before `level`, one child per
    /// distinct nibble at `level`, in nibble order. At least two children.
    Branch {
        level: u8,
        bitmap: u16,
        len: usize,
        key: ActorId,
        children: Vec<Arc<Node>>,
    },
}

impl Node {
    fn key(&self) -> &ActorId {
        match self {
            Node::Leaf { actor, .. } => actor,
            Node::Branch { key, .. } => key,
        }
    }

    fn len(&self) -> usize {
        match self {
            Node::Leaf { .. } => 1,
            Node::Branch { len, .. } => *len,
        }
    }
}

/// The nibble of `actor` at `level`, high nibble of each byte first.
fn nibble(actor: &ActorId, level: u8) -> u8 {
    let byte = actor.as_bytes()[usize::from(level / 2)];
    if level.is_multiple_of(2) {
        byte >> 4
    } else {
        byte & 0x0f
    }
}

/// The first nibble where `a` and `b` differ.
fn first_difference(a: &ActorId, b: &ActorId) -> Option<u8> {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let byte = (0..a.len()).find(|index| a[*index] != b[*index])?;
    let high = (a[byte] ^ b[byte]) >> 4 == 0;
    Some(byte as u8 * 2 + u8::from(high))
}

fn leaf(actor: ActorId, seq: u64) -> Arc<Node> {
    Arc::new(Node::Leaf { actor, seq })
}

/// A branch at `level` over two subtrees whose keys first differ there.
fn pair(level: u8, a: Arc<Node>, b: Arc<Node>) -> Arc<Node> {
    let (na, nb) = (nibble(a.key(), level), nibble(b.key(), level));
    let key = *a.key();
    let len = a.len() + b.len();
    let children = if na < nb { vec![a, b] } else { vec![b, a] };
    Arc::new(Node::Branch {
        level,
        bitmap: (1 << na) | (1 << nb),
        len,
        key,
        children,
    })
}

/// The slot of nibble `nibble` among the children a bitmap names.
fn slot(bitmap: u16, nibble: u8) -> usize {
    (bitmap & ((1_u16 << nibble) - 1)).count_ones() as usize
}

/// Set `actor` below `node` to `seq`, or remove it for `None`, copying shared
/// nodes on the way. Returns false when the node became empty.
fn set_node(node: &mut Arc<Node>, actor: &ActorId, seq: Option<u64>) -> bool {
    let (level, key) = match &**node {
        Node::Leaf { actor: held, .. } if held == actor => {
            return match seq {
                Some(seq) => {
                    *node = leaf(*actor, seq);
                    true
                }
                None => false,
            };
        }
        Node::Leaf { actor: held, .. } => (None, *held),
        Node::Branch { level, key, .. } => (Some(*level), *key),
    };
    let difference = first_difference(&key, actor);
    let outside = match (level, difference) {
        (None, _) => true,
        (Some(level), Some(difference)) => difference < level,
        (Some(_), None) => false,
    };
    if outside {
        if let (Some(seq), Some(difference)) = (seq, difference) {
            *node = pair(difference, Arc::clone(node), leaf(*actor, seq));
        }
        return true;
    }
    let mut only = None;
    if let Node::Branch {
        level,
        bitmap,
        len,
        key,
        children,
    } = Arc::make_mut(node)
    {
        let digit = nibble(actor, *level);
        let index = slot(*bitmap, digit);
        if *bitmap & (1 << digit) == 0 {
            if let Some(seq) = seq {
                children.insert(index, leaf(*actor, seq));
                *bitmap |= 1 << digit;
            }
        } else if !set_node(&mut children[index], actor, seq) {
            children.remove(index);
            *bitmap &= !(1 << digit);
            if children.len() == 1 {
                only = children.pop();
            }
        }
        if let Some(first) = children.first() {
            *key = *first.key();
        }
        *len = children.iter().map(|child| child.len()).sum();
    }
    if let Some(only) = only {
        *node = only;
    }
    true
}

/// Entries of either trie at the higher position, sharing every subtree the
/// result leaves unchanged.
fn union(a: &Arc<Node>, b: &Arc<Node>) -> Arc<Node> {
    if Arc::ptr_eq(a, b) {
        return Arc::clone(a);
    }
    let (upper, lower) = match (&**a, &**b) {
        (_, Node::Leaf { actor, seq }) => return observed(a, actor, *seq),
        (Node::Leaf { actor, seq }, _) => return observed(b, actor, *seq),
        (
            Node::Branch {
                level: la, key: ka, ..
            },
            Node::Branch {
                level: lb, key: kb, ..
            },
        ) => {
            let lowest = (*la).min(*lb);
            match first_difference(ka, kb) {
                Some(difference) if difference < lowest => {
                    return pair(difference, Arc::clone(a), Arc::clone(b));
                }
                _ if la == lb => return union_children(a, b),
                _ if la < lb => (a, b),
                _ => (b, a),
            }
        }
    };
    let Node::Branch {
        level,
        bitmap,
        children,
        ..
    } = &**upper
    else {
        unreachable!("both nodes are branches");
    };
    let digit = nibble(lower.key(), *level);
    let mut merged = (**upper).clone();
    let Node::Branch {
        bitmap: merged_bitmap,
        len,
        children: merged_children,
        ..
    } = &mut merged
    else {
        unreachable!("a branch clones to a branch");
    };
    let index = slot(*bitmap, digit);
    if *bitmap & (1 << digit) == 0 {
        merged_children.insert(index, Arc::clone(lower));
        *merged_bitmap |= 1 << digit;
    } else {
        let child = union(&children[index], lower);
        if Arc::ptr_eq(&child, &children[index]) {
            return Arc::clone(upper);
        }
        merged_children[index] = child;
    }
    *len = merged_children.iter().map(|child| child.len()).sum();
    Arc::new(merged)
}

/// [`union`] of two branches at the same level with the same prefix.
fn union_children(a: &Arc<Node>, b: &Arc<Node>) -> Arc<Node> {
    let (
        Node::Branch {
            level,
            bitmap: ba,
            key,
            children: ca,
            ..
        },
        Node::Branch {
            bitmap: bb,
            children: cb,
            ..
        },
    ) = (&**a, &**b)
    else {
        unreachable!("both nodes are branches");
    };
    let bitmap = ba | bb;
    let mut children = Vec::with_capacity(bitmap.count_ones() as usize);
    for digit in (0..16).filter(|digit| bitmap & (1 << digit) != 0) {
        let from_a = (ba & (1 << digit) != 0).then(|| &ca[slot(*ba, digit)]);
        let from_b = (bb & (1 << digit) != 0).then(|| &cb[slot(*bb, digit)]);
        children.push(match (from_a, from_b) {
            (Some(x), Some(y)) => union(x, y),
            (Some(x), None) | (None, Some(x)) => Arc::clone(x),
            (None, None) => unreachable!("the bitmap names the digit"),
        });
    }
    let same = |bits: u16, own: &[Arc<Node>]| {
        bits == bitmap && own.iter().zip(&children).all(|(x, y)| Arc::ptr_eq(x, y))
    };
    if same(*ba, ca) {
        return Arc::clone(a);
    }
    if same(*bb, cb) {
        return Arc::clone(b);
    }
    Arc::new(Node::Branch {
        level: *level,
        bitmap,
        len: children.iter().map(|child| child.len()).sum(),
        key: *key,
        children,
    })
}

/// `node` with `actor` at least at `seq`, inserted when absent.
fn observed(node: &Arc<Node>, actor: &ActorId, seq: u64) -> Arc<Node> {
    if lookup(node, actor).is_some_and(|held| held >= seq) {
        return Arc::clone(node);
    }
    let mut node = Arc::clone(node);
    set_node(&mut node, actor, Some(seq));
    node
}

fn lookup(mut node: &Node, actor: &ActorId) -> Option<u64> {
    loop {
        match node {
            Node::Leaf { actor: held, seq } => return (held == actor).then_some(*seq),
            Node::Branch {
                level,
                bitmap,
                children,
                ..
            } => {
                let digit = nibble(actor, *level);
                if bitmap & (1 << digit) == 0 {
                    return None;
                }
                node = &children[slot(*bitmap, digit)];
            }
        }
    }
}

impl ActorClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, actor: &ActorId) -> u64 {
        self.entry(actor).unwrap_or_default()
    }

    fn entry(&self, actor: &ActorId) -> Option<u64> {
        lookup(self.root.as_deref()?, actor)
    }

    /// Store `seq` for `actor`, or remove it for `None`.
    fn put(&mut self, actor: ActorId, seq: Option<u64>) {
        if self.entry(&actor) == seq {
            return;
        }
        match &mut self.root {
            Some(root) => {
                if !set_node(root, &actor, seq) {
                    self.root = None;
                }
            }
            None => self.root = seq.map(|seq| leaf(actor, seq)),
        }
    }

    pub fn advance(&mut self, actor: ActorId) -> u64 {
        let next = self.get(&actor).saturating_add(1);
        self.put(actor, Some(next));
        next
    }

    pub fn observe(&mut self, actor: ActorId, seq: u64) {
        let current = self.entry(&actor);
        self.put(actor, Some(current.unwrap_or_default().max(seq)));
    }

    /// Set `actor`'s counter, lowering it if needed; zero removes the entry.
    pub fn set(&mut self, actor: ActorId, seq: u64) {
        self.put(actor, (seq != 0).then_some(seq));
    }

    pub fn merge(&mut self, other: &Self) {
        self.root = match (&self.root, &other.root) {
            (Some(own), Some(theirs)) => Some(union(own, theirs)),
            (own, theirs) => own.clone().or_else(|| theirs.clone()),
        };
    }

    pub fn intersect(&self, other: &Self) -> Self {
        let (small, large) = if self.len() <= other.len() {
            (self, other)
        } else {
            (other, self)
        };
        let mut clock = Self::new();
        for (actor, counter) in small.iter() {
            if let Some(held) = large.entry(actor) {
                clock.put(*actor, Some((*counter).min(held)));
            }
        }
        clock
    }

    pub fn dominates(&self, other: &Self) -> bool {
        let shared = matches!((&self.root, &other.root), (Some(own), Some(theirs)) if Arc::ptr_eq(own, theirs));
        shared
            || other
                .iter()
                .all(|(actor, counter)| self.get(actor) >= *counter)
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Entries of the clock.
    pub(crate) fn len(&self) -> usize {
        self.root.as_deref().map_or(0, Node::len)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ActorId, &u64)> {
        let mut stack = Vec::new();
        stack.extend(self.root.as_deref());
        std::iter::from_fn(move || {
            loop {
                match stack.pop()? {
                    Node::Leaf { actor, seq } => return Some((actor, seq)),
                    Node::Branch { children, .. } => {
                        stack.extend(children.iter().rev().map(|child| &**child));
                    }
                }
            }
        })
    }
}

impl PartialEq for ActorClock {
    fn eq(&self, other: &Self) -> bool {
        match (&self.root, &other.root) {
            (Some(own), Some(theirs)) if Arc::ptr_eq(own, theirs) => true,
            _ => self.len() == other.len() && self.iter().eq(other.iter()),
        }
    }
}

impl Eq for ActorClock {}

impl std::fmt::Debug for ActorClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct Entries<'a>(&'a ActorClock);
        impl std::fmt::Debug for Entries<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_map().entries(self.0.iter()).finish()
            }
        }
        f.debug_struct("ActorClock")
            .field("entries", &Entries(self))
            .finish()
    }
}

impl Serialize for ActorClock {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        struct Entries<'a>(&'a ActorClock);
        impl Serialize for Entries<'_> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut map = serializer.serialize_map(Some(self.0.len()))?;
                for (actor, seq) in self.0.iter() {
                    map.serialize_entry(actor, seq)?;
                }
                map.end()
            }
        }
        #[derive(Serialize)]
        #[serde(rename = "ActorClock")]
        struct Wire<'a> {
            entries: Entries<'a>,
        }
        Wire {
            entries: Entries(self),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ActorClock {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "ActorClock")]
        struct Wire {
            entries: BTreeMap<ActorId, u64>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut clock = Self::new();
        for (actor, seq) in wire.entries {
            clock.put(actor, Some(seq));
        }
        Ok(clock)
    }
}
