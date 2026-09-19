// SPDX-License-Identifier: MIT OR Apache-2.0
//! Actor/vector-clock utilities used for admission, reducers, and sync.

use crate::ids::ActorId;
use serde::de::Deserializer;
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[cfg(feature = "fjall")]
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Weak};
#[cfg(feature = "fjall")]
use std::sync::{Mutex, OnceLock};

#[cfg(feature = "fjall")]
pub(crate) mod scan;

/// The cached hash of a node's stored form. Only Fjall stores nodes, so other
/// builds keep nothing here.
#[cfg(feature = "fjall")]
type NodeHash = OnceLock<[u8; 32]>;
#[cfg(not(feature = "fjall"))]
type NodeHash = ();

/// Actor positions in a persistent trie over actor-ID nibbles. Clones share nodes;
/// updates copy only changed paths. Iteration is ID-ordered; serialization is a map
/// of every entry, including zero positions.
#[derive(Clone, Default)]
pub struct ActorClock {
    root: Option<Arc<Node>>,
}

/// An owned traversal of immutable nodes, resumable without retaining a read lock.
#[derive(Default)]
pub(crate) struct ClockCursor {
    stack: Vec<Arc<Node>>,
}

impl ClockCursor {
    pub(crate) fn bytes(&self) -> usize {
        self.stack.capacity() * size_of::<Arc<Node>>()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }
}

impl Iterator for ClockCursor {
    type Item = (ActorId, u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &*self.stack.pop()? {
                Node::Leaf { actor, seq, .. } => return Some((*actor, *seq)),
                Node::Branch { children, .. } => {
                    self.stack.extend(children.iter().rev().cloned());
                }
            }
        }
    }
}

#[cfg_attr(
    not(feature = "fjall"),
    expect(dead_code, reason = "only Fjall reads node hashes")
)]
enum Node {
    Leaf {
        actor: ActorId,
        seq: u64,
        hash: NodeHash,
    },
    /// Entries sharing the nibbles of `key` before `level`, one child per
    /// distinct nibble at `level`, in nibble order. At least two children.
    Branch {
        level: u8,
        bitmap: u16,
        len: usize,
        key: ActorId,
        children: Vec<Arc<Node>>,
        hash: NodeHash,
    },
}

/// A clone is about to change, so it does not keep the hash of its source.
impl Clone for Node {
    fn clone(&self) -> Self {
        match self {
            Node::Leaf { actor, seq, .. } => Node::Leaf {
                actor: *actor,
                seq: *seq,
                hash: NodeHash::default(),
            },
            Node::Branch {
                level,
                bitmap,
                len,
                key,
                children,
                ..
            } => Node::Branch {
                level: *level,
                bitmap: *bitmap,
                len: *len,
                key: *key,
                children: children.clone(),
                hash: NodeHash::default(),
            },
        }
    }
}

#[cfg(feature = "fjall")]
/// The stored form of one trie node, its children named by their hashes.
#[derive(Serialize, Deserialize)]
enum Encoded {
    Leaf {
        actor: ActorId,
        seq: u64,
    },
    Branch {
        level: u8,
        bitmap: u16,
        len: u64,
        key: ActorId,
        children: Vec<[u8; 32]>,
    },
}

#[cfg(feature = "fjall")]
/// Separates node hashes from every other blake3 hash of this crate.
const NODE_DOMAIN: &[u8] = b"irokle/clock-node/1";

#[cfg(feature = "fjall")]
pub(crate) struct ClockRecord<'a> {
    node: &'a Node,
    pub(crate) hash: [u8; 32],
}

#[cfg(feature = "fjall")]
impl Serialize for ClockRecord<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.node.encoded().serialize(serializer)
    }
}

#[cfg(feature = "fjall")]
fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(NODE_DOMAIN);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

#[cfg(feature = "fjall")]
impl Node {
    fn encoded(&self) -> Encoded {
        match self {
            Node::Leaf { actor, seq, .. } => Encoded::Leaf {
                actor: *actor,
                seq: *seq,
            },
            Node::Branch {
                level,
                bitmap,
                len,
                key,
                children,
                ..
            } => Encoded::Branch {
                level: *level,
                bitmap: *bitmap,
                len: *len as u64,
                key: *key,
                children: children.iter().map(|child| child.hash()).collect(),
            },
        }
    }

    /// The hash of this node's stored form, computed once.
    fn hash(&self) -> [u8; 32] {
        let cell = match self {
            Node::Leaf { hash, .. } | Node::Branch { hash, .. } => hash,
        };
        *cell.get_or_init(|| {
            digest(&postcard::to_allocvec(&self.encoded()).expect("a clock node encodes"))
        })
    }
}

impl Node {
    fn bytes(&self) -> usize {
        let children = match self {
            Self::Leaf { .. } => 0,
            Self::Branch { children, .. } => children.capacity() * size_of::<Arc<Node>>() + 16,
        };
        size_of::<Self>() + 2 * size_of::<usize>() + 16 + children
    }

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

#[derive(Clone)]
pub(crate) struct ClockAllocation {
    pub(crate) address: usize,
    pub(crate) bytes: usize,
    node: Weak<Node>,
}

impl ClockAllocation {
    pub(crate) fn alive(&self) -> bool {
        self.node.strong_count() > 0
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
    Arc::new(Node::Leaf {
        actor,
        seq,
        hash: NodeHash::default(),
    })
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
        hash: NodeHash::default(),
    })
}

/// The slot of nibble `nibble` among the children a bitmap names.
fn slot(bitmap: u16, nibble: u8) -> usize {
    (bitmap & ((1_u16 << nibble) - 1)).count_ones() as usize
}

fn selected_node(
    node: &Arc<Node>,
    actors: &std::collections::BTreeSet<ActorId>,
) -> Option<Arc<Node>> {
    let Node::Branch {
        level, children, ..
    } = &**node
    else {
        return actors.contains(node.key()).then(|| Arc::clone(node));
    };
    let mut changed = None::<Vec<Arc<Node>>>;
    for (index, child) in children.iter().enumerate() {
        let selected = selected_node(child, actors);
        if changed.is_none()
            && selected
                .as_ref()
                .is_some_and(|selected| Arc::ptr_eq(selected, child))
        {
            continue;
        }
        let changed = changed.get_or_insert_with(|| {
            let mut changed = Vec::with_capacity(children.len());
            changed.extend(children[..index].iter().cloned());
            changed
        });
        changed.extend(selected);
    }
    let Some(children) = changed else {
        return Some(Arc::clone(node));
    };
    match children.len() {
        0 => None,
        1 => children.into_iter().next(),
        _ => Some(Arc::new(Node::Branch {
            level: *level,
            bitmap: children
                .iter()
                .fold(0, |bits, child| bits | (1 << nibble(child.key(), *level))),
            len: children.iter().map(|child| child.len()).sum(),
            key: *children[0].key(),
            children,
            hash: NodeHash::default(),
        })),
    }
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
        hash,
    } = Arc::make_mut(node)
    {
        // A node held only here changes in place and loses its hash.
        *hash = NodeHash::default();
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
        (_, Node::Leaf { actor, seq, .. }) => return observed(a, actor, *seq),
        (Node::Leaf { actor, seq, .. }, _) => return observed(b, actor, *seq),
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
        hash: NodeHash::default(),
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
            Node::Leaf {
                actor: held, seq, ..
            } => return (held == actor).then_some(*seq),
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
    pub(crate) fn visit_allocations(
        &self,
        mut visit: impl FnMut(ClockAllocation) -> crate::Result<bool>,
    ) -> crate::Result<()> {
        let mut stack = self.root.iter().collect::<Vec<_>>();
        while let Some(node) = stack.pop() {
            if !visit(ClockAllocation {
                address: Arc::as_ptr(node) as usize,
                bytes: node.bytes(),
                node: Arc::downgrade(node),
            })? {
                continue;
            }
            if let Node::Branch { children, .. } = &**node {
                stack.extend(children);
            }
        }
        Ok(())
    }
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

    /// Reserve for at most two nodes per entry and full child-vector capacity.
    pub(crate) fn allocation_bound(entries: usize) -> usize {
        let node = size_of::<Node>() + 2 * size_of::<usize>() + 16;
        entries.saturating_mul(2 * node + 16 * size_of::<Arc<Node>>() + 16)
    }

    pub(crate) fn selected(&self, actors: &std::collections::BTreeSet<ActorId>) -> Self {
        if actors.len().saturating_mul(2) >= self.len() {
            return Self {
                root: self
                    .root
                    .as_ref()
                    .and_then(|node| selected_node(node, actors)),
            };
        }
        let mut clock = Self::new();
        for actor in actors {
            if let Some(seq) = self.entry(actor) {
                clock.put(*actor, Some(seq));
            }
        }
        clock
    }

    #[cfg(all(feature = "fjall", test))]
    pub(crate) fn decode_selected(
        bytes: &[u8],
        actors: &std::collections::BTreeSet<ActorId>,
    ) -> crate::Result<Self> {
        Self::decode_counted(bytes, actors).map(|(clock, _)| clock)
    }

    #[cfg(feature = "fjall")]
    pub(crate) fn decode_counted(
        bytes: &[u8],
        actors: &std::collections::BTreeSet<ActorId>,
    ) -> crate::Result<(Self, usize)> {
        struct Selected<'a>(&'a std::collections::BTreeSet<ActorId>);
        impl<'de> serde::de::Visitor<'de> for Selected<'_> {
            type Value = (ActorClock, usize);
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("actor positions")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut selected = BTreeMap::new();
                let mut count = 0;
                while let Some((actor, seq)) = map.next_entry::<ActorId, u64>()? {
                    count += 1;
                    if self.0.contains(&actor) {
                        selected.insert(actor, seq);
                    }
                }
                let mut clock = ActorClock::new();
                for (actor, seq) in selected {
                    clock.put(actor, Some(seq));
                }
                Ok((clock, count))
            }
        }
        let mut decoder = postcard::Deserializer::from_bytes(bytes);
        let selected = decoder.deserialize_map(Selected(actors))?;
        if !decoder.finalize()?.is_empty() {
            return Err(crate::Error::Storage(
                "trailing bytes in stored clock".into(),
            ));
        }
        Ok(selected)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ActorId, &u64)> {
        let mut stack = Vec::new();
        stack.extend(self.root.as_deref());
        std::iter::from_fn(move || {
            loop {
                match stack.pop()? {
                    Node::Leaf { actor, seq, .. } => return Some((actor, seq)),
                    Node::Branch { children, .. } => {
                        stack.extend(children.iter().rev().map(|child| &**child));
                    }
                }
            }
        })
    }

    pub(crate) fn cursor(&self) -> ClockCursor {
        ClockCursor {
            stack: self.root.iter().cloned().collect(),
        }
    }
}

#[cfg(feature = "fjall")]
impl ActorClock {
    /// The hash naming this clock's stored root node; `None` when empty.
    pub(crate) fn root_hash(&self) -> Option<[u8; 32]> {
        self.root.as_deref().map(Node::hash)
    }

    /// The stored form of every node of this clock that `stored` does not
    /// report as held, by hash and parents first. The subtree of a held node
    /// is held too, so it is not visited.
    #[cfg(test)]
    pub(crate) fn unstored_nodes(
        &self,
        mut stored: impl FnMut(&[u8; 32]) -> crate::Result<bool>,
    ) -> crate::Result<Vec<([u8; 32], Vec<u8>)>> {
        let mut out = Vec::new();
        self.visit_nodes(|record| {
            if stored(&record.hash)? {
                return Ok(true);
            }
            out.push((record.hash, postcard::to_allocvec(&record)?));
            Ok(false)
        })?;
        Ok(out)
    }

    /// Visit missing nodes without collecting their encoded bodies in memory.
    pub(crate) fn visit_nodes(
        &self,
        mut visit: impl FnMut(ClockRecord<'_>) -> crate::Result<bool>,
    ) -> crate::Result<()> {
        let mut stack = Vec::new();
        stack.extend(self.root.as_deref());
        while let Some(node) = stack.pop() {
            let hash = node.hash();
            if visit(ClockRecord { node, hash })? {
                continue;
            }
            if let Node::Branch { children, .. } = node {
                stack.extend(children.iter().map(|child| &**child));
            }
        }
        Ok(())
    }

    /// The clock whose stored root node is `root`, reading nodes through
    /// `fetch` and sharing every node `cache` still holds. A node whose bytes
    /// do not hash to its name, or that breaks the trie's shape, is refused.
    pub(crate) fn load(
        root: &[u8; 32],
        cache: &ClockCache,
        mut fetch: impl FnMut(&[u8; 32]) -> crate::Result<Option<Vec<u8>>>,
    ) -> crate::Result<Self> {
        let root = cache.node(root, &mut fetch, None)?;
        Ok(Self { root: Some(root) })
    }
}

/// Reads the stored bytes of the node a hash names.
#[cfg(feature = "fjall")]
type NodeFetch<'a> = dyn FnMut(&[u8; 32]) -> crate::Result<Option<Vec<u8>>> + 'a;

#[cfg(feature = "fjall")]
fn corrupt() -> crate::Error {
    crate::Error::Storage("corrupt stored clock node".into())
}

#[cfg(feature = "fjall")]
/// Reuse shared clock nodes without reading them again. Only the latest clocks
/// retain nodes within an estimated byte limit; other cached nodes are weak references.
#[derive(Default)]
pub(crate) struct ClockCache {
    inner: Mutex<CacheInner>,
}

#[cfg(feature = "fjall")]
#[derive(Default)]
struct CacheInner {
    nodes: HashMap<[u8; 32], Weak<Node>>,
    latest: VecDeque<Arc<Node>>,
    retained: HashMap<usize, CachedNode>,
    bytes: usize,
}

#[cfg(feature = "fjall")]
struct CachedNode {
    references: usize,
    root: bool,
}

#[cfg(feature = "fjall")]
/// Modeled bytes of retained nodes and their ownership index.
const CACHE_BYTES: usize = 64 * 1024 * 1024;
#[cfg(feature = "fjall")]
/// A single root's conservative node and ownership-index allowance per entry.
const ENTRY_BYTES: usize = 512;
#[cfg(feature = "fjall")]
/// Node names the cache tracks before it drops those nothing holds.
const CACHE_NODES: usize = 1 << 18;

#[cfg(feature = "fjall")]
impl ClockCache {
    pub(crate) fn bytes(&self) -> usize {
        let inner = self.lock();
        let weak = inner
            .nodes
            .values()
            .filter(|node| !inner.retained.contains_key(&(node.as_ptr() as usize)))
            .count();
        inner.root_bytes()
            + table_bytes(&inner.nodes)
            + weak * (size_of::<Node>() + 2 * size_of::<usize>() + 16)
            + size_of::<Self>()
            + 2 * size_of::<usize>()
            + 16
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheInner> {
        // Only a shortcut: a poisoned cache still names valid nodes.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Hold `clock` among the latest clocks and name its nodes, so a clock
    /// loaded or derived from it next shares them.
    pub(crate) fn keep(&self, clock: &ActorClock) {
        let Some(root) = &clock.root else {
            return;
        };
        let bytes = root.len().saturating_mul(ENTRY_BYTES);
        if bytes > CACHE_BYTES {
            return;
        }
        let mut inner = self.lock();
        if inner
            .retained
            .get(&(Arc::as_ptr(root) as usize))
            .is_some_and(|node| node.root)
        {
            return;
        }
        inner.remember(root);
        if let Some(node) = inner.retained.get_mut(&(Arc::as_ptr(root) as usize)) {
            node.root = true;
        }
        inner.latest.push_back(Arc::clone(root));
        inner.trim(CACHE_BYTES);
    }

    /// The node named `hash`: shared when something still holds it, otherwise
    /// read, checked against its name and, below `parent`, against its place.
    fn node(
        &self,
        hash: &[u8; 32],
        fetch: &mut NodeFetch<'_>,
        parent: Option<(u8, &ActorId)>,
    ) -> crate::Result<Arc<Node>> {
        let held = self.lock().nodes.get(hash).and_then(Weak::upgrade);
        let node = match held {
            Some(node) => node,
            None => {
                let bytes = fetch(hash)?.ok_or_else(corrupt)?;
                if digest(&bytes) != *hash {
                    return Err(corrupt());
                }
                let node = match postcard::from_bytes::<Encoded>(&bytes)? {
                    Encoded::Leaf { actor, seq } => Node::Leaf {
                        actor,
                        seq,
                        #[cfg(feature = "fjall")]
                        hash: OnceLock::from(*hash),
                    },
                    Encoded::Branch {
                        level,
                        bitmap,
                        len,
                        key,
                        children,
                    } => {
                        if level >= 64
                            || children.len() < 2
                            || bitmap.count_ones() as usize != children.len()
                        {
                            return Err(corrupt());
                        }
                        let children = children
                            .iter()
                            .map(|child| self.node(child, fetch, Some((level, &key))))
                            .collect::<crate::Result<Vec<_>>>()?;
                        let digits = children
                            .iter()
                            .map(|child| 1_u16 << nibble(child.key(), level))
                            .fold(0, |bits, bit| bits | bit);
                        let total = children.iter().map(|child| child.len()).sum::<usize>();
                        if digits != bitmap
                            || total as u64 != len
                            || children.windows(2).any(|pair| {
                                nibble(pair[0].key(), level) >= nibble(pair[1].key(), level)
                            })
                        {
                            return Err(corrupt());
                        }
                        Node::Branch {
                            level,
                            bitmap,
                            len: total,
                            key,
                            children,
                            #[cfg(feature = "fjall")]
                            hash: OnceLock::from(*hash),
                        }
                    }
                };
                let node = Arc::new(node);
                self.lock().name(hash, &node);
                node
            }
        };
        // A child shares its parent's nibbles before the parent's level and
        // branches, if at all, below it.
        if let Some((level, key)) = parent {
            let below = match &*node {
                Node::Leaf { .. } => true,
                Node::Branch { level: child, .. } => *child > level,
            };
            if !below || first_difference(node.key(), key).is_some_and(|at| at < level) {
                return Err(corrupt());
            }
        }
        Ok(node)
    }
}

#[cfg(feature = "fjall")]
impl CacheInner {
    fn root_bytes(&self) -> usize {
        self.bytes
            + table_bytes(&self.retained)
            + self.latest.capacity() * size_of::<Arc<Node>>()
            + usize::from(self.latest.capacity() > 0) * 16
    }

    fn trim(&mut self, limit: usize) {
        while self.root_bytes() > limit {
            let Some(root) = self.latest.pop_front() else {
                self.retained.shrink_to_fit();
                self.latest.shrink_to_fit();
                break;
            };
            self.release(&root);
        }
    }

    fn name(&mut self, hash: &[u8; 32], node: &Arc<Node>) {
        if let Some(named) = self.nodes.get_mut(hash) {
            *named = Arc::downgrade(node);
            return;
        }
        if self.nodes.len() >= CACHE_NODES {
            self.nodes.retain(|_, node| node.strong_count() > 0);
            if self.nodes.len() >= CACHE_NODES / 2 {
                self.nodes.clear();
            }
        }
        self.nodes.insert(*hash, Arc::downgrade(node));
    }

    /// Count each retained parent edge and root, visiting a shared subtree once.
    fn remember(&mut self, node: &Arc<Node>) {
        let mut stack = vec![node];
        while let Some(node) = stack.pop() {
            let address = Arc::as_ptr(node) as usize;
            if let Some(node) = self.retained.get_mut(&address) {
                node.references += 1;
                continue;
            }
            self.retained.insert(
                address,
                CachedNode {
                    references: 1,
                    root: false,
                },
            );
            self.bytes += node.bytes();
            self.name(&node.hash(), node);
            if let Node::Branch { children, .. } = &**node {
                stack.extend(children);
            }
        }
    }

    fn release(&mut self, node: &Arc<Node>) {
        if let Some(node) = self.retained.get_mut(&(Arc::as_ptr(node) as usize)) {
            node.root = false;
        }
        let mut stack = vec![node];
        while let Some(node) = stack.pop() {
            let address = Arc::as_ptr(node) as usize;
            if let Some(node) = self.retained.get_mut(&address) {
                node.references -= 1;
                if node.references != 0 {
                    continue;
                }
            }
            self.retained.remove(&address);
            self.bytes -= node.bytes();
            if let Node::Branch { children, .. } = &**node {
                stack.extend(children);
            }
        }
    }
}

#[cfg(feature = "fjall")]
fn table_bytes<K, V>(table: &HashMap<K, V>) -> usize {
    if table.capacity() == 0 {
        return 0;
    }
    table.capacity().next_power_of_two() * (size_of::<(K, V)>() + 1) + 64
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

#[cfg(test)]
mod tests {
    #[test]
    fn cursor_keeps_snapshot() {
        use crate::clock::*;
        let mut clock = ActorClock::new();
        for index in 0_u32..65_537 {
            clock.observe(ActorId::hash(index.to_le_bytes()), u64::from(index));
        }
        let expected = clock
            .iter()
            .map(|(actor, seq)| (*actor, *seq))
            .collect::<Vec<_>>();
        let mut cursor = clock.cursor();
        let mut actual = Vec::new();
        for _ in 0..257 {
            actual.push(cursor.next().unwrap());
        }
        clock.observe(ActorId::hash(b"later"), 9);
        drop(clock);
        while let Some(entry) = cursor.next() {
            actual.push(entry);
            // At most fifteen pending siblings at each of sixty-four levels.
            assert!(cursor.bytes() <= 2048 * size_of::<Arc<Node>>());
        }
        assert_eq!(actual, expected);
        assert!(cursor.is_empty());
        assert_eq!(cursor.next(), None);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn selected_clock_matches() {
        let mut clock = ActorClock::new();
        for n in 0_u8..33 {
            clock.observe(ActorId::from_bytes([n; 32]), u64::from(n));
        }
        let actors = [0, 4, 32, 99].map(|n| ActorId::from_bytes([n; 32])).into();
        let bytes = postcard::to_allocvec(&clock).unwrap();
        let selected = ActorClock::decode_selected(&bytes, &actors).unwrap();
        assert_eq!(selected, clock.selected(&actors));
        assert_eq!(selected.len(), 3, "zero positions remain encoded");
        assert_eq!(ActorClock::decode_counted(&bytes, &actors).unwrap().1, 33);
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(ActorClock::decode_counted(&trailing, &actors).is_err());
        let mut damaged = bytes;
        *damaged.last_mut().unwrap() = 0x80;
        let first = [ActorId::from_bytes([0; 32])].into();
        assert!(ActorClock::decode_selected(&damaged, &first).is_err());
    }

    #[test]
    fn selected_shares() {
        let mut clock = ActorClock::new();
        for n in 0..1024_u32 {
            clock.observe(ActorId::hash(n.to_le_bytes()), u64::from(n % 7));
        }
        let all = clock
            .iter()
            .map(|(actor, _)| *actor)
            .collect::<std::collections::BTreeSet<_>>();
        let same = clock.selected(&all);
        assert!(Arc::ptr_eq(
            clock.root.as_ref().unwrap(),
            same.root.as_ref().unwrap()
        ));
        for stride in [1, 2, 3, 4, usize::MAX] {
            let actors = all
                .iter()
                .enumerate()
                .filter(|(index, _)| index % stride != 0)
                .map(|(_, actor)| *actor)
                .chain([ActorId::hash(b"absent selection")])
                .collect();
            let selected = clock.selected(&actors);
            let expected = Reference {
                entries: clock
                    .iter()
                    .filter(|(actor, _)| actors.contains(actor))
                    .map(|(actor, seq)| (*actor, *seq))
                    .collect(),
            };
            assert_same(&selected, &expected, true);
            let mut changed = clock.clone();
            for actor in &actors {
                changed.observe(*actor, 99);
            }
            assert_same(&selected, &expected, true);
        }
    }

    use crate::clock::*;

    /// The clock this type replaced, kept as the reference its behavior must match.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    struct Reference {
        entries: BTreeMap<ActorId, u64>,
    }

    impl Reference {
        fn observe(&mut self, actor: ActorId, seq: u64) {
            let current = self.entries.entry(actor).or_default();
            *current = (*current).max(seq);
        }

        fn set(&mut self, actor: ActorId, seq: u64) {
            if seq == 0 {
                self.entries.remove(&actor);
            } else {
                self.entries.insert(actor, seq);
            }
        }

        fn merge(&mut self, other: &Self) {
            for (actor, counter) in &other.entries {
                self.observe(*actor, *counter);
            }
        }
    }

    /// A seeded generator, so every run draws the same operations.
    struct Draw(u64);

    impl Draw {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// Actor ids from a small pool that share long prefixes, so tries get
        /// deep branches, splits inside compressed paths and collapses.
        fn actor(&mut self) -> ActorId {
            let mut bytes = [0xa5_u8; 32];
            let pick = self.next();
            let at = (pick % 32) as usize;
            bytes[at] = (pick >> 8) as u8 % 4;
            if pick >> 16 & 1 == 1 {
                bytes[31] = (pick >> 24) as u8 % 3;
            }
            if pick >> 20 & 15 == 0 {
                bytes = *blake3::hash(&(pick % 64).to_le_bytes()).as_bytes();
            }
            ActorId::from_bytes(bytes)
        }
    }

    fn assert_same(clock: &ActorClock, reference: &Reference, encoded: bool) {
        let entries = clock
            .iter()
            .map(|(actor, seq)| (*actor, *seq))
            .collect::<Vec<_>>();
        let expected = reference
            .entries
            .iter()
            .map(|(actor, seq)| (*actor, *seq))
            .collect::<Vec<_>>();
        assert_eq!(entries, expected);
        assert_eq!(clock.len(), expected.len());
        assert_eq!(clock.is_empty(), expected.is_empty());
        if !encoded {
            return;
        }
        assert_eq!(
            postcard::to_allocvec(clock).unwrap(),
            postcard::to_allocvec(reference).unwrap()
        );
        assert_eq!(
            format!("{clock:?}"),
            format!("{reference:?}").replace("Reference", "ActorClock")
        );
        let decoded: ActorClock =
            postcard::from_bytes(&postcard::to_allocvec(reference).unwrap()).unwrap();
        assert_eq!(&decoded, clock);
    }

    /// Random operations on shared and unshared clocks match the map they
    /// replaced: entries, zero positions, order, encoding, equality and every
    /// derived answer, and a clone never sees a later change of its source.
    #[test]
    fn matches_reference() {
        let mut draw = Draw(0x9e37_79b9_7f4a_7c15);
        let mut clocks = vec![(ActorClock::new(), Reference::default()); 4];
        for step in 0..20_000 {
            let which = (draw.next() % 4) as usize;
            let actor = draw.actor();
            let seq = draw.next() % 5;
            let before = clocks[which].clone();
            match draw.next() % 6 {
                0 => {
                    let next = clocks[which].0.advance(actor);
                    *clocks[which].1.entries.entry(actor).or_default() += 1;
                    assert_eq!(next, clocks[which].1.entries[&actor]);
                }
                1 | 2 => {
                    clocks[which].0.observe(actor, seq);
                    clocks[which].1.observe(actor, seq);
                }
                3 => {
                    clocks[which].0.set(actor, seq);
                    clocks[which].1.set(actor, seq);
                }
                4 => {
                    let other = clocks[(draw.next() % 4) as usize].clone();
                    clocks[which].0.merge(&other.0);
                    clocks[which].1.merge(&other.1);
                }
                _ => {
                    let other = clocks[(draw.next() % 4) as usize].clone();
                    clocks[which] = other;
                }
            }
            let (clock, reference) = &clocks[which];
            let encoded = step % 31 == 0;
            assert_same(clock, reference, encoded);
            assert_same(&before.0, &before.1, encoded);
            assert_eq!(
                clock.get(&actor),
                reference.entries.get(&actor).copied().unwrap_or(0)
            );
            if step % 97 == 0 {
                for (other, other_reference) in &clocks {
                    assert_eq!(clock == other, reference == other_reference, "step {step}");
                    let dominates = other_reference.entries.iter().all(|(actor, seq)| {
                        reference.entries.get(actor).copied().unwrap_or(0) >= *seq
                    });
                    assert_eq!(clock.dominates(other), dominates);
                    let intersection = reference
                        .entries
                        .iter()
                        .filter_map(|(actor, seq)| {
                            other_reference
                                .entries
                                .get(actor)
                                .map(|held| (*actor, (*seq).min(*held)))
                        })
                        .collect();
                    assert_same(
                        &clock.intersect(other),
                        &Reference {
                            entries: intersection,
                        },
                        true,
                    );
                }
            }
        }
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn cache_counts_shared() {
        let cache = ClockCache::default();
        let mut clock = ActorClock::new();
        let mut roots = Vec::new();
        for index in 0_u32..1024 {
            clock.observe(
                ActorId::from_bytes(*blake3::hash(&index.to_le_bytes()).as_bytes()),
                1,
            );
            cache.keep(&clock);
            roots.push(Arc::clone(clock.root.as_ref().unwrap()));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut bytes = 0;
        let mut stack = roots.iter().collect::<Vec<_>>();
        while let Some(node) = stack.pop() {
            if !seen.insert(Arc::as_ptr(node)) {
                continue;
            }
            bytes += size_of::<Node>() + 2 * size_of::<usize>() + 16;
            if let Node::Branch { children, .. } = &**node {
                bytes += children.capacity() * size_of::<Arc<Node>>() + 16;
                stack.extend(children);
            }
        }
        assert_eq!(
            cache.lock().bytes,
            bytes,
            "charge each retained allocation once"
        );
        cache.keep(&clock);
        assert_eq!(cache.lock().bytes, bytes);
        cache.keep(&ActorClock {
            root: Some(Arc::clone(&roots[0])),
        });
        assert_eq!(cache.lock().latest.len(), roots.len());
        let duplicate = ActorClock {
            root: clock.root.as_ref().map(|root| Arc::new((**root).clone())),
        };
        cache.keep(&duplicate);
        assert_eq!(
            cache.lock().bytes,
            bytes + duplicate.root.as_ref().unwrap().bytes()
        );
        assert!(cache.bytes() > cache.lock().bytes);
        {
            let mut inner = cache.lock();
            inner.trim(0);
            assert_eq!(inner.bytes, 0);
            assert_eq!(inner.root_bytes(), 0);
            assert!(inner.retained.is_empty());
        }
        let loaded = ActorClock::load(&clock.root_hash().unwrap(), &cache, |_| {
            panic!("external holders remain live")
        })
        .unwrap();
        assert_eq!(loaded, clock);
    }

    #[cfg(feature = "fjall")]
    #[test]
    fn unordered_nodes_refused() {
        let mut clock = ActorClock::new();
        for n in [1, 2, 3] {
            clock.observe(ActorId::from_bytes([n; 32]), u64::from(n));
        }
        let mut store = clock
            .unstored_nodes(|_| Ok(false))
            .unwrap()
            .into_iter()
            .collect::<HashMap<_, _>>();
        let root = clock.root_hash().unwrap();
        let Encoded::Branch {
            level,
            bitmap,
            len,
            key,
            mut children,
        } = postcard::from_bytes(&store[&root]).unwrap()
        else {
            panic!("branch required")
        };
        children.swap(0, 1);
        let bytes = postcard::to_allocvec(&Encoded::Branch {
            level,
            bitmap,
            len,
            key,
            children,
        })
        .unwrap();
        let forged = digest(&bytes);
        store.insert(forged, bytes);
        for warm in [false, true] {
            let cache = ClockCache::default();
            if warm {
                cache.keep(&clock);
            }
            let loaded = ActorClock::load(&forged, &cache, |hash| Ok(store.get(hash).cloned()));
            assert!(
                matches!(loaded, Err(crate::Error::Storage(_))),
                "unordered children accepted: {loaded:?}"
            );
            let valid =
                ActorClock::load(&root, &cache, |hash| Ok(store.get(hash).cloned())).unwrap();
            assert_eq!(valid, clock);
            assert_eq!(
                postcard::to_allocvec(&valid).unwrap(),
                postcard::to_allocvec(&clock).unwrap()
            );
        }
    }

    /// A clock stored as nodes loads back equal; a clock one position ahead
    /// stores only the path to that position; a node whose bytes do not match
    /// its name, or that sits below a parent it does not belong to, is refused.
    #[cfg(feature = "fjall")]
    #[test]
    fn nodes_round_trip() {
        let actor =
            |index: u32| ActorId::from_bytes(*blake3::hash(&index.to_le_bytes()).as_bytes());
        let mut store = std::collections::HashMap::new();
        let put = |clock: &ActorClock, store: &mut std::collections::HashMap<[u8; 32], Vec<u8>>| {
            let nodes = clock
                .unstored_nodes(|hash| Ok(store.contains_key(hash)))
                .unwrap();
            let written = nodes.len();
            store.extend(nodes);
            written
        };
        let mut clock = ActorClock::new();
        for index in 0..2048 {
            clock.observe(actor(index), u64::from(index % 7));
        }
        assert!(put(&clock, &mut store) > 2048);
        let mut ahead = clock.clone();
        ahead.observe(actor(5000), 1);
        let path = put(&ahead, &mut store);
        assert!((2..=6).contains(&path), "{path} nodes for one position");
        for (stored, cache) in [
            (&clock, ClockCache::default()),
            (&ahead, ClockCache::default()),
        ] {
            let fetch = |hash: &[u8; 32]| Ok(store.get(hash).cloned());
            let loaded = ActorClock::load(&stored.root_hash().unwrap(), &cache, fetch).unwrap();
            assert_eq!(&loaded, stored);
            assert_eq!(loaded.root_hash(), stored.root_hash());
        }
        // A cache shares the nodes of a clock it keeps with the next load.
        let cache = ClockCache::default();
        cache.keep(&clock);
        let mut reads = 0;
        let loaded = ActorClock::load(&ahead.root_hash().unwrap(), &cache, |hash| {
            reads += 1;
            Ok(store.get(hash).cloned())
        })
        .unwrap();
        assert_eq!(loaded, ahead);
        assert!(reads <= path, "{reads} reads");

        let root = clock.root_hash().unwrap();
        let mut damaged = store.clone();
        damaged.get_mut(&root).unwrap()[3] ^= 1;
        let refused = ActorClock::load(&root, &ClockCache::default(), |hash| {
            Ok(damaged.get(hash).cloned())
        });
        assert!(matches!(refused, Err(crate::Error::Storage(_))));
        // A well-named node that claims a child of another subtree is refused.
        let Some(Node::Branch { children, .. }) = clock.root.as_deref() else {
            panic!("a root of many entries is a branch");
        };
        let (first, second) = (children[0].hash(), children[1].hash());
        let Ok(Encoded::Branch {
            level,
            bitmap,
            len,
            key,
            children: named,
        }) = postcard::from_bytes::<Encoded>(&store[&first])
        else {
            panic!("a child of many entries is a branch");
        };
        let mut moved = named.clone();
        moved[0] = second;
        let forged = postcard::to_allocvec(&Encoded::Branch {
            level,
            bitmap,
            len,
            key,
            children: moved,
        })
        .unwrap();
        let forged_name = digest(&forged);
        let mut forged_store = store.clone();
        forged_store.insert(forged_name, forged);
        let refused = ActorClock::load(&forged_name, &ClockCache::default(), |hash| {
            Ok(forged_store.get(hash).cloned())
        });
        assert!(matches!(refused, Err(crate::Error::Storage(_))));
    }

    /// A clock derived by one more position shares all but the path to it, and
    /// merging a clock it already covers allocates nothing.
    #[test]
    fn derived_clocks_share() {
        let actor =
            |index: u32| ActorId::from_bytes(*blake3::hash(&index.to_le_bytes()).as_bytes());
        let mut clock = ActorClock::new();
        for index in 0..4096 {
            clock.observe(actor(index), 1);
        }
        let mut derived = clock.clone();
        derived.observe(actor(9999), 1);
        let (Some(own), Some(theirs)) = (&clock.root, &derived.root) else {
            panic!("both clocks hold entries");
        };
        let Node::Branch { children: own, .. } = &**own else {
            panic!("a root of many entries is a branch");
        };
        let Node::Branch {
            children: theirs, ..
        } = &**theirs
        else {
            panic!("a root of many entries is a branch");
        };
        let shared = own
            .iter()
            .filter(|child| theirs.iter().any(|other| Arc::ptr_eq(child, other)))
            .count();
        assert_eq!(shared, own.len() - 1);
        let mut merged = derived.clone();
        merged.merge(&clock);
        assert!(Arc::ptr_eq(
            merged.root.as_ref().unwrap(),
            derived.root.as_ref().unwrap()
        ));
    }
}
