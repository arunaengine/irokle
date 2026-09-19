// SPDX-License-Identifier: MIT OR Apache-2.0
//! Stored clock nodes: their encoding and hashes, a clock loaded from its root
//! node, and the cache that shares loaded nodes between clocks.

use crate::clock::{ActorClock, Node, first_difference, nibble};
use crate::ids::ActorId;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// The stored form of one trie node, its children named by their hashes.
#[derive(Serialize, Deserialize)]
pub(super) enum Encoded {
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

/// Separates node hashes from every other blake3 hash of this crate.
const NODE_DOMAIN: &[u8] = b"irokle/clock-node/1";

pub(crate) struct ClockRecord<'a> {
    node: &'a Node,
    pub(crate) hash: [u8; 32],
}

impl Serialize for ClockRecord<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.node.encoded().serialize(serializer)
    }
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(NODE_DOMAIN);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

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

impl ActorClock {
    #[cfg(test)]
    pub(crate) fn decode_selected(
        bytes: &[u8],
        actors: &std::collections::BTreeSet<ActorId>,
    ) -> crate::Result<Self> {
        Self::decode_counted(bytes, actors).map(|(clock, _)| clock)
    }

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
type NodeFetch<'a> = dyn FnMut(&[u8; 32]) -> crate::Result<Option<Vec<u8>>> + 'a;

pub(super) fn corrupt() -> crate::Error {
    crate::Error::Storage("corrupt stored clock node".into())
}

/// Reuse shared clock nodes without reading them again. Only the latest clocks
/// retain nodes within an estimated byte limit; other cached nodes are weak references.
#[derive(Default)]
pub(crate) struct ClockCache {
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    nodes: HashMap<[u8; 32], Weak<Node>>,
    latest: VecDeque<Arc<Node>>,
    retained: HashMap<usize, CachedNode>,
    bytes: usize,
}

struct CachedNode {
    references: usize,
    root: bool,
}

/// Modeled bytes of retained nodes and their ownership index.
pub(super) const CACHE_BYTES: usize = 64 * 1024 * 1024;
/// A single root's conservative node and ownership-index allowance per entry.
const ENTRY_BYTES: usize = 512;
/// Node names the cache tracks before it drops those nothing holds.
const CACHE_NODES: usize = 1 << 18;

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

fn table_bytes<K, V>(table: &HashMap<K, V>) -> usize {
    if table.capacity() == 0 {
        return 0;
    }
    table.capacity().next_power_of_two() * (size_of::<(K, V)>() + 1) + 64
}

#[cfg(test)]
mod tests {
    use crate::clock::store::*;
    use crate::clock::*;

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
}
