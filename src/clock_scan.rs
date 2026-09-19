//! Bounded validation of node records encountered by a frozen namespace scan.

use std::collections::HashMap;

use crate::clock::{
    ActorClock, ActorId, CACHE_BYTES, ClockCache, corrupt, digest, first_difference, nibble,
};

#[derive(serde::Deserialize)]
enum Prefix {
    Leaf {
        actor: ActorId,
        _seq: u64,
    },
    Branch {
        level: u8,
        bitmap: u16,
        len: u64,
        key: ActorId,
    },
}

struct Checked {
    key: ActorId,
    len: u64,
    level: u8,
    bitmap: u16,
    children: std::ops::Range<usize>,
}

pub(crate) struct ClockScan {
    nodes: Option<HashMap<[u8; 32], Checked>>,
    children: Vec<[u8; 32]>,
    limit: usize,
    complete: bool,
    fallback: ClockCache,
}

impl Default for ClockScan {
    fn default() -> Self {
        Self {
            nodes: Some(HashMap::new()),
            children: Vec::new(),
            limit: CACHE_BYTES,
            complete: false,
            fallback: ClockCache::default(),
        }
    }
}

impl ClockScan {
    fn bound(entries: usize, buffers: usize) -> usize {
        // Cover the old and new hash tables during growth, with allocator slack.
        entries
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX)
            .saturating_mul(4 * (size_of::<([u8; 32], Checked)>() + 1))
            .saturating_add(buffers)
            .saturating_add(256)
    }

    pub(crate) fn push(&mut self, hash: &[u8; 32], bytes: &[u8]) -> crate::Result<()> {
        if digest(bytes) != *hash {
            return Err(corrupt());
        }
        let Some(nodes) = &mut self.nodes else {
            return Ok(());
        };
        if nodes.contains_key(hash) {
            return Ok(());
        }
        if self.complete {
            return Err(corrupt());
        }
        let (prefix, rest): (Prefix, _) = postcard::take_from_bytes(bytes)?;
        let (key, len, level, bitmap, children) = match prefix {
            Prefix::Leaf { actor, .. } => (actor, 1, 64, 0, &[][..]),
            Prefix::Branch {
                level,
                bitmap,
                len,
                key,
            } => {
                let (count, rest): (usize, _) = postcard::take_from_bytes(rest)?;
                if level >= 64
                    || !(2..=16).contains(&count)
                    || bitmap.count_ones() as usize != count
                {
                    return Err(corrupt());
                }
                let children = rest
                    .get(..count * 32)
                    .ok_or_else(corrupt)?
                    .as_chunks::<32>()
                    .0;
                (key, len, level, bitmap, children)
            }
        };
        let needed = self.children.len() + children.len();
        let capacity = if needed > self.children.capacity() {
            needed.max(self.children.capacity() * 2).max(4)
        } else {
            self.children.capacity()
        };
        let buffers = capacity.saturating_mul(3 * 32).saturating_add(32);
        if Self::bound(nodes.len() + 1, buffers) > self.limit {
            // Drop collected records before the existing bounded cache is used.
            self.nodes = None;
            self.children = Vec::new();
            return Ok(());
        }
        let start = self.children.len();
        self.children.extend_from_slice(children);
        nodes.insert(
            *hash,
            Checked {
                key,
                len,
                level,
                bitmap,
                children: start..self.children.len(),
            },
        );
        Ok(())
    }

    pub(crate) fn validate(
        &mut self,
        root: &[u8; 32],
        fetch: impl FnMut(&[u8; 32]) -> crate::Result<Option<Vec<u8>>>,
    ) -> crate::Result<()> {
        let Some(nodes) = &self.nodes else {
            let clock = ActorClock::load(root, &self.fallback, fetch)?;
            self.fallback.keep(&clock);
            return Ok(());
        };
        if !self.complete {
            for node in nodes.values().filter(|node| node.level < 64) {
                let (mut bitmap, mut len, mut previous) = (0_u16, 0_u64, None);
                for hash in &self.children[node.children.clone()] {
                    let child = nodes.get(hash).ok_or_else(corrupt)?;
                    if child.level <= node.level
                        || first_difference(&child.key, &node.key).is_some_and(|at| at < node.level)
                    {
                        return Err(corrupt());
                    }
                    let digit = nibble(&child.key, node.level);
                    if previous.is_some_and(|before| before >= digit) {
                        return Err(corrupt());
                    }
                    previous = Some(digit);
                    bitmap |= 1 << digit;
                    len = len.checked_add(child.len).ok_or_else(corrupt)?;
                }
                if bitmap != node.bitmap || len != node.len {
                    return Err(corrupt());
                }
            }
            self.complete = true;
        }
        if !nodes.contains_key(root) {
            return Err(corrupt());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.nodes.as_ref().map_or_else(
            || self.fallback.bytes(),
            |nodes| Self::bound(nodes.len(), self.children.capacity() * 3 * 32 + 32),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::clock::Encoded;
    use crate::clock::scan::*;
    use std::collections::BTreeMap;

    fn history() -> (BTreeMap<[u8; 32], Vec<u8>>, Vec<ActorClock>) {
        let mut records = BTreeMap::new();
        let mut roots = Vec::new();
        let mut clock = ActorClock::new();
        for index in 0_u32..1024 {
            clock.observe(ActorId::hash(index.to_le_bytes()), u64::from(index % 7));
            records.extend(
                clock
                    .unstored_nodes(|hash| Ok(records.contains_key(hash)))
                    .unwrap(),
            );
            if [31, 32, 33, 255, 256, 257, 1024].contains(&(index + 1)) {
                roots.push(clock.clone());
            }
        }
        (records, roots)
    }

    #[test]
    fn scan_matches_cache() {
        let (records, roots) = history();
        let mut scan = ClockScan::default();
        for (hash, bytes) in &records {
            scan.push(hash, bytes).unwrap();
        }
        assert!(scan.bytes() <= CACHE_BYTES);
        for clock in roots {
            let root = clock.root_hash().unwrap();
            scan.validate(&root, |_| {
                panic!("collected nodes must need no point reads")
            })
            .unwrap();
            let loaded = ActorClock::load(&root, &ClockCache::default(), |hash| {
                Ok(records.get(hash).cloned())
            })
            .unwrap();
            assert_eq!(loaded, clock);
            assert_eq!(
                postcard::to_allocvec(&loaded).unwrap(),
                postcard::to_allocvec(&clock).unwrap()
            );
        }
    }

    #[test]
    fn scan_falls_back() {
        let (records, roots) = history();
        let mut scan = ClockScan {
            limit: 4096,
            ..ClockScan::default()
        };
        for (hash, bytes) in &records {
            scan.push(hash, bytes).unwrap();
        }
        assert!(scan.nodes.is_none());
        let mut reads = 0;
        for clock in roots {
            scan.validate(&clock.root_hash().unwrap(), |hash| {
                reads += 1;
                Ok(records.get(hash).cloned())
            })
            .unwrap();
        }
        assert!(reads > 0);
        assert!(scan.bytes() <= CACHE_BYTES);
        let (hash, bytes) = records.first_key_value().unwrap();
        let mut damaged = bytes.clone();
        damaged[0] ^= 1;
        assert!(scan.push(hash, &damaged).is_err());
    }

    #[test]
    fn scan_refuses_damage() {
        let (records, roots) = history();
        let root = roots.last().unwrap().root_hash().unwrap();
        let mut damaged = records[&root].clone();
        damaged[3] ^= 1;
        assert!(ClockScan::default().push(&root, &damaged).is_err());
        for kind in 0..6 {
            let mut stored = records.clone();
            let mut encoded: Encoded = postcard::from_bytes(&stored[&root]).unwrap();
            if kind == 4 {
                encoded = records
                    .values()
                    .find_map(
                        |bytes| match postcard::from_bytes::<Encoded>(bytes).unwrap() {
                            node @ Encoded::Branch { level: 1.., .. } => Some(node),
                            _ => None,
                        },
                    )
                    .unwrap();
            }
            let Encoded::Branch {
                level,
                len,
                key,
                bitmap,
                children,
                ..
            } = &mut encoded
            else {
                panic!("branch required")
            };
            match kind {
                0 => {
                    stored.remove(&children[0]);
                }
                1 => children.swap(0, 1),
                2 => *len += 1,
                3 => *level = 64,
                4 => {
                    let mut wrong: [u8; 32] = key.as_ref().try_into().unwrap();
                    wrong[0] ^= 0x80;
                    *key = ActorId::from_bytes(wrong);
                }
                5 => *bitmap ^= 1,
                _ => unreachable!(),
            }
            let bytes = postcard::to_allocvec(&encoded).unwrap();
            let changed = digest(&bytes);
            stored.insert(changed, bytes);
            let mut scan = ClockScan::default();
            let result = stored
                .iter()
                .try_for_each(|(hash, bytes)| scan.push(hash, bytes))
                .and_then(|()| scan.validate(&changed, |_| panic!("unexpected point read")));
            assert!(result.is_err());
            assert!(
                ActorClock::load(&changed, &ClockCache::default(), |hash| Ok(stored
                    .get(hash)
                    .cloned()))
                .is_err()
            );
        }
    }
}
