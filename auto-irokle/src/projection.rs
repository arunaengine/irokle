// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use crate::{AutoEvent, AutoIrokle, AutoPatch, Path, decode_value};
use irokle::reducer::OpMeta;
use irokle::{ActorClock, ActorId, OpId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct Dot {
    actor_id: ActorId,
    actor_seq: u64,
}

impl Dot {
    fn from_meta(meta: &OpMeta) -> Self {
        Self {
            actor_id: meta.actor_id,
            actor_seq: meta.actor_seq,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterMeta {
    dot: Dot,
    observed_clock: ActorClock,
    op_id: OpId,
}

impl RegisterMeta {
    pub fn from_op_meta(meta: &OpMeta) -> Self {
        Self {
            dot: Dot::from_meta(meta),
            observed_clock: meta.observed_clock.clone(),
            op_id: meta.op_id,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SetEntry {
    add_dots: BTreeSet<Dot>,
    remove_clock: ActorClock,
}

impl SetEntry {
    fn is_visible(&self) -> bool {
        self.add_dots
            .iter()
            .any(|dot| self.remove_clock.get(&dot.actor_id) < dot.actor_seq)
    }

    fn prune_removed(&mut self) {
        self.add_dots
            .retain(|dot| self.remove_clock.get(&dot.actor_id) < dot.actor_seq);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MapEntry {
    add_dots: BTreeSet<Dot>,
    remove_clock: ActorClock,
    value: Option<RegisterMeta>,
}

impl MapEntry {
    fn is_visible(&self) -> bool {
        self.add_dots
            .iter()
            .any(|dot| self.remove_clock.get(&dot.actor_id) < dot.actor_seq)
    }

    fn prune_removed(&mut self) {
        self.add_dots
            .retain(|dot| self.remove_clock.get(&dot.actor_id) < dot.actor_seq);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoProjection<T: AutoIrokle> {
    state: Option<T>,
    registers: BTreeMap<Path, RegisterMeta>,
    sets: BTreeMap<Path, BTreeMap<Vec<u8>, SetEntry>>,
    maps: BTreeMap<Path, BTreeMap<Vec<u8>, MapEntry>>,
}

impl<T: AutoIrokle> Default for AutoProjection<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: AutoIrokle> AutoProjection<T> {
    pub fn new() -> Self {
        Self {
            state: None,
            registers: BTreeMap::new(),
            sets: BTreeMap::new(),
            maps: BTreeMap::new(),
        }
    }

    pub fn state(&self) -> irokle::Result<&T> {
        self.state.as_ref().ok_or_else(missing_init)
    }

    pub fn state_mut(&mut self) -> irokle::Result<&mut T> {
        self.state.as_mut().ok_or_else(missing_init)
    }

    pub fn replace_state(&mut self, state: T) {
        self.state = Some(state);
        self.registers.clear();
        self.sets.clear();
        self.maps.clear();
    }

    pub fn apply(
        &mut self,
        record: &irokle::reducer::EventRecord<AutoEvent<T>>,
    ) -> irokle::Result<()> {
        match record.event.body() {
            AutoPatch::Init { value } => {
                let value = decode_value(value)?;
                T::apply_init(self, value, &record.meta)
            }
            AutoPatch::Patch { ops } => {
                for op in ops {
                    T::apply_patch_op(self, op, &record.meta)?;
                }
                Ok(())
            }
        }
    }

    pub fn init_register(&mut self, path: Path, meta: &OpMeta) {
        self.registers
            .insert(path, RegisterMeta::from_op_meta(meta));
    }

    pub fn init_set_values<I>(&mut self, path: Path, values: I, meta: &OpMeta)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let dot = Dot::from_meta(meta);
        let values = values
            .into_iter()
            .map(|value| {
                let mut entry = SetEntry::default();
                entry.add_dots.insert(dot);
                (value, entry)
            })
            .collect();
        self.sets.insert(path, values);
    }

    pub fn init_map_keys<I>(&mut self, path: Path, keys: I, meta: &OpMeta)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let dot = Dot::from_meta(meta);
        let value = RegisterMeta::from_op_meta(meta);
        let keys = keys
            .into_iter()
            .map(|key| {
                let mut entry = MapEntry {
                    value: Some(value.clone()),
                    ..MapEntry::default()
                };
                entry.add_dots.insert(dot);
                (key, entry)
            })
            .collect();
        self.maps.insert(path, keys);
    }

    pub fn apply_register(&mut self, path: Path, meta: &OpMeta) -> bool {
        let incoming = RegisterMeta::from_op_meta(meta);
        if self
            .registers
            .get(&path)
            .is_none_or(|current| incoming_register_wins(current, &incoming))
        {
            self.registers.insert(path, incoming);
            true
        } else {
            false
        }
    }

    pub fn insert_set_value(&mut self, path: Path, value: Vec<u8>, meta: &OpMeta) -> bool {
        let dot = Dot::from_meta(meta);
        let entry = self.sets.entry(path).or_default().entry(value).or_default();
        entry.add_dots.insert(dot);
        entry.is_visible()
    }

    pub fn remove_set_value(&mut self, path: Path, value: &[u8], meta: &OpMeta) -> bool {
        let entry = self
            .sets
            .entry(path)
            .or_default()
            .entry(value.to_vec())
            .or_default();
        entry.remove_clock.merge(&meta.observed_clock);
        entry.prune_removed();
        entry.is_visible()
    }

    pub fn set_map_value(&mut self, path: Path, key: Vec<u8>, meta: &OpMeta) -> bool {
        let dot = Dot::from_meta(meta);
        let incoming = RegisterMeta::from_op_meta(meta);
        let entry = self.maps.entry(path).or_default().entry(key).or_default();
        let was_visible = entry.is_visible();
        entry.add_dots.insert(dot);
        let value_wins = !was_visible
            || entry
                .value
                .as_ref()
                .is_none_or(|current| incoming_register_wins(current, &incoming));
        if value_wins {
            entry.value = Some(incoming);
        }
        entry.is_visible() && value_wins
    }

    pub fn remove_map_key(&mut self, path: Path, key: &[u8], meta: &OpMeta) -> bool {
        let entry = self
            .maps
            .entry(path)
            .or_default()
            .entry(key.to_vec())
            .or_default();
        entry.remove_clock.merge(&meta.observed_clock);
        entry.prune_removed();
        entry.is_visible()
    }
}

fn incoming_register_wins(current: &RegisterMeta, incoming: &RegisterMeta) -> bool {
    if current.op_id == incoming.op_id {
        return false;
    }

    let incoming_observes_current =
        incoming.observed_clock.get(&current.dot.actor_id) >= current.dot.actor_seq;
    let current_observes_incoming =
        current.observed_clock.get(&incoming.dot.actor_id) >= incoming.dot.actor_seq;

    match (incoming_observes_current, current_observes_incoming) {
        (true, false) => true,
        (false, true) => false,
        _ => incoming.op_id > current.op_id,
    }
}

fn missing_init() -> irokle::Error {
    irokle::Error::Decode("auto-irokle document has no init event".into())
}
