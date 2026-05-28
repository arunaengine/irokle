// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{__private, AutoCrdt, AutoEvent, AutoIrokle, AutoPatch, Path, decode_value};
use irokle::reducer::OpMeta;
use irokle::{ActorClock, ActorId, OpId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

    fn is_fully_observed(&self, stability: &ActorClock) -> bool {
        self.add_dots
            .iter()
            .all(|dot| stability.get(&dot.actor_id) >= dot.actor_seq)
            && self
                .remove_clock
                .iter()
                .all(|(actor, seq)| stability.get(actor) >= *seq)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

    fn is_fully_observed(&self, stability: &ActorClock) -> bool {
        self.add_dots
            .iter()
            .all(|dot| stability.get(&dot.actor_id) >= dot.actor_seq)
            && self
                .remove_clock
                .iter()
                .all(|(actor, seq)| stability.get(actor) >= *seq)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectionMeta {
    registers: BTreeMap<Path, RegisterMeta>,
    sets: BTreeMap<Path, BTreeMap<Vec<u8>, SetEntry>>,
    maps: BTreeMap<Path, BTreeMap<Vec<u8>, MapEntry>>,
}

impl ProjectionMeta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.registers.clear();
        self.sets.clear();
        self.maps.clear();
    }

    pub fn gc(&mut self, stability: &ActorClock) -> usize {
        let mut pruned = 0;
        for entries in self.sets.values_mut() {
            entries.retain(|_, entry| {
                if !entry.is_visible() && entry.is_fully_observed(stability) {
                    pruned += 1;
                    false
                } else {
                    true
                }
            });
        }
        for entries in self.maps.values_mut() {
            entries.retain(|_, entry| {
                if !entry.is_visible() && entry.is_fully_observed(stability) {
                    pruned += 1;
                    false
                } else {
                    true
                }
            });
        }
        pruned
    }

    #[doc(hidden)]
    pub fn init_register(&mut self, path: Path, meta: &OpMeta) {
        self.registers
            .insert(path, RegisterMeta::from_op_meta(meta));
    }

    #[doc(hidden)]
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

    #[doc(hidden)]
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

    #[doc(hidden)]
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

    #[doc(hidden)]
    pub fn insert_set_value(&mut self, path: Path, value: Vec<u8>, meta: &OpMeta) -> bool {
        let dot = Dot::from_meta(meta);
        let entry = self.sets.entry(path).or_default().entry(value).or_default();
        entry.add_dots.insert(dot);
        entry.is_visible()
    }

    #[doc(hidden)]
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

    #[doc(hidden)]
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

    #[doc(hidden)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: serde::de::DeserializeOwned"
))]
pub struct AutoProjection<T: AutoCrdt> {
    state: Option<T>,
    meta: ProjectionMeta,
    #[serde(default)]
    applied_ops: BTreeSet<OpId>,
}

impl<T: AutoCrdt> Default for AutoProjection<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: AutoCrdt> AutoProjection<T> {
    pub fn new() -> Self {
        Self {
            state: None,
            meta: ProjectionMeta::new(),
            applied_ops: BTreeSet::new(),
        }
    }

    pub fn state(&self) -> irokle::Result<&T> {
        self.state.as_ref().ok_or_else(missing_init)
    }

    #[doc(hidden)]
    pub fn state_opt(&self) -> Option<&T> {
        self.state.as_ref()
    }

    pub fn gc(&mut self, stability: &ActorClock) -> usize {
        self.meta.gc(stability)
    }
}

impl<T: AutoIrokle> AutoProjection<T> {
    pub fn apply(
        &mut self,
        record: &irokle::reducer::EventRecord<AutoEvent<T>>,
    ) -> irokle::Result<()> {
        if !self.applied_ops.insert(record.meta.op_id) {
            return Ok(());
        }
        match record.event.body() {
            AutoPatch::Init { value } => {
                if self.state.is_some() {
                    __private::log_replayed_init(T::EVENT_TYPE_ID);
                    return Ok(());
                }
                let value: T = decode_value(value)?;
                self.state = Some(value);
                self.meta.clear();
                let state = self.state.as_mut().expect("just set");
                T::init_into(&[], state, &mut self.meta, &record.meta)
            }
            AutoPatch::Patch { ops } => {
                let state = self.state.as_mut().ok_or_else(missing_init)?;
                for op in ops {
                    let matched = T::apply_into(&[], state, &mut self.meta, op, &record.meta)?;
                    if !matched {
                        __private::log_unsupported_patch_op(T::EVENT_TYPE_ID);
                        return Err(irokle::Error::Decode(format!(
                            "unsupported auto-irokle patch op for {}",
                            T::EVENT_TYPE_ID,
                        )));
                    }
                }
                Ok(())
            }
        }
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
