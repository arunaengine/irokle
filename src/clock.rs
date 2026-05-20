use crate::ids::ActorId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorClock {
    entries: BTreeMap<ActorId, u64>,
}

impl ActorClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, actor: &ActorId) -> u64 {
        self.entries.get(actor).copied().unwrap_or_default()
    }

    pub fn advance(&mut self, actor: ActorId) -> u64 {
        let next = self.get(&actor).saturating_add(1);
        self.entries.insert(actor, next);
        next
    }

    pub fn observe(&mut self, actor: ActorId, seq: u64) {
        let current = self.entries.entry(actor).or_default();
        *current = (*current).max(seq);
    }

    pub fn merge(&mut self, other: &Self) {
        for (actor, counter) in &other.entries {
            let current = self.entries.entry(*actor).or_default();
            *current = (*current).max(*counter);
        }
    }

    pub fn dominates(&self, other: &Self) -> bool {
        other
            .entries
            .iter()
            .all(|(actor, counter)| self.get(actor) >= *counter)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ActorId, &u64)> {
        self.entries.iter()
    }
}
