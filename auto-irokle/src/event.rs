// SPDX-License-Identifier: MIT OR Apache-2.0

use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{AutoIrokle, encode_value};

pub type Path = Vec<String>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AutoPatch {
    Init { value: Vec<u8> },
    Patch { ops: Vec<PatchOp> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PatchOp {
    Set {
        path: Path,
        value: Vec<u8>,
    },
    SetInsert {
        path: Path,
        value: Vec<u8>,
    },
    SetRemove {
        path: Path,
        value: Vec<u8>,
    },
    MapSet {
        path: Path,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    MapRemove {
        path: Path,
        key: Vec<u8>,
    },
}

impl PatchOp {
    pub fn set(path: Path, value: Vec<u8>) -> Self {
        Self::Set { path, value }
    }

    pub fn set_insert(path: Path, value: Vec<u8>) -> Self {
        Self::SetInsert { path, value }
    }

    pub fn set_remove(path: Path, value: Vec<u8>) -> Self {
        Self::SetRemove { path, value }
    }

    pub fn map_set(path: Path, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self::MapSet { path, key, value }
    }

    pub fn map_remove(path: Path, key: Vec<u8>) -> Self {
        Self::MapRemove { path, key }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoEvent<T: AutoIrokle> {
    patch: AutoPatch,
    _marker: PhantomData<fn() -> T>,
}

impl<T: AutoIrokle> AutoEvent<T> {
    pub fn init(value: &T) -> irokle::Result<Self> {
        Ok(Self::new(AutoPatch::Init {
            value: encode_value(value)?,
        }))
    }

    pub fn patch(ops: Vec<PatchOp>) -> Self {
        Self::new(AutoPatch::Patch { ops })
    }

    pub fn body(&self) -> &AutoPatch {
        &self.patch
    }

    fn new(patch: AutoPatch) -> Self {
        Self {
            patch,
            _marker: PhantomData,
        }
    }
}

impl<T: AutoIrokle> Serialize for AutoEvent<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.patch.serialize(serializer)
    }
}

impl<'de, T: AutoIrokle> Deserialize<'de> for AutoEvent<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        AutoPatch::deserialize(deserializer).map(Self::new)
    }
}

impl<T: AutoIrokle> irokle::Event for AutoEvent<T> {
    const TYPE_ID: &'static str = T::EVENT_TYPE_ID;
}
