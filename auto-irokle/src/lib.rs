// SPDX-License-Identifier: MIT OR Apache-2.0
//! Autosurgeon-style document layer on top of Irokle topics.

mod doc;
mod event;
mod projection;

use serde::{Serialize, de::DeserializeOwned};

pub use auto_irokle_derive::AutoIrokle;
pub use doc::{AutoDoc, AutoIrokleExt};
pub use event::{AutoEvent, AutoPatch, PatchOp, Path};
pub use projection::{AutoProjection, RegisterMeta};

pub use facet;
pub use irokle;

pub trait AutoIrokle:
    Clone + PartialEq + Serialize + DeserializeOwned + facet::Facet<'static> + Send + Sync + 'static
{
    const TYPE_ID: &'static str;
    const EVENT_TYPE_ID: &'static str;

    fn diff(old: &Self, new: &Self) -> irokle::Result<Vec<PatchOp>>;

    fn apply_init(
        projection: &mut AutoProjection<Self>,
        value: Self,
        meta: &irokle::reducer::OpMeta,
    ) -> irokle::Result<()>;

    fn apply_patch_op(
        projection: &mut AutoProjection<Self>,
        op: &PatchOp,
        meta: &irokle::reducer::OpMeta,
    ) -> irokle::Result<()>;
}

pub fn encode_value<T: Serialize + ?Sized>(value: &T) -> irokle::Result<Vec<u8>> {
    postcard::to_allocvec(value).map_err(Into::into)
}

pub fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> irokle::Result<T> {
    postcard::from_bytes(bytes).map_err(|err| irokle::Error::Decode(err.to_string()))
}

pub fn field_path(name: &str) -> Path {
    vec![name.to_owned()]
}

pub fn path_is(path: &[String], segments: &[&str]) -> bool {
    path.len() == segments.len()
        && path
            .iter()
            .zip(segments.iter())
            .all(|(actual, expected)| actual == expected)
}
