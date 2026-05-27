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

pub use irokle;

pub trait AutoIrokle:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    #[doc(hidden)]
    const EVENT_TYPE_ID: &'static str;

    #[doc(hidden)]
    fn diff(old: &Self, new: &Self) -> irokle::Result<Vec<PatchOp>>;

    #[doc(hidden)]
    fn apply_init(
        projection: &mut AutoProjection<Self>,
        value: Self,
        meta: &irokle::reducer::OpMeta,
    ) -> irokle::Result<()>;

    #[doc(hidden)]
    fn apply_patch_op(
        projection: &mut AutoProjection<Self>,
        op: &PatchOp,
        meta: &irokle::reducer::OpMeta,
    ) -> irokle::Result<()>;
}

#[doc(hidden)]
pub mod __private {
    use serde::{Serialize, de::DeserializeOwned};

    pub use crate::event::{AutoEvent, AutoPatch, PatchOp, Path};
    pub use crate::projection::{AutoProjection, RegisterMeta};
    pub use ::irokle;
    pub use ::irokle::reducer::OpMeta;

    pub fn encode_value<T: Serialize + ?Sized>(value: &T) -> irokle::Result<Vec<u8>> {
        super::encode_value(value)
    }

    pub fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> irokle::Result<T> {
        super::decode_value(bytes)
    }

    pub fn field_path(name: &str) -> Path {
        super::field_path(name)
    }

    pub fn path_is(path: &[String], segments: &[&str]) -> bool {
        super::path_is(path, segments)
    }

    pub fn log_replayed_init(type_id: &'static str) {
        tracing::warn!(
            target: "auto_irokle",
            type_id,
            "ignoring replayed AutoEvent::Init on already-initialized document"
        );
    }

    pub fn log_unsupported_patch_op(type_id: &'static str) {
        tracing::warn!(
            target: "auto_irokle",
            type_id,
            "received unsupported AutoPatch op for document type"
        );
    }
}

pub(crate) fn encode_value<T: Serialize + ?Sized>(value: &T) -> irokle::Result<Vec<u8>> {
    postcard::to_allocvec(value).map_err(Into::into)
}

pub(crate) fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> irokle::Result<T> {
    postcard::from_bytes(bytes).map_err(|err| irokle::Error::Decode(err.to_string()))
}

pub(crate) fn field_path(name: &str) -> Path {
    vec![name.to_owned()]
}

pub(crate) fn path_is(path: &[String], segments: &[&str]) -> bool {
    path.len() == segments.len()
        && path
            .iter()
            .zip(segments.iter())
            .all(|(actual, expected)| actual == expected)
}
