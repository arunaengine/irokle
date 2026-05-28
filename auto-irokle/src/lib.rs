// SPDX-License-Identifier: MIT OR Apache-2.0
//! Autosurgeon-style document layer on top of Irokle topics.

mod doc;
mod event;
mod projection;

use serde::{Serialize, de::DeserializeOwned};

pub use auto_irokle_derive::AutoIrokle;
pub use doc::{AutoDoc, AutoIrokleExt};
pub use event::{AutoEvent, AutoPatch, PatchOp, Path};
pub use projection::{AutoProjection, ProjectionMeta, RegisterMeta};

pub use irokle;

pub trait AutoCrdt:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    #[doc(hidden)]
    fn diff_into(
        prefix: &[String],
        old: &Self,
        new: &Self,
        ops: &mut Vec<PatchOp>,
    ) -> irokle::Result<()>;

    #[doc(hidden)]
    fn init_into(
        prefix: &[String],
        state: &Self,
        meta: &mut ProjectionMeta,
        op_meta: &irokle::reducer::OpMeta,
    ) -> irokle::Result<()>;

    #[doc(hidden)]
    fn apply_into(
        prefix: &[String],
        state: &mut Self,
        meta: &mut ProjectionMeta,
        op: &PatchOp,
        op_meta: &irokle::reducer::OpMeta,
    ) -> irokle::Result<bool>;
}

pub trait AutoIrokle: AutoCrdt {
    #[doc(hidden)]
    const EVENT_TYPE_ID: &'static str;
}

#[doc(hidden)]
pub mod __private {
    use serde::{Serialize, de::DeserializeOwned};

    pub use crate::event::{AutoEvent, AutoPatch, PatchOp, Path};
    pub use crate::projection::{AutoProjection, ProjectionMeta, RegisterMeta};
    pub use ::irokle;
    pub use ::irokle::reducer::OpMeta;

    pub fn encode_value<T: Serialize + ?Sized>(value: &T) -> irokle::Result<Vec<u8>> {
        super::encode_value(value)
    }

    pub fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> irokle::Result<T> {
        super::decode_value(bytes)
    }

    pub fn extend_prefix(prefix: &[String], segment: &str) -> Path {
        let mut out = Vec::with_capacity(prefix.len() + 1);
        out.extend_from_slice(prefix);
        out.push(segment.to_owned());
        out
    }

    pub fn path_matches(path: &[String], prefix: &[String], leaf: &str) -> bool {
        path.len() == prefix.len() + 1
            && path[..prefix.len()] == *prefix
            && path[prefix.len()] == leaf
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
