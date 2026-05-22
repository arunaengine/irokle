// SPDX-License-Identifier: MIT OR Apache-2.0
//! Integration tests for the `irokle::Event` derive macro.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, irokle::Event, Serialize, Deserialize)]
#[irokle(type_id = "test.derive.note")]
struct Note {
    text: String,
}

#[test]
fn sets_type_id() {
    let note = Note {
        text: "hello".into(),
    };

    let envelope = irokle::EventEnvelope::encode_event(&note).unwrap();
    let decoded = envelope.decode_event::<Note>().unwrap();

    assert_eq!(<Note as irokle::Event>::TYPE_ID, "test.derive.note");
    assert_eq!(decoded, note);
}

#[test]
fn supports_renamed_crate() {
    use irokle as renamed_irokle;

    #[derive(Clone, Debug, PartialEq, Eq, renamed_irokle::Event, Serialize, Deserialize)]
    #[irokle(type_id = "test.derive.renamed", crate = "renamed_irokle")]
    struct Renamed {
        value: u32,
    }

    let envelope = renamed_irokle::EventEnvelope::encode_event(&Renamed { value: 7 }).unwrap();
    let decoded = envelope.decode_event::<Renamed>().unwrap();

    assert_eq!(decoded.value, 7);
}
