use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdParseError {
    expected_len: usize,
}

impl fmt::Display for IdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected {} bytes of lowercase hex", self.expected_len)
    }
}

impl std::error::Error for IdParseError {}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
        )]
        pub struct $name([u8; 32]);

        impl $name {
            pub const LEN: usize = 32;

            pub const fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            pub fn hash(bytes: impl AsRef<[u8]>) -> Self {
                Self(*blake3::hash(bytes.as_ref()).as_bytes())
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for byte in &self.0 {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }

        impl From<[u8; 32]> for $name {
            fn from(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }
        }

        impl From<$name> for [u8; 32] {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                if input.len() != Self::LEN * 2 {
                    return Err(IdParseError {
                        expected_len: Self::LEN,
                    });
                }

                let mut bytes = [0_u8; 32];
                for (idx, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
                    let hi = hex_nibble(chunk[0]).ok_or(IdParseError {
                        expected_len: Self::LEN,
                    })?;
                    let lo = hex_nibble(chunk[1]).ok_or(IdParseError {
                        expected_len: Self::LEN,
                    })?;
                    bytes[idx] = (hi << 4) | lo;
                }
                Ok(Self(bytes))
            }
        }
    };
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

id_type!(OpId);
id_type!(TopicId);
id_type!(ActorId);
id_type!(PeerId);

pub fn actor_id_for(topic_id: TopicId, peer_id: PeerId) -> ActorId {
    ActorId::hash([topic_id.as_ref(), peer_id.as_ref()].concat())
}
