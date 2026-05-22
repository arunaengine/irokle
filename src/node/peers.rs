// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeSet;

use smallvec::SmallVec;

use crate::{PeerId, TopicId};

use super::SYNC_PEER_SHARED_OVERLAP;

pub(crate) fn select_sync_peers(
    topic_id: TopicId,
    local_peer: PeerId,
    state: &crate::storage::TopicState,
) -> Vec<PeerId> {
    let max = state.replication_policy.max_sync_peers;
    if max == 0 {
        return Vec::new();
    }

    let mut scope = if state.replication_policy.selected_peers.is_empty() {
        state.members.clone()
    } else {
        state
            .replication_policy
            .selected_peers
            .intersection(&state.members)
            .copied()
            .collect()
    };
    scope.insert(local_peer);

    let candidates: SmallVec<[PeerId; 16]> = scope
        .iter()
        .copied()
        .filter(|peer| *peer != local_peer)
        .collect();
    if candidates.len() <= max {
        return candidates.into_vec();
    }

    let mut selected = BTreeSet::new();
    let mut shared = candidates
        .iter()
        .copied()
        .map(|peer| {
            (
                sync_peer_score(topic_id, PeerId::hash(b"shared"), peer),
                peer,
            )
        })
        .collect::<SmallVec<[_; 16]>>();
    shared.sort_by(|(left_score, left_peer), (right_score, right_peer)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_peer.cmp(right_peer))
    });
    let shared_budget = SYNC_PEER_SHARED_OVERLAP
        .saturating_add(1)
        .min(max)
        .min(candidates.len());
    for (_, peer) in shared.into_iter().take(shared_budget) {
        selected.insert(peer);
        if selected.len() >= max {
            break;
        }
    }

    let ring = scope.into_iter().collect::<SmallVec<[PeerId; 16]>>();
    if let Some(local_index) = ring.iter().position(|peer| *peer == local_peer) {
        for offset in 1..ring.len() {
            if selected.len() >= max {
                break;
            }
            selected.insert(ring[(local_index + offset) % ring.len()]);
            if selected.len() >= max {
                break;
            }
            selected.insert(ring[(local_index + ring.len() - offset) % ring.len()]);
        }
    }

    let mut remaining = candidates
        .into_iter()
        .filter(|peer| !selected.contains(peer))
        .map(|peer| (sync_peer_score(topic_id, local_peer, peer), peer))
        .collect::<Vec<_>>();
    remaining.sort_by(|(left_score, left_peer), (right_score, right_peer)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_peer.cmp(right_peer))
    });
    for (_, peer) in remaining {
        if selected.len() >= max {
            break;
        }
        selected.insert(peer);
    }

    selected.into_iter().collect()
}

fn sync_peer_score(topic_id: TopicId, local_peer: PeerId, peer: PeerId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"irokle-sync-peer-v1");
    hasher.update(topic_id.as_ref());
    hasher.update(local_peer.as_ref());
    hasher.update(peer.as_ref());
    *hasher.finalize().as_bytes()
}
