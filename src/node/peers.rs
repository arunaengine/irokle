// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use smallvec::SmallVec;

use crate::{PeerId, TopicId};

use super::SHARED_OVERLAP;

/// Consecutive failed attempts a peer may collect before selection passes it over.
pub(crate) const PEER_FAILURE_LIMIT: u64 = 2;
/// Alternates reachable by rotation when the whole budget is a single peer.
const PEER_PROBE_WINDOW: usize = 4;
/// Rotation epochs between probes that retry a peer past its retry budget.
const PEER_PROBE_PERIOD: u64 = 4;

static NO_ATTEMPTS: BTreeMap<PeerId, u64> = BTreeMap::new();

/// Peers one health table tracks at once. A full table treats further peers as
/// healthy rather than growing without limit; that only costs an attempt.
#[cfg(any(feature = "iroh", test))]
const MAX_TRACKED_PEERS: usize = 1024;

/// Runtime peer reachability, updated from real attempts and shared by every
/// selection path so one layer cannot pass over a peer another layer still
/// picks. Held outside signed topic state: it is local observation, not history.
#[derive(Debug, Default)]
pub(crate) struct PeerHealthStore {
    inner: std::sync::RwLock<HealthInner>,
}

#[derive(Debug, Default)]
struct HealthInner {
    attempts: BTreeMap<PeerId, u64>,
    epoch: u64,
}

impl PeerHealthStore {
    /// Records one unreachable attempt and advances the rotation epoch, so the
    /// next selection reaches an alternate without waiting for a new publish.
    /// Returns whether selection may have moved, which every recorded failure can.
    #[cfg(any(feature = "iroh", test))]
    pub(crate) fn record_failure(&self, peer: PeerId) -> bool {
        let Ok(mut inner) = self.inner.write() else {
            return false;
        };
        let tracked = inner.attempts.len();
        let failures = match inner.attempts.get_mut(&peer) {
            Some(failures) => {
                *failures = failures.saturating_add(1);
                *failures
            }
            None if tracked < MAX_TRACKED_PEERS => {
                inner.attempts.insert(peer, 1);
                1
            }
            None => 0,
        };
        inner.epoch = inner.epoch.wrapping_add(1);
        failures > 0
    }

    /// Clears a peer's failure record once an attempt reaches it again; with no
    /// peer failing the rotation epoch resets to the policy's preferred order.
    /// Returns whether the peer had failures, so selection may move back to it.
    #[cfg(any(feature = "iroh", test))]
    pub(crate) fn record_success(&self, peer: &PeerId) -> bool {
        let Ok(mut inner) = self.inner.write() else {
            return false;
        };
        let failed = inner.attempts.remove(peer).is_some();
        if inner.attempts.is_empty() {
            inner.epoch = 0;
        }
        failed
    }

    /// Runs `select` against the current view. [`PeerHealth`] borrows the
    /// table, so the read guard has to outlive the selection.
    pub(crate) fn with_view<R>(&self, select: impl FnOnce(PeerHealth<'_>) -> R) -> R {
        match self.inner.read() {
            Ok(inner) => select(PeerHealth::new(&inner.attempts, inner.epoch)),
            Err(_) => select(PeerHealth::empty()),
        }
    }

    /// Consecutive failures recorded for `peer`.
    #[cfg(test)]
    pub(crate) fn failures(&self, peer: &PeerId) -> u64 {
        self.inner
            .read()
            .map(|inner| inner.attempts.get(peer).copied().unwrap_or(0))
            .unwrap_or(0)
    }
}

/// Failed attempt counts and the rotation epoch owned by the caller. Health is
/// deliberately outside signed topic state, so it arrives as a borrowed view.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PeerHealth<'a> {
    attempts: &'a BTreeMap<PeerId, u64>,
    epoch: u64,
}

impl<'a> PeerHealth<'a> {
    pub(crate) fn new(attempts: &'a BTreeMap<PeerId, u64>, epoch: u64) -> Self {
        Self { attempts, epoch }
    }

    /// View without attempt history, selecting by replication policy alone.
    pub(crate) fn empty() -> Self {
        Self::new(&NO_ATTEMPTS, 0)
    }

    fn demoted(&self, peer: &PeerId) -> bool {
        self.attempts
            .get(peer)
            .is_some_and(|failures| *failures >= PEER_FAILURE_LIMIT)
    }

    fn rotation(&self, len: usize) -> usize {
        if len == 0 {
            0
        } else {
            (self.epoch % len as u64) as usize
        }
    }

    /// Whether this epoch spends one slot retrying a peer past its retry budget.
    fn probes_demoted(&self) -> bool {
        self.epoch != 0 && self.epoch.is_multiple_of(PEER_PROBE_PERIOD)
    }
}

/// Selected sync targets, plus whether policy leaves no peer worth attempting.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PeerSelection {
    pub(crate) peers: Vec<PeerId>,
    pub(crate) blocked: bool,
}

/// Membership and policy derived candidate order for one local peer.
struct CandidateOrder {
    candidates: SmallVec<[PeerId; 16]>,
    hubs: SmallVec<[PeerId; 16]>,
    ring: SmallVec<[PeerId; 16]>,
    probe: SmallVec<[PeerId; 16]>,
}

/// Select sync targets for `local_peer` without attempt history; production
/// selection adds runtime health through [`super::Irokle::sync_peers`].
#[cfg(test)]
pub(crate) fn select_sync_peers(
    topic_id: TopicId,
    local_peer: PeerId,
    state: &crate::storage::TopicState,
) -> Vec<PeerId> {
    select_sync_targets(topic_id, local_peer, state, PeerHealth::empty()).peers
}

/// Chooses which peers to sync a topic with, within its replication policy and peer health.
#[doc = include_str!("peer_selection.md")]
pub(crate) fn select_sync_targets(
    topic_id: TopicId,
    local_peer: PeerId,
    state: &crate::storage::TopicState,
    health: PeerHealth<'_>,
) -> PeerSelection {
    let max = state.replication_policy.max_sync_peers;
    if max == 0 {
        return PeerSelection::default();
    }
    let order = build_order(topic_id, local_peer, state);
    if order.candidates.is_empty() {
        return PeerSelection::default();
    }

    let blocked = order.candidates.iter().all(|peer| health.demoted(peer));
    let peers = if order.candidates.len() <= max {
        order.candidates.to_vec()
    } else {
        let mut selected = BTreeSet::new();
        let rotation = health.rotation(order.probe.len());
        if health.probes_demoted() {
            take_slots(&mut selected, &order.probe, rotation, 1, health, true);
        }
        if max == 1 {
            take_slots(&mut selected, &order.probe, rotation, max, health, false);
        } else {
            // Leave at least one slot for a ring neighbour instead of spending
            // the whole budget on the hubs every node prefers.
            let preferred = SHARED_OVERLAP.saturating_add(1).min(max - 1);
            take_slots(&mut selected, &order.hubs, 0, preferred, health, false);
        }
        take_slots(&mut selected, &order.ring, rotation, max, health, false);
        take_slots(
            &mut selected,
            &order.candidates,
            rotation,
            max,
            health,
            true,
        );
        selected.into_iter().collect()
    };

    let selection = PeerSelection { peers, blocked };
    if selection.blocked {
        tracing::debug!(%topic_id, "no allowed sync peer is under its retry budget");
    }
    selection
}

/// Fills slots from `order` starting at `start` and wrapping once, until
/// `selected` holds `limit` peers.
fn take_slots(
    selected: &mut BTreeSet<PeerId>,
    order: &[PeerId],
    start: usize,
    limit: usize,
    health: PeerHealth<'_>,
    demoted_allowed: bool,
) {
    if order.is_empty() {
        return;
    }
    for step in 0..order.len() {
        if selected.len() >= limit {
            return;
        }
        let peer = order[(start + step) % order.len()];
        if demoted_allowed || !health.demoted(&peer) {
            selected.insert(peer);
        }
    }
}

fn build_order(
    topic_id: TopicId,
    local_peer: PeerId,
    state: &crate::storage::TopicState,
) -> CandidateOrder {
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
    let shared = PeerId::hash(b"shared");
    let mut scored = candidates
        .iter()
        .copied()
        .map(|peer| (sync_peer_score(topic_id, shared, peer), peer))
        .collect::<SmallVec<[_; 16]>>();
    scored.sort_by(|(left_score, left_peer), (right_score, right_peer)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_peer.cmp(right_peer))
    });
    let hubs: SmallVec<[PeerId; 16]> = scored.into_iter().map(|(_, peer)| peer).collect();
    let ring = ring_order(&scope, local_peer);
    let probe = probe_order(&hubs, &ring);

    CandidateOrder {
        candidates,
        hubs,
        ring,
        probe,
    }
}

/// Neighbours of `local_peer` in scope order, nearest first, alternating sides.
fn ring_order(scope: &BTreeSet<PeerId>, local_peer: PeerId) -> SmallVec<[PeerId; 16]> {
    let nodes = scope.iter().copied().collect::<SmallVec<[PeerId; 16]>>();
    let mut ring = SmallVec::new();
    let Some(local_index) = nodes.iter().position(|peer| *peer == local_peer) else {
        return ring;
    };
    let mut seen = BTreeSet::new();
    for offset in 1..nodes.len() {
        for index in [
            (local_index + offset) % nodes.len(),
            (local_index + nodes.len() - offset) % nodes.len(),
        ] {
            let peer = nodes[index];
            if peer != local_peer && seen.insert(peer) {
                ring.push(peer);
            }
        }
    }
    ring
}

/// Bounded rotation order for a single-peer budget: top hub, then neighbours.
fn probe_order(hubs: &[PeerId], ring: &[PeerId]) -> SmallVec<[PeerId; 16]> {
    let mut probe = SmallVec::new();
    for peer in hubs.iter().take(1).chain(ring.iter()) {
        if probe.len() >= PEER_PROBE_WINDOW {
            break;
        }
        if !probe.contains(peer) {
            probe.push(*peer);
        }
    }
    probe
}

fn sync_peer_score(topic_id: TopicId, local_peer: PeerId, peer: PeerId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"irokle-sync-peer-v1");
    hasher.update(topic_id.as_ref());
    hasher.update(local_peer.as_ref());
    hasher.update(peer.as_ref());
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::TopicState;
    use crate::topic::ReplicationPolicy;
    use crate::{OpId, PeerId, TopicId};

    fn peer_at(index: u8) -> PeerId {
        PeerId::hash([0x5e, 0xed, index])
    }

    /// Fanout budget, expected healthy edges, expected failover edges, saturated
    /// hub count and peak inbound after failover.
    type GraphCase = (usize, &'static [usize], &'static [usize], usize, usize);

    fn index_of(members: &[PeerId], peer: PeerId) -> usize {
        members
            .iter()
            .position(|member| *member == peer)
            .expect("peer outside the generated member set")
    }

    fn topic_state(members: &[PeerId], policy: ReplicationPolicy) -> TopicState {
        TopicState {
            topic_id: TopicId::hash(b"peer-selection"),
            event_type_id: "peers::test".into(),
            genesis: OpId::hash(b"peer-selection-genesis"),
            heads: BTreeSet::new(),
            members: members.iter().copied().collect(),
            replication_policy: policy,
            membership_controls: BTreeMap::new(),
            replication_policy_control: None,
        }
    }

    #[test]
    fn small_fanout_graph() {
        let members = (0..7_u8).map(peer_at).collect::<Vec<_>>();
        // Expected selections for this fixed member set, flattened per local
        // member 0..=6. Failover demotes the two preferred hubs at epoch 1.
        let cases: [GraphCase; 3] = [
            (1, &[3, 3, 3, 4, 3, 3, 3], &[6, 5, 6, 5, 1, 2, 0], 1, 2),
            (
                2,
                &[3, 4, 3, 4, 3, 6, 5, 4, 3, 1, 3, 2, 3, 0],
                &[5, 6, 5, 0, 5, 0, 5, 1, 5, 0, 6, 1, 5, 2],
                1,
                6,
            ),
            (
                3,
                &[
                    3, 6, 4, 3, 5, 4, 3, 6, 4, 5, 4, 1, 3, 5, 1, 3, 2, 4, 3, 0, 4,
                ],
                &[
                    5, 6, 1, 5, 6, 0, 5, 6, 0, 5, 6, 1, 5, 6, 0, 2, 6, 1, 5, 2, 1,
                ],
                2,
                6,
            ),
        ];

        for (max, healthy_edges, failover_edges, saturated_hubs, failover_peak) in cases {
            let state = topic_state(&members, ReplicationPolicy::all().with_max_sync_peers(max));
            let order = build_order(state.topic_id, members[0], &state);
            let mut attempts = BTreeMap::new();
            for peer in order.hubs.iter().take(2) {
                attempts.insert(*peer, PEER_FAILURE_LIMIT);
            }
            let mut healthy_inbound: BTreeMap<usize, usize> = BTreeMap::new();
            let mut failover_inbound: BTreeMap<usize, usize> = BTreeMap::new();

            for (position, local) in members.iter().enumerate() {
                let healthy = select_sync_peers(state.topic_id, *local, &state);
                let failover = select_sync_targets(
                    state.topic_id,
                    *local,
                    &state,
                    PeerHealth::new(&attempts, 1),
                );
                assert_eq!(healthy.len(), max);
                assert_eq!(failover.peers.len(), max);
                assert!(!failover.blocked);
                for peer in &failover.peers {
                    assert!(!attempts.contains_key(peer), "max {max} local {position}");
                }
                let slot = position * max..(position + 1) * max;
                let healthy = healthy
                    .iter()
                    .map(|peer| index_of(&members, *peer))
                    .collect::<Vec<_>>();
                let failover = failover
                    .peers
                    .iter()
                    .map(|peer| index_of(&members, *peer))
                    .collect::<Vec<_>>();
                assert_eq!(healthy, healthy_edges[slot.clone()], "max {max}");
                assert_eq!(failover, failover_edges[slot], "max {max}");
                for peer in healthy {
                    *healthy_inbound.entry(peer).or_default() += 1;
                }
                for peer in failover {
                    *failover_inbound.entry(peer).or_default() += 1;
                }
            }

            let full = members.len() - 1;
            let saturated = healthy_inbound.values().filter(|c| **c == full).count();
            assert_eq!(saturated, saturated_hubs, "max {max}");
            assert_eq!(
                failover_inbound.values().copied().max(),
                Some(failover_peak)
            );
        }
    }

    #[test]
    fn demotes_preferred_peer() {
        let members = (0..9_u8).map(peer_at).collect::<Vec<_>>();
        let state = topic_state(&members, ReplicationPolicy::all().with_max_sync_peers(3));
        let local = members[0];
        let baseline = select_sync_peers(state.topic_id, local, &state);
        let order = build_order(state.topic_id, local, &state);
        let lost = order.hubs[0];
        assert!(baseline.contains(&lost));

        let mut attempts = BTreeMap::new();
        attempts.insert(lost, PEER_FAILURE_LIMIT);
        let selection =
            select_sync_targets(state.topic_id, local, &state, PeerHealth::new(&attempts, 1));
        assert!(!selection.peers.contains(&lost));
        assert_eq!(selection.peers.len(), 3);
        assert!(!selection.blocked);
        let scope = &state.members;
        assert!(
            selection
                .peers
                .iter()
                .all(|peer| scope.contains(peer) && *peer != local)
        );
    }

    #[test]
    fn demotes_ring_neighbour() {
        let members = (0..9_u8).map(peer_at).collect::<Vec<_>>();
        let state = topic_state(&members, ReplicationPolicy::all().with_max_sync_peers(3));
        let local = members[4];
        let baseline = select_sync_peers(state.topic_id, local, &state);
        let order = build_order(state.topic_id, local, &state);
        let ring = order.ring.iter().find(|peer| baseline.contains(peer));
        let neighbour = *ring.expect("ring slot is part of the budget");

        let mut attempts = BTreeMap::new();
        attempts.insert(neighbour, PEER_FAILURE_LIMIT + 3);
        let selection =
            select_sync_targets(state.topic_id, local, &state, PeerHealth::new(&attempts, 2));
        assert!(!selection.peers.contains(&neighbour));
        assert_eq!(selection.peers.len(), 3);
        assert!(!selection.blocked);
        assert!(selection.peers.iter().all(|peer| *peer != local));
    }

    #[test]
    fn restriction_reports_blocked() {
        let members = (0..6_u8).map(peer_at).collect::<Vec<_>>();
        let allowed = [members[2], members[3]];
        let local = members[0];
        let mut attempts = BTreeMap::new();
        attempts.insert(allowed[0], PEER_FAILURE_LIMIT + 5);
        attempts.insert(allowed[1], PEER_FAILURE_LIMIT);
        for max in [1_usize, 3] {
            let state = topic_state(
                &members,
                ReplicationPolicy::selected(allowed).with_max_sync_peers(max),
            );
            let healthy = select_sync_peers(state.topic_id, local, &state);
            assert!(
                healthy.iter().all(|peer| allowed.contains(peer)),
                "max {max}"
            );
            let selection =
                select_sync_targets(state.topic_id, local, &state, PeerHealth::new(&attempts, 3));
            assert!(selection.blocked, "max {max}");
            assert!(!selection.peers.is_empty(), "max {max}");
            assert!(selection.peers.len() <= max, "max {max}");
            assert!(
                selection.peers.iter().all(|peer| allowed.contains(peer)),
                "max {max}"
            );
        }
    }

    #[test]
    fn damps_single_failure() {
        let members = (0..9_u8).map(peer_at).collect::<Vec<_>>();
        let state = topic_state(&members, ReplicationPolicy::all().with_max_sync_peers(3));
        let local = members[2];
        let baseline = select_sync_peers(state.topic_id, local, &state);
        let order = build_order(state.topic_id, local, &state);
        let shaky = order.hubs[0];

        let mut attempts = BTreeMap::new();
        attempts.insert(shaky, PEER_FAILURE_LIMIT - 1);
        let damped =
            select_sync_targets(state.topic_id, local, &state, PeerHealth::new(&attempts, 0));
        assert_eq!(damped.peers, baseline);
        assert!(!damped.blocked);

        attempts.insert(shaky, PEER_FAILURE_LIMIT);
        let moved =
            select_sync_targets(state.topic_id, local, &state, PeerHealth::new(&attempts, 0));
        assert!(!moved.peers.contains(&shaky));
        assert_eq!(moved.peers.len(), 3);
    }

    #[test]
    fn probe_retries_demoted() {
        let members = (0..8_u8).map(peer_at).collect::<Vec<_>>();
        let state = topic_state(&members, ReplicationPolicy::all().with_max_sync_peers(1));
        let local = members[5];
        let order = build_order(state.topic_id, local, &state);
        let hub = order.hubs[0];
        assert_eq!(select_sync_peers(state.topic_id, local, &state), vec![hub]);

        let mut attempts = BTreeMap::new();
        attempts.insert(hub, PEER_FAILURE_LIMIT);
        let mut alternates = BTreeSet::new();
        for epoch in 1..PEER_PROBE_PERIOD {
            let health = PeerHealth::new(&attempts, epoch);
            let selection = select_sync_targets(state.topic_id, local, &state, health);
            assert_eq!(selection.peers.len(), 1, "epoch {epoch}");
            assert!(!selection.peers.contains(&hub), "epoch {epoch}");
            assert!(!selection.blocked, "epoch {epoch}");
            alternates.extend(selection.peers);
        }
        assert!(alternates.len() > 1);

        let health = PeerHealth::new(&attempts, PEER_PROBE_PERIOD);
        let probe = select_sync_targets(state.topic_id, local, &state, health);
        assert_eq!(probe.peers, vec![hub]);
    }
}
