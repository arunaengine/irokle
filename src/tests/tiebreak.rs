use super::support::*;

use crate::TopicEviction;
use crate::oplog::Oplog;
use crate::sync::SyncEngine;

/// One forked topic after both sides have exchanged chains, normalized so
/// `winner_*` refers to the side whose genesis id is the lexicographically
/// smaller one (the side that must keep its chain).
struct Fork {
    topic_id: TopicId,
    a_won: bool,
    winner_oplog: Oplog,
    winner_signer: Ed25519Signer,
    winner_genesis: Op,
    winner_event: Op,
    winner_result: crate::Admitted,
    loser_oplog: Oplog,
    loser_signer: Ed25519Signer,
    loser_genesis: Op,
    loser_event: Op,
    loser_result: crate::Admitted,
}

fn seed_side(
    topic_id: TopicId,
    seed: u8,
    peer_seed: u8,
    text: &str,
) -> (Oplog, Ed25519Signer, Op, Op) {
    let peer = Ed25519Signer::from_bytes(&[peer_seed; 32]).peer_id();
    forked_side(MemoryStorage::new(), topic_id, seed, [peer], text)
}

fn build_fork(seed_a: u8, seed_b: u8) -> Fork {
    let topic_id = TopicId::hash(b"genesis-fork-topic");
    let (oplog_a, signer_a, g_a, e_a) = seed_side(topic_id, seed_a, seed_b, "a-branch");
    let (oplog_b, signer_b, g_b, e_b) = seed_side(topic_id, seed_b, seed_a, "b-branch");

    let result_a = oplog_a
        .receive_ops_from_peer_evicting(Some(signer_b.peer_id()), vec![g_b.clone(), e_b.clone()])
        .unwrap();
    let result_b = oplog_b
        .receive_ops_from_peer_evicting(Some(signer_a.peer_id()), vec![g_a.clone(), e_a.clone()])
        .unwrap();

    let a_won = g_a.id < g_b.id;
    if a_won {
        Fork {
            topic_id,
            a_won,
            winner_oplog: oplog_a,
            winner_signer: signer_a,
            winner_genesis: g_a,
            winner_event: e_a,
            winner_result: result_a,
            loser_oplog: oplog_b,
            loser_signer: signer_b,
            loser_genesis: g_b,
            loser_event: e_b,
            loser_result: result_b,
        }
    } else {
        Fork {
            topic_id,
            a_won,
            winner_oplog: oplog_b,
            winner_signer: signer_b,
            winner_genesis: g_b,
            winner_event: e_b,
            winner_result: result_b,
            loser_oplog: oplog_a,
            loser_signer: signer_a,
            loser_genesis: g_a,
            loser_event: e_a,
            loser_result: result_a,
        }
    }
}

fn admitted_ids(oplog: &Oplog, topic_id: &TopicId) -> BTreeSet<OpId> {
    oplog.storage().list_op_ids(topic_id).unwrap()
}

fn assert_fork_converged(fork: &Fork) {
    // Exactly one side reset: only the loser evicts.
    assert!(fork.winner_result.evictions.is_empty());
    assert_eq!(fork.loser_result.evictions.len(), 1);

    // Both sides now agree on the winning genesis and the same admitted ops.
    let winner_genesis = fork.winner_genesis.id;
    let expected: BTreeSet<OpId> = [winner_genesis, fork.winner_event.id].into();
    assert_eq!(
        fork.winner_oplog
            .storage()
            .topic_state(&fork.topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        winner_genesis
    );
    assert_eq!(
        fork.loser_oplog
            .storage()
            .topic_state(&fork.topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        winner_genesis
    );
    assert_eq!(admitted_ids(&fork.winner_oplog, &fork.topic_id), expected);
    assert_eq!(admitted_ids(&fork.loser_oplog, &fork.topic_id), expected);
}

#[test]
fn fork_resolves_to_smaller_genesis() {
    let fork = build_fork(1, 2);
    assert_fork_converged(&fork);

    // The loser's eviction carries its pre-reset payloads in order.
    let eviction: &TopicEviction = &fork.loser_result.evictions[0];
    assert_eq!(eviction.topic_id, fork.topic_id);
    assert_eq!(eviction.losing_genesis, fork.loser_genesis.id);
    assert_eq!(eviction.winning_genesis, fork.winner_genesis.id);
    assert_eq!(eviction.evicted.len(), 1);
    assert_eq!(eviction.evicted[0].op_id, fork.loser_event.id);
    assert_eq!(
        eviction.evicted[0].actor_id,
        fork.loser_event.signed.body.actor_id
    );
    assert_eq!(eviction.evicted[0].author, fork.loser_signer.peer_id());
    assert_eq!(eviction.evicted[0].actor_seq, 2);
    assert_eq!(
        eviction.evicted[0].payload,
        fork.loser_event.signed.body.payload
    );
}

#[test]
fn sync_receive_data_returns_genesis_eviction() {
    let topic_id = TopicId::hash(b"genesis-fork-sync-receive");
    let (oplog_a, signer_a, g_a, e_a) = seed_side(topic_id, 1, 2, "a-branch");
    let (oplog_b, signer_b, g_b, e_b) = seed_side(topic_id, 2, 1, "b-branch");
    let (
        loser_oplog,
        loser_peer,
        winner_peer,
        loser_genesis,
        loser_event,
        winner_genesis,
        winner_event,
    ) = if g_a.id < g_b.id {
        (
            oplog_b,
            signer_b.peer_id(),
            signer_a.peer_id(),
            g_b,
            e_b,
            g_a,
            e_a,
        )
    } else {
        (
            oplog_a,
            signer_a.peer_id(),
            signer_b.peer_id(),
            g_a,
            e_a,
            g_b,
            e_b,
        )
    };

    let engine = SyncEngine::new(loser_oplog, loser_peer);
    let (ack, evictions) = engine
        .receive_data(
            winner_peer,
            loser_peer,
            sync::SyncData {
                topic_id,
                ops: vec![winner_genesis.clone(), winner_event],
            },
        )
        .unwrap();

    assert!(ack.accepted.contains(&winner_genesis.id));
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0].losing_genesis, loser_genesis.id);
    assert_eq!(evictions[0].winning_genesis, winner_genesis.id);
    assert_eq!(evictions[0].evicted[0].op_id, loser_event.id);
}

#[test]
fn node_receive_sync_data_from_returns_genesis_eviction() {
    let topic_id = TopicId::hash(b"genesis-fork-node-receive");
    let (_, signer_a, g_a, e_a) = seed_side(topic_id, 1, 2, "a-branch");
    let (_, signer_b, g_b, e_b) = seed_side(topic_id, 2, 1, "b-branch");
    let (loser_signer, loser_genesis, loser_event, winner_signer, winner_genesis, winner_event) =
        if g_a.id < g_b.id {
            (signer_b, g_b, e_b, signer_a, g_a, e_a)
        } else {
            (signer_a, g_a, e_a, signer_b, g_b, e_b)
        };
    let node = Irokle::new(NodeConfig {
        signer: loser_signer.clone(),
        default_write_concern: WriteConcern::Local,
        ..NodeConfig::default()
    })
    .unwrap();
    node.receive_sync_data_from(
        loser_signer.peer_id(),
        sync::SyncData {
            topic_id,
            ops: vec![loser_genesis.clone(), loser_event.clone()],
        },
    )
    .unwrap();

    let (ack, evictions) = node
        .receive_sync_data_from(
            winner_signer.peer_id(),
            sync::SyncData {
                topic_id,
                ops: vec![winner_genesis.clone(), winner_event],
            },
        )
        .unwrap();

    assert!(ack.accepted.contains(&winner_genesis.id));
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0].losing_genesis, loser_genesis.id);
    assert_eq!(evictions[0].winning_genesis, winner_genesis.id);
    assert_eq!(evictions[0].evicted[0].op_id, loser_event.id);
}

#[test]
fn non_member_smaller_genesis_does_not_reset() {
    let topic_id = TopicId::hash(b"genesis-fork-topic");
    // Local chain with no other members, so its membership is exactly {local}.
    let local_signer = Ed25519Signer::from_bytes(&[1; 32]);
    let local = Oplog::with_storage(MemoryStorage::new());
    let local_actor = actor_id_for(topic_id, local_signer.peer_id());
    let local_genesis = local
        .create_topic_genesis(
            topic_id,
            local_actor,
            TopicGenesis {
                event_type_id: Note::TYPE_ID.into(),
                initial_peers: BTreeSet::new(),
                replication_policy: ReplicationPolicy::default(),
            },
            &local_signer,
        )
        .unwrap();
    let local_event = local
        .create_event_op(
            topic_id,
            local_actor,
            EventEnvelope::encode_event(&Note {
                text: "local".into(),
            })
            .unwrap(),
            &local_signer,
        )
        .unwrap();

    // A non-member foreign genesis whose id is smaller than the local one, so
    // only the membership gate — not id ordering — keeps the local chain.
    let (foreign_signer, foreign_genesis) = (2..=255_u8)
        .find_map(|seed| {
            let signer = Ed25519Signer::from_bytes(&[seed; 32]);
            let source = Oplog::with_storage(MemoryStorage::new());
            let actor = actor_id_for(topic_id, signer.peer_id());
            let genesis = source
                .create_topic_genesis(
                    topic_id,
                    actor,
                    TopicGenesis {
                        event_type_id: Note::TYPE_ID.into(),
                        initial_peers: BTreeSet::new(),
                        replication_policy: ReplicationPolicy::default(),
                    },
                    &signer,
                )
                .unwrap();
            (genesis.id < local_genesis.id).then_some((signer, genesis))
        })
        .expect("a smaller-id non-member genesis seed exists");

    let members = local
        .storage()
        .topic_state(&topic_id)
        .unwrap()
        .unwrap()
        .members;
    assert!(!members.contains(&foreign_signer.peer_id()));

    let result = local
        .receive_ops_from_peer_evicting(
            Some(foreign_signer.peer_id()),
            vec![foreign_genesis.clone()],
        )
        .unwrap();
    assert!(result.evictions.is_empty());
    assert!(result.accepted.is_empty());

    // The local chain survives untouched despite the smaller foreign id.
    assert_eq!(
        local
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        local_genesis.id
    );
    assert_eq!(
        admitted_ids(&local, &topic_id),
        [local_genesis.id, local_event.id].into()
    );
}

#[test]
fn fork_resolution_is_symmetric() {
    // Deterministic ed25519 signing makes genesis ids stable, so scanning seed
    // pairs surfaces both orderings (each physical side wins at least once).
    let mut saw_a_win = false;
    let mut saw_b_win = false;
    for peer_seed in 2..=16_u8 {
        let fork = build_fork(1, peer_seed);
        assert_fork_converged(&fork);
        if fork.a_won {
            saw_a_win = true;
        } else {
            saw_b_win = true;
        }
    }
    assert!(
        saw_a_win && saw_b_win,
        "expected the winner to fall on each physical side across seeds"
    );
}

#[test]
fn reset_completeness_lets_acks_converge() {
    let fork = build_fork(1, 2);
    assert_fork_converged(&fork);

    let topic_id = fork.topic_id;
    let winner_peer = fork.winner_signer.peer_id();
    let loser_peer = fork.loser_signer.peer_id();
    let sync_winner = SyncEngine::new(fork.winner_oplog.clone(), winner_peer);
    let sync_loser = SyncEngine::new(fork.loser_oplog.clone(), loser_peer);

    // A normal sync round in both directions. Fingerprints already match after
    // resolution, so no ops move; the point is that the signed acks validate
    // and each clock dominates the other's.
    let loser_summary = sync_loser.summary(topic_id).unwrap();
    let data_for_loser = sync_winner.plan_data(loser_peer, &loser_summary).unwrap();
    assert!(data_for_loser.ops.is_empty());
    let (mut ack_from_loser, _) = sync_loser
        .receive_data(winner_peer, loser_peer, data_for_loser)
        .unwrap();
    ack_from_loser.sign(&fork.loser_signer).unwrap();
    sync_winner.apply_ack(&ack_from_loser).unwrap();

    let winner_summary = sync_winner.summary(topic_id).unwrap();
    let data_for_winner = sync_loser.plan_data(winner_peer, &winner_summary).unwrap();
    assert!(data_for_winner.ops.is_empty());
    let (mut ack_from_winner, _) = sync_winner
        .receive_data(loser_peer, winner_peer, data_for_winner)
        .unwrap();
    ack_from_winner.sign(&fork.winner_signer).unwrap();
    sync_loser.apply_ack(&ack_from_winner).unwrap();

    // target_needs_sync is false in both directions: no obligations and each
    // stored ack clock dominates the local clock.
    let winner_clock = fork.winner_oplog.storage().actor_clock(&topic_id).unwrap();
    let loser_clock = fork.loser_oplog.storage().actor_clock(&topic_id).unwrap();
    let ack_of_loser = fork
        .winner_oplog
        .storage()
        .peer_ack(&loser_peer, &topic_id)
        .unwrap()
        .unwrap();
    let ack_of_winner = fork
        .loser_oplog
        .storage()
        .peer_ack(&winner_peer, &topic_id)
        .unwrap()
        .unwrap();
    assert!(ack_of_loser.clock.dominates(&winner_clock));
    assert!(ack_of_winner.clock.dominates(&loser_clock));
    assert_eq!(winner_clock, loser_clock);
    assert!(
        fork.winner_oplog
            .storage()
            .all_sync_obligations()
            .unwrap()
            .is_empty()
    );
    assert!(
        fork.loser_oplog
            .storage()
            .all_sync_obligations()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn winner_purges_losing_pending() {
    // `build_fork` feeds the loser's [genesis, event] to the winner in one
    // batch. The winner keeps its own smaller genesis and filters the loser
    // genesis, so the loser event lands in pending waiting on a genesis that
    // will never arrive. The resolution must purge it.
    let fork = build_fork(1, 2);
    assert_fork_converged(&fork);

    assert!(
        fork.winner_oplog
            .storage()
            .pending_waiters(&fork.loser_genesis.id)
            .unwrap()
            .is_empty()
    );
    assert!(
        fork.winner_oplog
            .storage()
            .pending_waiters(&fork.loser_event.id)
            .unwrap()
            .is_empty()
    );
    // No pending op references the topic at all: the loser event is gone.
    assert!(
        fork.winner_oplog
            .storage()
            .ready_pending_ops()
            .unwrap()
            .is_empty()
    );
    assert!(
        fork.winner_oplog
            .storage()
            .get_op(&fork.loser_event.id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn adoption_preserves_partial_winner_pending() {
    // The winning chain can arrive partially: a descendant whose parent is not
    // in the batch must survive in pending through the adoption reset instead
    // of being wiped by it.
    let topic_id = TopicId::hash(b"genesis-fork-topic");
    let seed_three = |seed: u8, peer_seed: u8| {
        let signer = Ed25519Signer::from_bytes(&[seed; 32]);
        let peer = Ed25519Signer::from_bytes(&[peer_seed; 32]).peer_id();
        let oplog = Oplog::with_storage(MemoryStorage::new());
        let actor = actor_id_for(topic_id, signer.peer_id());
        let g = oplog
            .create_topic_genesis(
                topic_id,
                actor,
                TopicGenesis {
                    event_type_id: Note::TYPE_ID.into(),
                    initial_peers: [peer].into(),
                    replication_policy: ReplicationPolicy::default(),
                },
                &signer,
            )
            .unwrap();
        let e1 = oplog
            .create_event_op(
                topic_id,
                actor,
                EventEnvelope::encode_event(&Note { text: "e1".into() }).unwrap(),
                &signer,
            )
            .unwrap();
        let e2 = oplog
            .create_event_op(
                topic_id,
                actor,
                EventEnvelope::encode_event(&Note { text: "e2".into() }).unwrap(),
                &signer,
            )
            .unwrap();
        (signer, oplog, g, e1, e2)
    };

    let (a_signer, a_oplog, a_g, a_e1, a_e2) = seed_three(1, 2);
    let (b_signer, b_oplog, b_g, b_e1, b_e2) = seed_three(2, 1);

    // The loser adopts the winner (smaller) genesis; feed it the winner's
    // partial batch [genesis, e2], omitting e1.
    let (winner_g, winner_e1, winner_e2, winner_peer, loser_oplog) = if a_g.id < b_g.id {
        (a_g, a_e1, a_e2, a_signer.peer_id(), b_oplog)
    } else {
        (b_g, b_e1, b_e2, b_signer.peer_id(), a_oplog)
    };

    let result = loser_oplog
        .receive_ops_from_peer_evicting(
            Some(winner_peer),
            vec![winner_g.clone(), winner_e2.clone()],
        )
        .unwrap();

    // The loser reset and adopted the winner genesis; only the genesis admits.
    assert_eq!(result.evictions.len(), 1);
    assert_eq!(result.accepted, [winner_g.id].into());
    assert_eq!(
        loser_oplog
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        winner_g.id
    );
    // The partial descendant survived the reset, still waiting on its parent.
    assert!(
        loser_oplog
            .storage()
            .get_op(&winner_e2.id)
            .unwrap()
            .is_none()
    );
    let waiters = loser_oplog
        .storage()
        .pending_waiters(&winner_e1.id)
        .unwrap();
    assert_eq!(waiters.len(), 1);
    assert_eq!(waiters[0].1.id, winner_e2.id);
}

#[test]
fn resending_winner_genesis_is_a_noop() {
    let fork = build_fork(1, 2);
    let before = admitted_ids(&fork.winner_oplog, &fork.topic_id);
    let again = fork
        .winner_oplog
        .receive_ops_from_peer_evicting(
            Some(fork.winner_signer.peer_id()),
            vec![fork.winner_genesis.clone()],
        )
        .unwrap();
    assert!(again.accepted.is_empty());
    assert!(again.evictions.is_empty());
    assert_eq!(admitted_ids(&fork.winner_oplog, &fork.topic_id), before);
}

#[test]
fn fresh_topic_genesis_admits_without_resolution() {
    let signer = Ed25519Signer::from_bytes(&[1; 32]);
    let topic_id = TopicId::hash(b"fresh-topic");
    let actor = actor_id_for(topic_id, signer.peer_id());
    let source = Oplog::with_storage(MemoryStorage::new());
    let genesis = source
        .create_topic_genesis(
            topic_id,
            actor,
            TopicGenesis {
                event_type_id: Note::TYPE_ID.into(),
                initial_peers: BTreeSet::new(),
                replication_policy: ReplicationPolicy::default(),
            },
            &signer,
        )
        .unwrap();

    let receiver = Oplog::with_storage(MemoryStorage::new());
    let admitted = receiver
        .receive_ops_from_peer_evicting(Some(signer.peer_id()), vec![genesis.clone()])
        .unwrap();
    assert_eq!(admitted.accepted, [genesis.id].into());
    assert!(admitted.evictions.is_empty());
    assert_eq!(
        receiver
            .storage()
            .topic_state(&topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        genesis.id
    );
}

#[test]
fn structurally_invalid_genesis_is_rejected_without_reset() {
    let fork = build_fork(1, 2);
    let winner_genesis = fork.winner_genesis.id;

    // A genesis payload with actor_seq 2 is not a structural genesis, so it can
    // never win the tie-break; admission must reject it and leave the topic.
    let intruder = Ed25519Signer::from_bytes(&[9; 32]);
    let bogus = Op::sign(
        OpBody {
            topic_id: fork.topic_id,
            author: intruder.peer_id(),
            actor_id: actor_id_for(fork.topic_id, intruder.peer_id()),
            actor_seq: 2,
            actor_prev: None,
            deps: BTreeSet::new(),
            generation: 0,
            payload: TopicPayload::Genesis(TopicGenesis {
                event_type_id: Note::TYPE_ID.into(),
                initial_peers: BTreeSet::new(),
                replication_policy: ReplicationPolicy::default(),
            }),
        },
        &intruder,
    )
    .unwrap();

    let result = fork
        .loser_oplog
        .receive_ops_from_peer_evicting(Some(intruder.peer_id()), vec![bogus]);
    assert!(matches!(result, Err(Error::InvalidGenesis)));
    assert_eq!(
        fork.loser_oplog
            .storage()
            .topic_state(&fork.topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        winner_genesis
    );
}

/// Every admitted op in `topic_id` must be able to resolve its dependencies.
fn assert_dag_whole(oplog: &Oplog, topic_id: &TopicId) {
    for op_id in oplog.storage().list_op_ids(topic_id).unwrap() {
        let meta = oplog.storage().get_meta(&op_id).unwrap().unwrap();
        for dep in &meta.deps {
            assert!(
                oplog.storage().dep_resolvable(dep).unwrap(),
                "admitted {op_id} references dependency {dep} that is not fully stored"
            );
        }
    }
}

#[test]
fn reset_defers_stale_dependents() {
    // The reset path reads dep presence before it wipes the topic. An op whose
    // dependency only exists in the chain about to be discarded must be
    // buffered, never admitted against storage that is one step from empty.
    let fork = build_fork(11, 12);
    let (loser_seed, winner_seed) = if fork.a_won { (12, 11) } else { (11, 12) };
    let (loser, _, _, loser_event) = seed_side(fork.topic_id, loser_seed, winner_seed, "replay");
    let stale_dependent = Op::sign(
        OpBody {
            topic_id: fork.topic_id,
            author: fork.winner_signer.peer_id(),
            actor_id: actor_id_for(fork.topic_id, fork.winner_signer.peer_id()),
            actor_seq: 3,
            actor_prev: Some(fork.winner_event.id),
            deps: [fork.winner_event.id, loser_event.id].into(),
            generation: 2,
            payload: TopicPayload::Event(
                EventEnvelope::encode_event(&Note {
                    text: "straddles both chains".into(),
                })
                .unwrap(),
            ),
        },
        &fork.winner_signer,
    )
    .unwrap();

    loser
        .receive_ops_from_peer_evicting(
            Some(fork.winner_signer.peer_id()),
            vec![
                fork.winner_genesis.clone(),
                fork.winner_event.clone(),
                stale_dependent.clone(),
            ],
        )
        .unwrap();

    let admitted = admitted_ids(&loser, &fork.topic_id);
    assert!(!admitted.contains(&stale_dependent.id));
    assert_dag_whole(&loser, &fork.topic_id);
}

#[test]
fn reset_keeps_dag_whole() {
    let fork = build_fork(13, 14);
    assert_dag_whole(&fork.winner_oplog, &fork.topic_id);
    assert_dag_whole(&fork.loser_oplog, &fork.topic_id);
}

#[test]
fn reset_ignores_tips() {
    // Adopting a winning genesis judges the batch against a fresh topic: the
    // actor slots and tips it would otherwise read belong to the chain the same
    // transaction removes, so a re-emitted op must not read as a fork.
    let fork = build_fork(21, 22);
    let reemitted = fork
        .loser_oplog
        .create_event_op(
            fork.topic_id,
            actor_id_for(fork.topic_id, fork.loser_signer.peer_id()),
            EventEnvelope::encode_event(&Note {
                text: "re-emitted under the winner".into(),
            })
            .unwrap(),
            &fork.loser_signer,
        )
        .unwrap();

    let late = Oplog::with_storage(MemoryStorage::new());
    late.receive_ops(vec![fork.loser_genesis.clone(), fork.loser_event.clone()])
        .unwrap();
    late.receive_ops_from_peer_evicting(
        Some(fork.winner_signer.peer_id()),
        vec![
            fork.winner_genesis.clone(),
            fork.winner_event.clone(),
            reemitted.clone(),
        ],
    )
    .unwrap();

    assert_eq!(
        late.storage()
            .topic_state(&fork.topic_id)
            .unwrap()
            .unwrap()
            .genesis,
        fork.winner_genesis.id
    );
    assert_eq!(
        admitted_ids(&late, &fork.topic_id),
        [fork.winner_genesis.id, fork.winner_event.id, reemitted.id].into()
    );
}

/// Rebuild the store the pre-`reset_topic_and_admit` genesis reset could leave:
/// the winning genesis installed while a descendant of the replaced chain stays
/// admitted with its ancestry gone.
fn assert_quarantines_orphan<S: Corrupt>(storage: S) {
    let topic_id = TopicId::hash(b"legacy-reset-topic");
    let keeper = Ed25519Signer::from_bytes(&[203; 32]);
    let peer = |seed: u8| Ed25519Signer::from_bytes(&[seed; 32]).peer_id();
    let (_, sign_a, g_a, e_a) = forked_side(
        MemoryStorage::new(),
        topic_id,
        201,
        [peer(202), keeper.peer_id()],
        "a-branch",
    );
    let (_, sign_b, g_b, e_b) = forked_side(
        MemoryStorage::new(),
        topic_id,
        202,
        [peer(201), keeper.peer_id()],
        "b-branch",
    );
    let a_won = g_a.id < g_b.id;
    let (won_genesis, won_event) = if a_won {
        (g_a.clone(), e_a.clone())
    } else {
        (g_b.clone(), e_b.clone())
    };
    let (lost_genesis, lost_event) = if a_won { (g_b, e_b) } else { (g_a, e_a) };
    let (won_signer, lost_signer) = if a_won {
        (sign_a, sign_b)
    } else {
        (sign_b, sign_a)
    };

    // A peer that never saw the collision still serves the replaced chain.
    let keeper_store = MemoryStorage::new();
    Oplog::with_storage(keeper_store.clone())
        .receive_ops(vec![lost_genesis.clone(), lost_event.clone()])
        .unwrap();
    let keeper_node = Irokle::with_storage(
        keeper_store.clone(),
        NodeConfig {
            signer: keeper,
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();

    let holder_log = oplog::Oplog::with_storage(storage.clone());
    holder_log
        .receive_ops(vec![lost_genesis.clone(), lost_event.clone()])
        .unwrap();
    holder_log
        .receive_ops_from_peer_evicting(
            Some(won_signer.peer_id()),
            vec![won_genesis.clone(), won_event.clone()],
        )
        .unwrap();
    let orphan_meta = keeper_store.get_meta(&lost_event.id).unwrap().unwrap();
    storage.orphan_op(&lost_event, &orphan_meta);

    let holder = Irokle::with_storage(
        storage.clone(),
        NodeConfig {
            signer: lost_signer.clone(),
            default_write_concern: WriteConcern::Local,
            ..NodeConfig::default()
        },
    )
    .unwrap();
    let lost_actor = actor_id_for(topic_id, lost_signer.peer_id());
    assert_eq!(
        storage.actor_tip(&topic_id, &lost_actor).unwrap(),
        Some((orphan_meta.actor_seq, lost_event.id))
    );
    assert_eq!(
        holder.topic_unresolved(topic_id).unwrap(),
        [lost_genesis.id].into()
    );

    // Ordinary anti-entropy against the peer that still holds the replaced
    // chain must not merge its genesis back into the winner.
    let plan = holder
        .negotiate_sync(
            keeper_node.peer_id(),
            &keeper_node.sync_summary(topic_id).unwrap(),
        )
        .unwrap();
    assert!(plan.need.contains(&lost_genesis.id));
    let data = keeper_node
        .plan_sync_response_data(
            holder.peer_id(),
            &crate::sync::SyncRequest {
                topic_id,
                known: plan.common,
                wants: plan.need,
                actor_range_hints: plan.actor_range_hints,
            },
        )
        .unwrap();
    holder
        .receive_sync_data_from(keeper_node.peer_id(), data)
        .unwrap();
    assert_eq!(
        storage.topic_state(&topic_id).unwrap().unwrap().genesis,
        won_genesis.id
    );
    assert!(storage.get_op(&lost_genesis.id).unwrap().is_none());
    assert_eq!(
        holder.topic_unresolved(topic_id).unwrap(),
        [lost_genesis.id].into()
    );

    // A hole the frontier does reach is repair work, not quarantine work.
    damage_op(&storage, &won_event.id, Damage::Op);
    assert!(holder.quarantine_orphans(topic_id).unwrap().is_none());
    assert_eq!(
        storage.get_op(&lost_event.id).unwrap(),
        Some(lost_event.clone())
    );
    holder_log.receive_ops(vec![won_event.clone()]).unwrap();

    let eviction = holder
        .quarantine_orphans(topic_id)
        .unwrap()
        .expect("the orphan closure no peer can repair must be quarantined");
    assert_eq!(eviction.topic_id, topic_id);
    assert_eq!(eviction.losing_genesis, won_genesis.id);
    assert_eq!(eviction.winning_genesis, won_genesis.id);
    assert_eq!(
        eviction
            .evicted
            .iter()
            .map(|op| op.op_id)
            .collect::<Vec<_>>(),
        vec![lost_event.id]
    );
    assert_eq!(eviction.evicted[0].payload, lost_event.signed.body.payload);

    // The rebuild journalled its payloads in the transaction that discarded
    // them, so losing the returned copy is not losing the payloads.
    assert!(storage.pending_evictions().unwrap().contains(&eviction));
    storage.clear_eviction(&eviction.key()).unwrap();
    assert!(!storage.pending_evictions().unwrap().contains(&eviction));

    assert!(holder.topic_unresolved(topic_id).unwrap().is_empty());
    assert!(storage.get_op(&lost_event.id).unwrap().is_none());
    assert!(storage.get_meta(&lost_event.id).unwrap().is_none());
    assert_eq!(
        storage.list_op_ids(&topic_id).unwrap(),
        [won_genesis.id, won_event.id].into()
    );
    assert_eq!(storage.heads(&topic_id).unwrap(), [won_event.id].into());
    let state = storage.topic_state(&topic_id).unwrap().unwrap();
    assert_eq!(state.genesis, won_genesis.id);
    assert_eq!(state.heads, [won_event.id].into());
    assert_eq!(storage.actor_tip(&topic_id, &lost_actor).unwrap(), None);
    assert_eq!(
        storage
            .actor_index(&topic_id, &lost_actor, orphan_meta.actor_seq)
            .unwrap(),
        None
    );
    assert_eq!(storage.actor_clock(&topic_id).unwrap().get(&lost_actor), 0);
    assert!(storage.children(&lost_genesis.id).unwrap().is_empty());
    assert_eq!(
        oplog::topological(&storage, &topic_id)
            .unwrap()
            .iter()
            .map(|op| op.id)
            .collect::<Vec<_>>(),
        vec![won_genesis.id, won_event.id]
    );
    assert!(holder.quarantine_orphans(topic_id).unwrap().is_none());
}

#[test]
fn memory_quarantines_orphan() {
    assert_quarantines_orphan(MemoryStorage::new());
}

#[cfg(feature = "fjall")]
#[test]
fn fjall_quarantines_orphan() {
    let dir = tempfile::tempdir().unwrap();
    assert_quarantines_orphan(crate::storage::FjallStorage::open(dir.path()).unwrap());
}
