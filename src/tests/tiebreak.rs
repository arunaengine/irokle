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
    let signer = Ed25519Signer::from_bytes(&[seed; 32]);
    let peer = Ed25519Signer::from_bytes(&[peer_seed; 32]).peer_id();
    let oplog = Oplog::with_storage(MemoryStorage::new());
    let actor = actor_id_for(topic_id, signer.peer_id());
    let genesis = TopicGenesis {
        event_type_id: Note::TYPE_ID.into(),
        initial_peers: [peer].into(),
        replication_policy: ReplicationPolicy::default(),
    };
    let genesis_op = oplog
        .create_topic_genesis(topic_id, actor, genesis, &signer)
        .unwrap();
    let event_op = oplog
        .create_event_op(
            topic_id,
            actor,
            EventEnvelope::encode_event(&Note { text: text.into() }).unwrap(),
            &signer,
        )
        .unwrap();
    (oplog, signer, genesis_op, event_op)
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
    let sync_winner = SyncEngine::new(fork.winner_oplog.clone());
    let sync_loser = SyncEngine::new(fork.loser_oplog.clone());

    // A normal sync round in both directions. Fingerprints already match after
    // resolution, so no ops move; the point is that the signed acks validate
    // and each clock dominates the other's.
    let loser_summary = sync_loser.summary(topic_id).unwrap();
    let data_for_loser = sync_winner.plan_data(loser_peer, &loser_summary).unwrap();
    assert!(data_for_loser.ops.is_empty());
    let mut ack_from_loser = sync_loser
        .receive_data(winner_peer, loser_peer, data_for_loser)
        .unwrap();
    ack_from_loser.sign(&fork.loser_signer).unwrap();
    sync_winner.apply_ack(&ack_from_loser).unwrap();

    let winner_summary = sync_winner.summary(topic_id).unwrap();
    let data_for_winner = sync_loser.plan_data(winner_peer, &winner_summary).unwrap();
    assert!(data_for_winner.ops.is_empty());
    let mut ack_from_winner = sync_winner
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
