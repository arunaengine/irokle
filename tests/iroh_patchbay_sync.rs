#![cfg(all(feature = "iroh", target_os = "linux"))]

use std::time::Duration;

use irokle::{ReplicationPolicy, TopicConfig};

mod support;

use support::patchbay::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_sync() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id =
        publish_initial_topic(&env.alice_dev, &env.alice, [env.bob.node.peer_id()]).await?;
    publish(&env.alice_dev, &env.alice, topic_id, "clean direct").await?;

    sync(&env.alice_dev, &env.alice, &env.bob, topic_id).await?;

    assert_eq!(
        history_texts(&env.bob_dev, &env.bob, topic_id).await?,
        ["clean direct"]
    );
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flaky_sync() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id =
        publish_initial_topic(&env.alice_dev, &env.alice, [env.bob.node.peer_id()]).await?;
    publish(&env.alice_dev, &env.alice, topic_id, "alice one").await?;
    sync(&env.alice_dev, &env.alice, &env.bob, topic_id).await?;
    publish(&env.bob_dev, &env.bob, topic_id, "bob one").await?;

    env.impair_bob_link().await?;
    isolate(&env.bob_dev).await?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    env.impair_bob_link().await?;

    converge_pair(
        &env.alice_dev,
        &env.alice,
        &env.bob_dev,
        &env.bob,
        topic_id,
        2,
    )
    .await?;

    assert_eq!(
        history_texts(&env.alice_dev, &env.alice, topic_id)
            .await?
            .len(),
        2
    );
    assert_eq!(
        history_texts(&env.bob_dev, &env.bob, topic_id).await?.len(),
        2
    );
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn churn() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id =
        publish_initial_topic(&env.alice_dev, &env.alice, [env.bob.node.peer_id()]).await?;
    publish(&env.alice_dev, &env.alice, topic_id, "before join").await?;
    converge_pair(
        &env.alice_dev,
        &env.alice,
        &env.bob_dev,
        &env.bob,
        topic_id,
        1,
    )
    .await?;

    add_peer(
        &env.alice_dev,
        &env.alice,
        topic_id,
        env.carol.node.peer_id(),
    )
    .await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 1).await?;
    publish(&env.carol_dev, &env.carol, topic_id, "late joiner publish").await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 2).await?;

    leave(&env.bob_dev, &env.bob, topic_id).await?;
    sync(&env.bob_dev, &env.bob, &env.alice, topic_id).await?;
    sync(&env.bob_dev, &env.bob, &env.carol, topic_id).await?;
    converge_all(&env, topic_id, &[&env.alice, &env.carol], 2).await?;
    add_peer(&env.alice_dev, &env.alice, topic_id, env.bob.node.peer_id()).await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 2).await?;
    publish(&env.bob_dev, &env.bob, topic_id, "re-added publish").await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 3).await?;

    assert_current_members(
        &env.alice_dev,
        &env.alice,
        topic_id,
        [
            env.alice.node.peer_id(),
            env.bob.node.peer_id(),
            env.carol.node.peer_id(),
        ],
    )
    .await?;
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removed_peer() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id = publish_initial_topic(
        &env.alice_dev,
        &env.alice,
        [env.bob.node.peer_id(), env.carol.node.peer_id()],
    )
    .await?;
    publish(&env.alice_dev, &env.alice, topic_id, "visible").await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 1).await?;

    remove_peer(&env.alice_dev, &env.alice, topic_id, env.bob.node.peer_id()).await?;
    converge_all(&env, topic_id, &[&env.alice, &env.carol], 1).await?;
    sync(&env.alice_dev, &env.alice, &env.bob, topic_id).await?;
    let before = raw_history_len(&env.bob_dev, &env.bob, topic_id).await?;

    publish(&env.alice_dev, &env.alice, topic_id, "after remove").await?;
    converge_all(&env, topic_id, &[&env.alice, &env.carol], 2).await?;
    sync(&env.alice_dev, &env.alice, &env.bob, topic_id).await?;

    assert_eq!(
        raw_history_len(&env.bob_dev, &env.bob, topic_id).await?,
        before
    );
    assert_eq!(
        history_texts(&env.carol_dev, &env.carol, topic_id)
            .await?
            .len(),
        2
    );
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_member() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id =
        publish_initial_topic(&env.alice_dev, &env.alice, [env.bob.node.peer_id()]).await?;
    publish(&env.alice_dev, &env.alice, topic_id, "secret").await?;

    sync(&env.alice_dev, &env.alice, &env.carol, topic_id).await?;

    assert!(topic_missing(&env.carol_dev, &env.carol, topic_id).await?);
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whitelist() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    let topic_id =
        publish_initial_topic(&env.alice_dev, &env.alice, [env.bob.node.peer_id()]).await?;
    publish(&env.alice_dev, &env.alice, topic_id, "blocked first").await?;

    let _ = sync(&env.alice_dev, &env.alice, &env.bob, topic_id).await;
    assert!(topic_missing(&env.bob_dev, &env.bob, topic_id).await?);

    env.bob
        .node
        .add_peer_to_whitelist(env.alice.node.peer_id())?;
    sync(&env.alice_dev, &env.alice, &env.bob, topic_id).await?;
    assert_eq!(
        history_texts(&env.bob_dev, &env.bob, topic_id).await?.len(),
        1
    );
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id = publish_initial_topic(
        &env.alice_dev,
        &env.alice,
        [env.bob.node.peer_id(), env.carol.node.peer_id()],
    )
    .await?;
    publish(&env.alice_dev, &env.alice, topic_id, "before partition").await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 1).await?;

    isolate(&env.carol_dev).await?;
    publish(&env.alice_dev, &env.alice, topic_id, "alice partition").await?;
    publish(&env.bob_dev, &env.bob, topic_id, "bob partition").await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob], 3).await?;
    assert_eq!(
        history_texts(&env.carol_dev, &env.carol, topic_id)
            .await?
            .len(),
        1
    );

    restore(&env.carol_dev).await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 3).await?;
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_writes() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id = publish_initial_topic(
        &env.alice_dev,
        &env.alice,
        [env.bob.node.peer_id(), env.carol.node.peer_id()],
    )
    .await?;
    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 0).await?;

    let (a, b, c) = tokio::join!(
        publish(&env.alice_dev, &env.alice, topic_id, "alice concurrent"),
        publish(&env.bob_dev, &env.bob, topic_id, "bob concurrent"),
        publish(&env.carol_dev, &env.carol, topic_id, "carol concurrent"),
    );
    a?;
    b?;
    c?;

    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 3).await?;
    let heads = raw_heads_len(&env.alice_dev, &env.alice, topic_id).await?;
    assert!(
        heads >= 2,
        "concurrent writers should leave multiple DAG heads before a later write"
    );
    guard.ok();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fanout_cap() -> TestResult<()> {
    let env = PatchLab::new().await?;
    let guard = env.lab.test_guard();
    env.whitelist_all()?;
    let topic_id = publish_initial_topic_with_config(
        &env.alice_dev,
        &env.alice,
        TopicConfig {
            initial_peers: [env.bob.node.peer_id(), env.carol.node.peer_id()].into(),
            replication_policy: ReplicationPolicy::all().with_max_sync_peers(1),
        },
    )
    .await?;
    publish(&env.alice_dev, &env.alice, topic_id, "capped fanout").await?;

    sync_topic(&env.alice_dev, &env.alice, topic_id).await?;

    let bob_has = !topic_missing(&env.bob_dev, &env.bob, topic_id).await?;
    let carol_has = !topic_missing(&env.carol_dev, &env.carol, topic_id).await?;
    assert_eq!(usize::from(bob_has) + usize::from(carol_has), 1);

    converge_all(&env, topic_id, &[&env.alice, &env.bob, &env.carol], 1).await?;
    guard.ok();
    Ok(())
}
