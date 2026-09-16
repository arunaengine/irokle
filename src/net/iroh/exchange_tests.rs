// SPDX-License-Identifier: MIT OR Apache-2.0

use super::*;

fn batch(budget: &Arc<ByteBudget>) -> SyncResponses {
    let message = SyncMessage::Page(crate::sync::SyncPage {
        topic_id: crate::TopicId::hash(b"leased-result"),
        more: false,
        missing: Default::default(),
        positions: Default::default(),
        continued: false,
    });
    SyncResponses {
        messages: vec![message.clone(), message],
        charges: vec![
            budget
                .try_take(Pool::Results, 4096, OwnedClass::Results)
                .unwrap(),
        ],
    }
}

fn assert_held(budget: &Arc<ByteBudget>) {
    assert_eq!(budget.owned().current[&OwnedClass::Results], 4096);
    assert!(
        budget
            .try_take(
                Pool::Results,
                budget.capacity(Pool::Results),
                OwnedClass::Results,
            )
            .is_err()
    );
}

fn assert_released(budget: &Arc<ByteBudget>) {
    assert_eq!(budget.owned().current[&OwnedClass::Results], 0);
    assert_eq!(
        budget.available(Pool::Results),
        budget.capacity(Pool::Results)
    );
}

#[test]
fn collection_retains_bytes() {
    let budget = ByteBudget::new(0, 0);
    let mut items = batch(&budget).into_iter().collect::<Vec<_>>();
    assert_held(&budget);
    let item = items.pop().unwrap();
    drop(items);
    assert_held(&budget);
    assert!(matches!(item.as_ref(), SyncMessage::Page(_)));
    drop(item);
    assert_released(&budget);
}

#[test]
fn extraction_retains_bytes() {
    let budget = ByteBudget::new(0, 0);
    let mut iter = batch(&budget).into_iter();
    let item = iter.next().unwrap();
    assert_held(&budget);
    drop(iter);
    assert_held(&budget);
    drop(item);
    assert_released(&budget);
}

#[test]
fn adapters_retain_bytes() {
    let budget = ByteBudget::new(0, 0);
    let items = batch(&budget)
        .into_iter()
        .filter(|item| matches!(&**item, SyncMessage::Page(_)))
        .take(1)
        .collect::<Vec<_>>();
    assert_eq!(items.len(), 1);
    assert_held(&budget);
    drop(items);
    assert_released(&budget);
}

#[test]
fn error_retains_item() {
    let budget = ByteBudget::new(0, 0);
    let mut retained = None;
    let result: Result<(), ()> = batch(&budget).into_iter().try_for_each(|item| {
        retained = Some(item);
        Err(())
    });
    assert!(result.is_err());
    assert_held(&budget);
    drop(retained);
    assert_released(&budget);
}

#[test]
fn unwind_releases_bytes() {
    let budget = ByteBudget::new(0, 0);
    let owned = Arc::clone(&budget);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _items = batch(&owned).into_iter().collect::<Vec<_>>();
        assert_held(&owned);
        panic!("drop leased response items during unwinding");
    }));
    assert!(result.is_err());
    assert_released(&budget);
}

#[tokio::test]
async fn cancellation_releases_bytes() {
    let budget = ByteBudget::new(0, 0);
    let responses = batch(&budget);
    let (started, arrival) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _items = responses.into_iter().collect::<Vec<_>>();
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    tokio::time::timeout(Duration::from_secs(60), arrival)
        .await
        .unwrap()
        .unwrap();
    assert_held(&budget);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_released(&budget);
}

#[tokio::test]
async fn cancelled_storage_retains() {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
        .secret_key(iroh::SecretKey::from_bytes(&[213; 32]))
        .bind()
        .await
        .unwrap();
    let node = crate::Irokle::builder()
        .with_iroh_secret_key(endpoint.secret_key())
        .build()
        .unwrap();
    let net = Arc::new(super::super::IrohNet::new(endpoint, node).unwrap());
    let (messages, lease) = batch(&net.budget).into_session(&net.budget).unwrap();
    let (started, arrival) = tokio::sync::oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let (finished, completion) = tokio::sync::oneshot::channel();
    let worker = Arc::clone(&net);
    let task = tokio::spawn(async move {
        worker
            .run_job(super::super::Lane::Bulk, move |_| {
                let retained = (messages, lease);
                started.send(()).unwrap();
                gate.recv_timeout(Duration::from_secs(60)).unwrap();
                drop(retained);
                finished.send(()).unwrap();
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(60), arrival)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(net.budget.owned().current[&OwnedClass::Session], 4096);
    let control = tokio::time::timeout(
        Duration::from_secs(60),
        net.run_job(super::super::Lane::Control, |_| 42),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(control, 42);
    assert_eq!(net.budget.owned().current[&OwnedClass::Session], 4096);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(60), completion)
        .await
        .unwrap()
        .unwrap();
    net.shutdown().await;
    assert_eq!(net.budget.owned().current[&OwnedClass::Session], 0);
}

#[test]
fn transfer_preserves_ownership() {
    let budget = ByteBudget::new(0, 0);
    let (messages, lease) = batch(&budget).into_session(&budget).unwrap();
    assert_released(&budget);
    assert_eq!(budget.owned().current[&OwnedClass::Session], 4096);
    let job = Arc::clone(&lease);
    drop(lease);
    assert_eq!(budget.owned().current[&OwnedClass::Session], 4096);
    drop((messages, job));
    assert_eq!(budget.owned().current[&OwnedClass::Session], 0);
}

#[test]
fn transfer_full_fails() {
    let budget = ByteBudget::new(0, 0);
    let _held = budget
        .try_take(
            Pool::Session,
            budget.capacity(Pool::Session),
            OwnedClass::Session,
        )
        .unwrap();
    let error = batch(&budget).into_session(&budget).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_released(&budget);
}
