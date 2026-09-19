// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::Signer;
use crate::net::iroh::exchange::*;

fn payload_message() -> SyncMessage {
    let signer = crate::Ed25519Signer::from_bytes(&[214; 32]);
    let topic_id = crate::TopicId::hash(b"leased-result");
    let op = crate::Op::sign(
        crate::OpBody {
            topic_id,
            author: signer.peer_id(),
            actor_id: crate::actor_id_for(topic_id, signer.peer_id()),
            actor_seq: 2,
            actor_prev: None,
            deps: Default::default(),
            generation: 1,
            payload: crate::TopicPayload::Event(crate::EventEnvelope {
                type_id: "test.payload".into(),
                payload: bytes::Bytes::from(vec![42; 1024]),
            }),
        },
        &signer,
    )
    .unwrap();
    SyncMessage::Data(crate::sync::SyncData {
        topic_id,
        ops: vec![op],
    })
}

fn reservation() -> usize {
    2 * crate::net::decoded_message_bound(&payload_message()).unwrap()
}

fn batch(budget: &Arc<ByteBudget>) -> SyncResponses {
    SyncResponses {
        messages: vec![payload_message(), payload_message()],
        charges: vec![
            budget
                .try_take(Pool::Results, reservation(), OwnedClass::Results)
                .unwrap(),
        ],
    }
}

fn assert_held(budget: &Arc<ByteBudget>) {
    assert_eq!(
        budget.owned().current[&OwnedClass::Results],
        reservation() as u64
    );
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
    let SyncMessage::Data(data) = item.as_ref() else {
        panic!("expected payload");
    };
    assert_eq!(data.ops.len(), 1);
    data.ops[0].validate().unwrap();
    let crate::TopicPayload::Event(event) = &data.ops[0].signed.body.payload else {
        panic!("expected event");
    };
    assert_eq!(event.payload.as_ref(), &[42; 1024]);
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
        .filter(|item| matches!(&**item, SyncMessage::Data(_)))
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
    let net = Arc::new(crate::net::iroh::IrohNet::new(endpoint, node).unwrap());
    let (messages, charge) = batch(&net.budget).into_session_charge(&net.budget).unwrap();
    let (started, arrival) = tokio::sync::oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let (finished, completion) = tokio::sync::oneshot::channel();
    let worker = Arc::clone(&net);
    let task = tokio::spawn(async move {
        worker
            .run_job(crate::net::iroh::Lane::Bulk, move |_| {
                let retained = (messages, charge);
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
    assert_eq!(
        net.budget.owned().current[&OwnedClass::Session],
        reservation() as u64
    );
    let control = tokio::time::timeout(
        Duration::from_secs(60),
        net.run_job(crate::net::iroh::Lane::Control, |_| 42),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(control, 42);
    assert_eq!(
        net.budget.owned().current[&OwnedClass::Session],
        reservation() as u64
    );
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
    let (messages, charge) = batch(&budget).into_session_charge(&budget).unwrap();
    assert_released(&budget);
    assert_eq!(
        budget.owned().current[&OwnedClass::Session],
        reservation() as u64
    );
    let job = Arc::clone(&charge);
    drop(charge);
    assert_eq!(
        budget.owned().current[&OwnedClass::Session],
        reservation() as u64
    );
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
    let error = batch(&budget).into_session_charge(&budget).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert_released(&budget);
}
