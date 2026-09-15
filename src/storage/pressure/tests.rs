use super::*;

#[test]
fn refusal_precedes_encoding() {
    struct Value(std::sync::atomic::AtomicUsize);
    impl serde::Serialize for Value {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            serializer.serialize_bytes(&[0; 1024])
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let db = fjall::OptimisticTxDatabase::builder(directory.path())
        .open()
        .unwrap();
    let records = db
        .keyspace("probe", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    let pressure = Pressure::shared(records.path()).unwrap();
    let mut policy = StoragePressure::default().with_probe(|_| Ok(1024 * 1024 * 1024));
    policy.buffer_bytes = 64 * 1024;
    pressure.configure(policy).unwrap();
    let reservation = pressure.begin(false, || Ok((0, 0))).unwrap();
    let mut tx = Transaction::new(db.write_tx().unwrap(), reservation);
    let value = Value(std::sync::atomic::AtomicUsize::new(0));
    assert!(matches!(
        tx.put(&records, b"large", &value),
        Err(Error::StorageBuffer { .. })
    ));
    assert_eq!(value.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(tx);
    assert!(
        fjall::Readable::get(&db.read_tx(), &records, b"large")
            .unwrap()
            .is_none()
    );
    assert_eq!(pressure.usage().unwrap().reserved.values().sum::<u64>(), 0);
}

#[test]
fn reservations_balance() {
    let directory = tempfile::tempdir().unwrap();
    let pressure = Pressure::shared(directory.path().to_path_buf()).unwrap();
    pressure
        .configure(StoragePressure::default().with_probe(|_| Ok(1024 * 1024 * 1024)))
        .unwrap();
    let mut first = pressure.begin(false, || Ok((0, 0))).unwrap();
    first
        .grow(StorageDomain::ClockNodes, 200 * 1024 * 1024)
        .unwrap();
    let mut second = pressure.begin(false, || Ok((0, 0))).unwrap();
    assert!(matches!(
        second.grow(StorageDomain::Metadata, 200 * 1024 * 1024),
        Err(Error::StoragePressure(_))
    ));
    drop(first);
    second
        .grow(StorageDomain::Metadata, 200 * 1024 * 1024)
        .unwrap();
    second.committed();
    drop(second);
    let usage = pressure.usage().unwrap();
    assert_eq!(usage.reserved.values().sum::<u64>(), 0);
    assert_eq!(
        usage.committed_bytes[&StorageDomain::Metadata],
        200 * 1024 * 1024 + 32 * 1024
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _held = pressure.begin(true, || Ok((0, 0))).unwrap();
        panic!("reservation owner failed");
    }));
    assert!(result.is_err());
    assert_eq!(pressure.usage().unwrap().reserved.values().sum::<u64>(), 0);
}
