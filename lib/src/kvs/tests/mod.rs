use crate::kvs::clock::SizedClock;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
pub(crate) enum Kvs {
	#[allow(dead_code)]
	Mem,
	#[allow(dead_code)]
	Rocksdb,
	#[allow(dead_code)]
	Speedb,
	#[allow(dead_code)]
	Tikv,
	#[allow(dead_code)]
	Fdb,
}

// This type is unsused when no store is enabled.
#[allow(dead_code)]
type ClockType = Arc<RwLock<SizedClock>>;

#[cfg(any(feature = "kv-tikv", feature = "kv-fdb"))]
const DIST_RETRIES: u32 = 100;
#[cfg(any(feature = "kv-tikv", feature = "kv-fdb"))]
const DIST_SLEEP_MS: u64 = 100;

#[cfg(feature = "kv-mem")]
mod mem {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use crate::kvs::LockType;
	use crate::kvs::Transaction;
	use crate::kvs::TransactionType;
	use serial_test::serial;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		(
			Datastore::new_full("memory", Some(clock_override))
				.await
				.unwrap()
				.with_node_id(node_id),
			Kvs::Mem,
		)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::from_str("361893b5-a041-40c0-996c-c3a8828ef06b").unwrap();
		let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use crate::kvs::LockType;
	use crate::kvs::Transaction;
	use crate::kvs::TransactionType;
	use serial_test::serial;
	use temp_dir::TempDir;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		(
			Datastore::new_full(format!("rocksdb:{path}").as_str(), Some(clock_override))
				.await
				.unwrap()
				.with_node_id(node_id),
			Kvs::Rocksdb,
		)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::from_str("22358e5e-87bd-4040-8c63-01db896191ab").unwrap();
		let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
}

#[cfg(feature = "kv-speedb")]
mod speedb {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Transaction;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use serial_test::serial;
	use temp_dir::TempDir;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		(
			Datastore::new_full(format!("speedb:{path}").as_str(), Some(clock_override))
				.await
				.unwrap()
				.with_node_id(node_id),
			Kvs::Speedb,
		)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::from_str("5877e580-12ac-49e4-95e1-3c407c4887f3").unwrap();
		let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
}

#[cfg(feature = "kv-tikv")]
mod tikv {

	use crate::kvs::tests::{ClockType, Kvs, DIST_RETRIES, DIST_SLEEP_MS};
	use crate::kvs::Transaction;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use serial_test::serial;
	use std::time::Duration;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let ds = Datastore::new_full("tikv:127.0.0.1:2379", Some(clock_override))
			.await
			.unwrap()
			.with_node_id(node_id);
		// Clear any previous test entries
		let mut tx_err: Option<Error> = None;
		for _ in 0..DIST_RETRIES {
			let tx = ds.transaction(Write, Optimistic).await.unwrap();
			if let Err(e) = clear_cluster(tx).await {
				error!("Failed cluster wipe: {}", e);
				tx_err = Some(e);
				tokio::time::sleep(Duration::from_millis(DIST_SLEEP_MS)).await;
			} else {
				tx_err = None;
				break;
			}
		}
		if let Some(e) = tx_err {
			panic!("Failed cluster wipe: {}", e);
		}
		// Return the datastore
		(ds, Kvs::Tikv)
	}

	async fn clear_cluster(mut tx: Transaction) -> Result<(), Error> {
		if let Err(e) = tx.delp(vec![], u32::MAX).await {
			let second_err = tx.cancel().await;
			return match second_err {
				Ok(_) => Err(e),
				Err(e2) => Err(Error::TxRollbackFailed(format!(
					"Failed to cancel transaction: {}, original error cause was: {}",
					e2, e
				))),
			};
		}
		tx.commit().await
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::from_str("18717a0f-0ab0-421e-b20c-e69fb03e90a3").unwrap();
		let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
}

#[cfg(feature = "kv-fdb")]
mod fdb {
	use crate::kvs::tests::{ClockType, Kvs, DIST_RETRIES, DIST_SLEEP_MS};
	use crate::kvs::Transaction;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use serial_test::serial;
	use std::time::Duration;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let ds = Datastore::new_full("fdb:/etc/foundationdb/fdb.cluster", Some(clock_override))
			.await
			.unwrap()
			.with_node_id(node_id);
		// Clear any previous test entries
		let mut tx_err: Option<Error> = None;
		for _ in 0..DIST_RETRIES {
			let tx = ds.transaction(Write, Optimistic).await.unwrap();
			if let Err(e) = clear_cluster(tx).await {
				error!("Failed cluster wipe: {}", e);
				tx_err = Some(e);
				tokio::time::sleep(Duration::from_millis(DIST_SLEEP_MS)).await;
			} else {
				tx_err = None;
				break;
			}
		}
		if let Some(e) = tx_err {
			panic!("Failed cluster wipe: {}", e);
		}
		// Return the datastore
		(ds, Kvs::Fdb)
	}

	async fn clear_cluster(mut tx: Transaction) -> Result<(), Error> {
		if let Err(err) = tx.delp(vec![], u32::MAX).await {
			let second_err = tx.cancel().await;
			return match second_err {
				Ok(_) => Err(err),
				Err(e2) => Err(Error::TxRollbackFailed(format!(
					"Failed to cancel transaction: {}, original error cause was: {}",
					e2, err
				))),
			};
		}
		tx.commit().await
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::from_str("50f5bdf5-8abe-406b-8002-a79c942f510f").unwrap();
		let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_allow.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
}
