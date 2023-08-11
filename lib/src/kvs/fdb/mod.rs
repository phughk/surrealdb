#![cfg(feature = "kv-fdb")]

use futures::TryStreamExt;

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{u64_to_versionstamp, Versionstamp};
use std::ops::Range;
use std::sync::Arc;
// We use it to work-around the fact that foundationdb-rs' Transaction
// have incompatible lifetimes for the cancel and the commit methods.
// More concretely, fdb-rs's cancel/commit takes the receiver as just `self`,
// which result in it moves and drops the receiver on the function call,
// which results in a compile error on cancel/commit that takes the self as `&mut self` which doesn't drop
// self or the fdb-rs Transaction it contains.
//
// We use mutex from the futures crate instead of the std's due to https://rust-lang.github.io/wg-async/vision/submitted_stories/status_quo/alan_thinks_he_needs_async_locks.html.
use foundationdb::options::MutationType;
use futures::lock::Mutex;
use once_cell::sync::Lazy;

// In case you're curious why FDB store doesn't work as you've expected,
// run a few queries via surrealdb-sql or via the REST API, and
// run the following command to what have been saved to FDB:
//   fdbcli --exec 'getrangekeys \x00 \xff'
pub struct Datastore {
	db: foundationdb::Database,
	_fdbnet: Arc<foundationdb::api::NetworkAutoStop>,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	lock: bool,
	// The distributed datastore transaction
	tx: Arc<Mutex<Option<foundationdb::Transaction>>>,
}

impl Drop for Transaction {
	fn drop(&mut self) {
		if !self.ok && !self.rw {
			trace!("Aborting transaction as it was incomplete and dropped");
			let mut counter = 0u16;
			loop {
				let tx = self.tx.try_lock();
				if let Some(mut mutex) = tx {
					if let Some(tx) = mutex.take() {
						trace!("Aborted transaction after {} retries", counter);
						tx.cancel();
						break;
					}
				}
				counter += 1;
			}
		}
	}
}

impl Datastore {
	/// Open a new database
	///
	/// path must be an empty string or a local file path to a FDB cluster file.
	/// An empty string results in using the default cluster file placed
	/// at a system-dependent location defined by FDB.
	/// See https://apple.github.io/foundationdb/administration.html#default-cluster-file for more information on that.
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		static FDBNET: Lazy<Arc<foundationdb::api::NetworkAutoStop>> =
			Lazy::new(|| Arc::new(unsafe { foundationdb::boot() }));
		let _fdbnet = (*FDBNET).clone();

		match foundationdb::Database::from_path(path) {
			Ok(db) => Ok(Datastore {
				db,
				_fdbnet,
			}),
			Err(e) => Err(Error::Ds(e.to_string())),
		}
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: bool, lock: bool) -> Result<Transaction, Error> {
		match self.db.create_trx() {
			Ok(tx) => Ok(Transaction {
				ok: false,
				rw: write,
				lock,
				tx: Arc::new(Mutex::new(Some(tx))),
			}),
			Err(e) => Err(Error::Tx(e.to_string())),
		}
	}
}

impl Transaction {
	/// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
	}
	/// We use lock=true to enable the tikv's own pessimistic tx (https://docs.pingcap.com/tidb/v4.0/pessimistic-transaction)
	/// for tikv kvs.
	/// FDB's standard transaction(snapshot=false) behaves like a tikv perssimistic tx
	/// by automatically retrying on conflict at the fdb client layer.
	/// So in fdb kvs we assume that lock=true is basically a request to
	/// use the standard fdb tx to make transactions Serializable.
	/// In case the tx is rw, we assume the user never wants to lose serializability
	/// so we go with the standard fdb serializable tx in that case too.
	fn snapshot(&self) -> bool {
		!self.rw && !self.lock
	}
	/// Cancel a transaction
	pub async fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		//
		// To overcome the limitation in the rust fdb client that
		// it's `cancel` and `commit` methods require you to move the
		// whole tx object to the method, we wrap it inside a Arc<Mutex<Option<_>>>
		// so that we can atomically `take` the tx out of the container and
		// replace it with the new `reset`ed tx.
		let tx = match self.tx.lock().await.take() {
			Some(tx) => {
				let tc = tx.cancel();
				tc.reset()
			}
			_ => return Err(Error::Ds("Unexpected error".to_string())),
		};
		self.tx = Arc::new(Mutex::new(Some(tx)));
		// Continue
		Ok(())
	}
	/// Commit a transaction
	pub async fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Mark this transaction as done
		self.ok = true;
		// Cancel this transaction
		//
		// To overcome the limitation in the rust fdb client that
		// it's `cancel` and `commit` methods require you to move the
		// whole tx object to the method, we wrap it inside a Arc<Mutex<Option<_>>>
		// so that we can atomically `take` the tx out of the container and
		// replace it with the new `reset`ed tx.
		let r = match self.tx.lock().await.take() {
			Some(tx) => tx.commit().await,
			_ => return Err(Error::Ds("Unexpected error".to_string())),
		};
		match r {
			Ok(_r) => {}
			Err(e) => {
				return Err(Error::Tx(format!("Transaction commit error: {}", e)));
			}
		}
		// Continue
		Ok(())
	}
	/// Check if a key exists
	pub async fn exi<K>(&mut self, key: K) -> Result<bool, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check the key
		let key: Vec<u8> = key.into();
		let key: &[u8] = &key[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		tx.get(key, self.snapshot())
			.await
			.map(|v| v.is_some())
			.map_err(|e| Error::Tx(format!("Unable to get kv from FoundationDB: {}", e)))
	}
	/// Fetch a key from the database
	pub async fn get<K>(&mut self, key: K) -> Result<Option<Val>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Get the key
		let key: Vec<u8> = key.into();
		let key = &key[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		tx.get(key, self.snapshot())
			.await
			.map(|v| v.as_ref().map(|v| v.to_vec()))
			.map_err(|e| Error::Tx(format!("Unable to get kv from FoundationDB: {}", e)))
	}
	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[allow(unused)]
	pub async fn get_timestamp(&mut self) -> Result<Versionstamp, Error> {
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		let res = tx
			.get_read_version()
			.await
			.map_err(|e| Error::Tx(format!("Unable to get read version from FDB: {}", e)))?;
		let res: u64 = res.try_into().unwrap();
		let res = u64_to_versionstamp(res);

		// Return the uint64 representation of the timestamp as the result
		Ok(res)
	}
	/// Inserts or update a key in the database
	pub async fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Set the key
		let key: Vec<u8> = key.into();
		let key = &key[..];
		let val: Vec<u8> = val.into();
		let val = &val[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		tx.set(key, val);
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	///
	/// This function is used when the client sent a CREATE query,
	/// where the key is derived from namespace, database, table name,
	/// and either an auto-generated record ID or a the record ID specified by the client
	/// after the colon in the CREATE query's first argument.
	///
	/// Suppose you've sent a query like `CREATE author:john SET ...` with
	/// the namespace `test` and the database `test`-
	/// You'll see SurrealDB sets a value to the key `/*test\x00*test\x00*author\x00*\x00\x00\x00\x01john\x00`.
	pub async fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		let key: Vec<u8> = key.into();
		if self.exi(key.clone().as_slice()).await? {
			return Err(Error::TxKeyAlreadyExists);
		}
		// Set the key
		let key: &[u8] = &key[..];
		let val: Vec<u8> = val.into();
		let val: &[u8] = &val[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		tx.set(key, val);
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
	pub async fn putc<K, V>(&mut self, key: K, val: V, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Get the key
		let key: Vec<u8> = key.into();
		let key: &[u8] = key.as_slice();
		// Get the val
		let val: Vec<u8> = val.into();
		let val: &[u8] = val.as_slice();
		// Get the check
		let chk = chk.map(Into::into);
		// Delete the key
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		let res = tx.get(key, false).await;
		let res = res.map_err(|e| Error::Tx(format!("Unable to get kv from FoundationDB: {}", e)));
		match (res, chk) {
			(Ok(Some(v)), Some(w)) if *v.as_ref() == w => tx.set(key, val),
			(Ok(None), None) => tx.set(key, val),
			(Err(e), _) => return Err(e),
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}
	// Sets the value for a versionstamped key prefixed with the user-supplied key.
	pub async fn set_versionstamped_key<K, V>(
		&mut self,
		prefix: K,
		suffix: K,
		val: V,
	) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Set the key
		let mut k: Vec<u8> = prefix.into();
		let pos = k.len();
		let pos: u32 = pos.try_into().unwrap();
		// The incomplete versionstamp is 10 bytes long.
		// See the documentation of SetVersionstampedKey for more information.
		let mut ts_placeholder: Vec<u8> =
			vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
		k.append(&mut ts_placeholder);
		k.append(&mut suffix.into());
		// FDB's SetVersionstampedKey expects the parameter, the start position of the 10-bytes placeholder
		// to be replaced by the versionstamp, to be in little endian.
		let mut posbs: Vec<u8> = pos.to_le_bytes().to_vec();
		k.append(&mut posbs);

		let key: &[u8] = &k[..];
		let val: Vec<u8> = val.into();
		let val: &[u8] = &val[..];
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		tx.atomic_op(key, val, MutationType::SetVersionstampedKey);
		// Return result
		Ok(())
	}
	/// Delete a key
	pub async fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		// Delete the key
		let key: Vec<u8> = key.into();
		let key: &[u8] = key.as_slice();
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		tx.clear(key);
		// Return result
		Ok(())
	}
	/// Delete a key
	pub async fn delc<K, V>(&mut self, key: K, chk: Option<V>) -> Result<(), Error>
	where
		K: Into<Key>,
		V: Into<Val>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Check to see if transaction is writable
		if !self.rw {
			return Err(Error::TxReadonly);
		}
		let key: Vec<u8> = key.into();
		let key: &[u8] = key.as_slice();
		// Get the check
		let chk: Option<Val> = chk.map(Into::into);
		// Delete the key
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		let res = tx
			.get(key, false)
			.await
			.map_err(|e| Error::Tx(format!("FoundationDB tx failure: {}", e)));
		match (res, chk) {
			(Ok(Some(v)), Some(w)) if *v.as_ref() == w => tx.clear(key),
			(Ok(None), None) => tx.clear(key),
			_ => return Err(Error::TxConditionNotMet),
		};
		// Return result
		Ok(())
	}
	/// Retrieve a range of keys from the databases
	pub async fn scan<K>(&mut self, rng: Range<K>, limit: u32) -> Result<Vec<(Key, Val)>, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Scan the keys
		let begin: Vec<u8> = rng.start;
		let end: Vec<u8> = rng.end;
		let opt = foundationdb::RangeOption {
			limit: Some(limit.try_into().unwrap()),
			..foundationdb::RangeOption::from((begin.as_slice(), end.as_slice()))
		};
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Assuming the `lock` argument passed to the datastore creation function
		// is meant for conducting a pessimistic lock on the underlying kv store to
		// make the transaction serializable, we use the inverse of it to enable the snapshot isolation
		// on the get request.
		// See https://apple.github.io/foundationdb/api-c.html#snapshot-reads for more information on how the snapshot get is supposed to work in FDB.
		let mut stream = tx.get_ranges_keyvalues(opt, self.snapshot());
		let mut res: Vec<(Key, Val)> = vec![];
		loop {
			let x = stream.try_next().await;
			match x {
				Ok(Some(v)) => {
					let x = (Key::from(v.key()), Val::from(v.value()));
					res.push(x)
				}
				Ok(None) => break,
				Err(e) => return Err(Error::Tx(format!("GetRanges failed: {}", e))),
			}
		}
		Ok(res)
	}
}
