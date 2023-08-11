#![cfg(feature = "kv-speedb")]

use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::vs::{try_to_u64_be, u64_to_versionstamp, Versionstamp};
use futures::lock::Mutex;
use speedb::{OptimisticTransactionDB, OptimisticTransactionOptions, ReadOptions, WriteOptions};
use std::backtrace::{Backtrace, BacktraceStatus};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone)]
pub struct Datastore {
	db: Pin<Arc<OptimisticTransactionDB>>,
}

pub struct Transaction {
	// Is the transaction complete?
	ok: bool,
	// Is the transaction read+write?
	rw: bool,
	// The distributed datastore transaction
	tx: Arc<Mutex<Option<speedb::Transaction<'static, OptimisticTransactionDB>>>>,
	// The read options containing the Snapshot
	ro: ReadOptions,
	// the above, supposedly 'static, transaction actually points here, so keep the memory alive
	// note that this is dropped last, as it is declared last
	_db: Pin<Arc<OptimisticTransactionDB>>,
}

impl Drop for Transaction {
	fn drop(&mut self) {
		if !self.ok && self.rw {
			warn!("A write transaction was dropped without being resolved");
			let backtrace = Backtrace::force_capture();
			if let BacktraceStatus::Captured = backtrace.status() {
				// printing the backtrace is prettier than logging individual entries in trace
				println!("{}", backtrace);
			}
			#[cfg(debug_assertions)]
			panic!("Panicking because of a transaction that was not handled correctly");
		}
	}
}

impl Datastore {
	/// Open a new database
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		Ok(Datastore {
			db: Arc::pin(OptimisticTransactionDB::open_default(path)?),
		})
	}
	/// Start a new transaction
	pub async fn transaction(&self, write: bool, _: bool) -> Result<Transaction, Error> {
		// Activate the snapshot options
		let mut to = OptimisticTransactionOptions::default();
		to.set_snapshot(true);
		// Create a new transaction
		let tx = self.db.transaction_opt(&WriteOptions::default(), &to);
		// The database reference must always outlive
		// the transaction. If it doesn't then this
		// is undefined behaviour. This unsafe block
		// ensures that the transaction reference is
		// static, but will cause a crash if the
		// datastore is dropped prematurely.
		let tx = unsafe {
			std::mem::transmute::<
				speedb::Transaction<'_, OptimisticTransactionDB>,
				speedb::Transaction<'static, OptimisticTransactionDB>,
			>(tx)
		};
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&tx.snapshot());
		// Return the transaction
		Ok(Transaction {
			ok: false,
			rw: write,
			tx: Arc::new(Mutex::new(Some(tx))),
			ro,
			_db: self.db.clone(),
		})
	}
}

impl Transaction {
	/// Check if closed
	pub fn closed(&self) -> bool {
		self.ok
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
		match self.tx.lock().await.take() {
			Some(tx) => tx.rollback()?,
			None => unreachable!(),
		};
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
		match self.tx.lock().await.take() {
			Some(tx) => tx.commit()?,
			None => unreachable!(),
		};
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
		let res = self.tx.lock().await.as_ref().unwrap().get_opt(key.into(), &self.ro)?.is_some();
		// Return result
		Ok(res)
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
		let res = self.tx.lock().await.as_ref().unwrap().get_opt(key.into(), &self.ro)?;
		// Return result
		Ok(res)
	}
	/// Obtain a new change timestamp for a key
	/// which is replaced with the current timestamp when the transaction is committed.
	/// NOTE: This should be called when composing the change feed entries for this transaction,
	/// which should be done immediately before the transaction commit.
	/// That is to keep other transactions commit delay(pessimistic) or conflict(optimistic) as less as possible.
	#[allow(unused)]
	pub async fn get_timestamp<K>(&mut self, key: K) -> Result<Versionstamp, Error>
	where
		K: Into<Key>,
	{
		// Check to see if transaction is closed
		if self.ok {
			return Err(Error::TxFinished);
		}
		// Write the timestamp to the "last-write-timestamp" key
		// to ensure that no other transactions can commit with older timestamps.
		let k: Key = key.into();
		let prev = self.tx.lock().await.as_ref().unwrap().get_opt(k.clone(), &self.ro)?;
		let ver = match prev {
			Some(prev) => {
				let slice = prev.as_slice();
				let res: Result<[u8; 10], Error> = match slice.try_into() {
					Ok(ba) => Ok(ba),
					Err(e) => Err(Error::Ds(e.to_string())),
				};
				let array = res?;
				let prev = try_to_u64_be(array)?;
				prev + 1
			}
			None => 1,
		};

		let verbytes = u64_to_versionstamp(ver);

		self.tx.lock().await.as_ref().unwrap().put(k, verbytes)?;
		// Return the uint64 representation of the timestamp as the result
		Ok(verbytes)
	}
	/// Obtain a new key that is suffixed with the change timestamp
	pub async fn get_versionstamped_key<K>(
		&mut self,
		ts_key: K,
		prefix: K,
		suffix: K,
	) -> Result<Vec<u8>, Error>
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
		let ts = self.get_timestamp(ts_key).await?;
		let mut k: Vec<u8> = prefix.into();
		k.append(&mut ts.to_vec());
		k.append(&mut suffix.into());
		Ok(k)
	}
	/// Insert or update a key in the database
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
		self.tx.lock().await.as_ref().unwrap().put(key.into(), val.into())?;
		// Return result
		Ok(())
	}
	/// Insert a key if it doesn't exist in the database
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		// Set the key if empty
		match tx.get_opt(&key, &self.ro)? {
			None => tx.put(key, val)?,
			_ => return Err(Error::TxKeyAlreadyExists),
		};
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let val = val.into();
		let chk = chk.map(Into::into);
		// Set the key if valid
		match (tx.get_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v == w => tx.put(key, val)?,
			(None, None) => tx.put(key, val)?,
			_ => return Err(Error::TxConditionNotMet),
		};
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
		// Remove the key
		self.tx.lock().await.as_ref().unwrap().delete(key.into())?;
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Get the arguments
		let key = key.into();
		let chk = chk.map(Into::into);
		// Delete the key if valid
		match (tx.get_opt(&key, &self.ro)?, chk) {
			(Some(v), Some(w)) if v == w => tx.delete(key)?,
			(None, None) => tx.delete(key)?,
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
		// Get the transaction
		let tx = self.tx.lock().await;
		let tx = tx.as_ref().unwrap();
		// Convert the range to bytes
		let rng: Range<Key> = Range {
			start: rng.start.into(),
			end: rng.end.into(),
		};
		// Create result set
		let mut res = vec![];
		// Set the key range
		let beg = rng.start.as_slice();
		let end = rng.end.as_slice();
		// Set the ReadOptions with the snapshot
		let mut ro = ReadOptions::default();
		ro.set_snapshot(&tx.snapshot());
		// Create the iterator
		let mut iter = tx.raw_iterator_opt(ro);
		// Seek to the start key
		iter.seek(&rng.start);
		// Scan the keys in the iterator
		while iter.valid() {
			// Check the scan limit
			if res.len() < limit as usize {
				// Get the key and value
				let (k, v) = (iter.key(), iter.value());
				// Check the key and value
				if let (Some(k), Some(v)) = (k, v) {
					if k >= beg && k < end {
						res.push((k.to_vec(), v.to_vec()));
						iter.next();
						continue;
					}
				}
			}
			// Exit
			break;
		}
		// Return result
		Ok(res)
	}
}
