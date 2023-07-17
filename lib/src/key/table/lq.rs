//! Stores a LIVE SELECT query definition on the table
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Lq<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	#[serde(with = "uuid::serde::compact")]
	pub lv: Uuid,
}

pub fn new<'a>(ns: &'a str, db: &'a str, tb: &'a str, lv: Uuid) -> Lq<'a> {
	Lq::new(ns, db, tb, lv)
}

pub fn prefix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'q', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'l', b'q', 0xff]);
	k
}

impl<'a> Lq<'a> {
	pub fn new(ns: &'a str, db: &'a str, tb: &'a str, lv: Uuid) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'l',
			_f: b'q',
			lv,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::key::debug;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let live_query_id = Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
		let val = Lq::new("testns", "testdb", "testtb", live_query_id);
		let enc = Lq::encode(&val).unwrap();
		println!("{:?}", debug::sprint_key(&enc));
		assert_eq!(
			enc,
			b"/*testns\x00*testdb\x00*testtb\x00!lq\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
		);

		let dec = Lq::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn prefix() {
		let val = super::prefix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\x00*testdb\x00*testtb\x00!lq\x00")
	}

	#[test]
	fn suffix() {
		let val = super::suffix("testns", "testdb", "testtb");
		assert_eq!(val, b"/*testns\x00*testdb\x00*testtb\x00!lq\xff")
	}
}