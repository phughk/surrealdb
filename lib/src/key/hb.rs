use crate::sql::cluster_timestamp::Timestamp;
use derive::Key;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Hb {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	_d: u8,
	pub hb: Timestamp,
	pub nd: Uuid,
}

pub fn new(hb: Timestamp, nd: &Uuid) -> Hb {
	Hb::new(hb, nd.to_owned())
}

impl Hb {
	pub fn new(hb: Timestamp, nd: Uuid) -> Self {
		Self {
			__: 0x2f, // /
			_a: 0x21, // !
			_b: 0x68, // h
			_c: 0x62, // b
			hb,
			_d: 0x2f, // /
			nd,
		}
	}
}

impl From<Timestamp> for Hb {
	fn from(ts: Timestamp) -> Self {
		let empty_uuid = uuid::Uuid::nil();
			Hb::new(
				Timestamp {
					value: 0, // We want to delete everything from start
				},
				empty_uuid,
			)
		Self::new(ts, Uuid::new_v4())
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
            let val = Hb::new(
            Timestamp { value: 123 },
            Uuid::default(),
        );
		let enc = Hb::encode(&val).unwrap();
		let dec = Hb::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}