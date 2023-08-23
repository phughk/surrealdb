use crate::err::Error;
use crate::iam::Auth;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;
use uuid::Uuid;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Auth>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Auth>, Error>;
	type SerializeTuple = Impossible<Option<Auth>, Error>;
	type SerializeTupleStruct = Impossible<Option<Auth>, Error>;
	type SerializeTupleVariant = Impossible<Option<Auth>, Error>;
	type SerializeMap = Impossible<Option<Auth>, Error>;
	type SerializeStruct = Impossible<Option<Auth>, Error>;
	type SerializeStructVariant = Impossible<Option<Auth>, Error>;

	const EXPECTED: &'static str = "an `Option<Auth>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(super::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Auth> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Auth::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
