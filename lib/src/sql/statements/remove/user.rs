use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveUserStatement {
	pub name: Ident,
	pub base: Base,
}

impl RemoveUserStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match self.base {
			Base::Root => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::root::us::new(&self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Delete the definition
				let key = crate::key::namespace::us::new(opt.ns(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Delete the definition
				let key = crate::key::database::us::new(opt.ns(), opt.db(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for RemoveUserStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE USER {} ON {}", self.name, self.base)
	}
}
