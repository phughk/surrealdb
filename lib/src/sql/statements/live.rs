use crate::ctx::Context;
use crate::dbs::{Auth, Options};
use crate::dbs::{Level, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::IResult;
use crate::sql::fetch::{fetch, Fetchs};
use crate::sql::field::{fields, Fields};
use crate::sql::param::param;
use crate::sql::table::table;
use crate::sql::value::Value;
use crate::sql::{Statement, Uuid};
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::preceded;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: uuid::Uuid,
	pub expr: Fields,
	pub what: Value,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,

	// Non-query properties that are necessary for storage or otherwise carrying information

	// When a live query is archived, this should be the node ID that archived the query.
	pub archived: Option<uuid::Uuid>,
	// A live query is run with permissions, and we must validate that during the run.
	// It is optional, because the live query may be constructed without it being set.
	// It is populated during compute.
	pub auth: Option<Auth>,
}

impl LiveStatement {
	pub(crate) fn augment(&self, ctx: &Context, opt: &Options) -> Result<Statement, Error> {
		let copy = LiveStatement {
			id: self.id.clone(),
			node: self.node,
			expr: self.expr.clone(),
			what: self.what.clone(),
			cond: self.cond.clone(),
			fetch: self.fetch.clone(),
			archived: self.archived.clone(),
			auth: Some(opt.auth.as_ref().clone()),
		};
		Ok(Statement::Live(copy))
	}
}

impl LiveStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.realtime()?;
		// Selected DB?
		opt.needs(Level::Db)?;
		// Allowed to run?
		opt.check(Level::No)?;
		// Check that auth has been set
		self.auth.as_ref().ok_or(Error::UnknownAuth)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Process the live query table
		match self.what.compute(ctx, opt, txn, doc).await? {
			Value::Table(tb) => {
				// Clone the current statement
				let mut stm = self.clone();
				// Store the current Node ID
				if let Err(e) = opt.id() {
					trace!("No ID for live query {:?}, error={:?}", stm, e)
				}
				stm.node = opt.id()?;
				// Insert the node live query
				let key = crate::key::node::lq::new(opt.id()?, self.id.0, opt.ns(), opt.db());
				run.putc(key, tb.as_str(), None).await?;
				// Insert the table live query
				let key = crate::key::table::lq::new(opt.ns(), opt.db(), &tb, self.id.0);
				run.putc(key, stm, None).await?;
			}
			v => {
				return Err(Error::LiveStatement {
					value: v.to_string(),
				})
			}
		};
		// Return the query id
		Ok(self.id.clone().into())
	}

	pub(crate) fn archive(mut self, node_id: uuid::Uuid) -> LiveStatement {
		self.archived = Some(node_id);
		self
	}
}

impl fmt::Display for LiveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIVE SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

pub fn live(i: &str) -> IResult<&str, LiveStatement> {
	let (i, _) = tag_no_case("LIVE SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, expr) = alt((map(tag_no_case("DIFF"), |_| Fields::default()), fields))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("FROM")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = alt((map(param, Value::from), map(table, Value::from)))(i)?;
	let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
	let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
	Ok((
		i,
		LiveStatement {
			id: Uuid::new_v4(),
			node: uuid::Uuid::new_v4(),
			expr,
			what,
			cond,
			fetch,
			archived: None,
			auth: None, // Auth is set via options in compute()
		},
	))
}
