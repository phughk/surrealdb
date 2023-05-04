use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Get the record id
		let _ = self.id.as_ref().unwrap();
		// Loop through all index statements
		for lv in self.lv(opt, txn).await?.iter() {
			// Create a new statement
			let stm = Statement::from(lv);
			// Check LIVE SELECT where condition // TODO important for perms
			// TODO check auth tokens/scope here
			if self.check(ctx, opt, txn, &stm).await.is_err() {
				continue;
			}
			let lq_opt = Options::default(); // TODO resolve actual auth scope from KV
			if self.allow(ctx, &lq_opt, txn, &stm).await.is_err() {
				// Not allowed to view this document
			}
			// Check what type of data change this is
			if stm.is_delete() {
				// Send a DELETE notification to the WebSocket
			} else if self.is_new() {
				// Process the CREATE notification to send
				let _ = self.pluck(ctx, &lq_opt, txn, &stm).await?; // TODO the value based on the LQ. Diff vs fields
				                                    // 1. Queue CREATE notification
			} else {
				// Process the CREATE notification to send
				let _ = self.pluck(ctx, &lq_opt, txn, &stm).await?;
			};
		}
		// Carry on
		Ok(())
	}
}
