use crate::ctx::Context;
use crate::dbs::Notification;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::{Action, Transaction};
use crate::doc::CursorDoc;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::permission::Permission;
use crate::sql::Value;
use std::ops::Deref;
use std::sync::Arc;

impl<'a> Document<'a> {
	pub async fn lives(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Check if we can send notifications
		println!("CHECKING LIVES");
		if let Some(chn) = &opt.sender {
			// Clone the sending channel
			let chn = chn.clone();
			// Loop through all index statements
			for lv in self.lv(opt, txn).await?.iter() {
				// Create a new statement
				let lq = Statement::from(lv);
				// Get the event action
				let met = if stm.is_delete() {
					Value::from("DELETE")
				} else if self.is_new() {
					Value::from("CREATE")
				} else {
					Value::from("UPDATE")
				};
				// Check if this is a delete statement
				let doc = match stm.is_delete() {
					true => &self.initial,
					false => &self.current,
				};
				// Ensure that a session exists on the LIVE query
				let sess = match lv.session.as_ref() {
					Some(v) => v,
					None => continue,
				};
				// Ensure that auth info exists on the LIVE query
				let auth = match lv.auth.clone() {
					Some(v) => v,
					None => continue,
				};
				// We need to create a new context which we will
				// use for processing this LIVE query statement.
				// This ensures that we are using the session
				// of the user who created the LIVE query.
				let mut lqctx = Context::background().with_live_value(sess.clone());
				// We need to create a new options which we will
				// use for processing this LIVE query statement.
				// This ensures that we are using the auth data
				// of the user who created the LIVE query.
				let lqopt = opt.new_with_perms(true).with_auth(Arc::from(auth));
				// Add $before, $after, $value, and $event params
				// to this LIVE query so that user can use these
				// within field projections and WHERE clauses.
				lqctx.add_value("event", met);
				lqctx.add_value("value", self.current.doc.deref());
				lqctx.add_value("after", self.current.doc.deref());
				lqctx.add_value("before", self.initial.doc.deref());
				// First of all, let's check to see if the WHERE
				// clause of the LIVE query is matched by this
				// document. If it is then we can continue.
				match self.lq_check(&lqctx, &lqopt, txn, &lq, doc).await {
					Err(Error::Ignore) => continue,
					Err(e) => return Err(e),
					Ok(_) => (),
				}
				// Secondly, let's check to see if any PERMISSIONS
				// clause for this table allows this document to
				// be viewed by the user who created this LIVE
				// query. If it does, then we can continue.
				match self.lq_allow(&lqctx, &lqopt, txn, &lq, doc).await {
					Err(Error::Ignore) => continue,
					Err(e) => return Err(e),
					Ok(_) => (),
				}
				// Finally, let's check what type of statement
				// caused this LIVE query to run, and send the
				// relevant notification based on the statement.
				let mut tx = txn.lock().await;
				let ts = tx.clock().await;
				let not_id = crate::sql::Uuid::new_v4();
				if stm.is_delete() {
					// Send a DELETE notification
					let thing = (*rid).clone();
					let notification = Notification {
						live_id: lv.id.clone(),
						node_id: lv.node.clone(),
						notification_id: not_id.clone(),
						action: Action::Delete,
						result: Value::Thing(thing),
						timestamp: ts.clone(),
					};
					if opt.id()? == lv.node.0 {
						let previous_nots = tx
							.scan_tbnt(
								opt.ns(),
								opt.db(),
								&self.id.unwrap().tb,
								lv.id.clone(),
								1000,
							)
							.await;
						match previous_nots {
							Ok(nots) => {
								for not in nots {
									if let Err(e) = chn.write().await.send(not).await {
										error!("Error sending scanned notification: {}", e);
									}
								}
							}
							Err(err) => {
								error!("Error scanning notifications: {}", err);
							}
						}
						chn.write().await.send(notification).await?;
					} else {
						tx.putc_tbnt(
							opt.ns(),
							opt.db(),
							&self.id.unwrap().tb,
							lv.id.clone(),
							ts,
							not_id,
							notification,
							None,
						)
						.await?;
					}
				} else if self.is_new() {
					// Send a CREATE notification
					let plucked = self.pluck(_ctx, opt, txn, &lq).await?;
					let notification = Notification {
						live_id: lv.id.clone(),
						node_id: lv.node.clone(),
						notification_id: not_id.clone(),
						action: Action::Create,
						result: plucked,
						timestamp: ts.clone(),
					};
					println!("\n\nCREATE NOTIFICATION: {:?}\n\n", notification);
					if opt.id()? == lv.node.0 {
						println!("LV node {} was same as {}", lv.node.0, opt.id()?);
						let previous_nots = tx
							.scan_tbnt(
								opt.ns(),
								opt.db(),
								&self.id.unwrap().tb,
								lv.id.clone(),
								1000,
							)
							.await;
						match previous_nots {
							Ok(nots) => {
								println!("Found {} notifications in create", nots.len());
								let channel = chn.write().await;
								for not in &nots {
									if let Err(e) = channel.send(not.clone()).await {
										println!("Error sending scanned notification: {}", e);
										error!("Error sending scanned notification: {}", e);
									} else {
										println!("Sent notification: {:?}", not);
									}
								}
							}
							Err(err) => {
								println!("Error scanning notifications: {}", err);
								error!("Error scanning notifications: {}", err);
							}
						}
						chn.write().await.send(notification).await?;
					} else {
						println!(
							"LV node {} was not same as {}. Putting {}",
							lv.node.0,
							opt.id()?,
							notification
						);
						tx.putc_tbnt(
							opt.ns(),
							opt.db(),
							&self.id.unwrap().tb,
							lv.id.clone(),
							ts,
							not_id,
							notification,
							None,
						)
						.await?;
					}
				} else {
					// Send a UPDATE notification
					let notification = Notification {
						live_id: lv.id.clone(),
						node_id: lv.node.clone(),
						notification_id: not_id.clone(),
						action: Action::Update,
						result: self.pluck(_ctx, opt, txn, &lq).await?,
						timestamp: ts.clone(),
					};
					if opt.id()? == lv.node.0 {
						let previous_nots = tx
							.scan_tbnt(
								opt.ns(),
								opt.db(),
								&self.id.unwrap().tb,
								lv.id.clone(),
								1000,
							)
							.await;
						match previous_nots {
							Ok(nots) => {
								for not in nots {
									if let Err(e) = chn.write().await.send(not).await {
										error!("Error sending scanned notification: {}", e);
									}
								}
							}
							Err(err) => {
								error!("Error scanning notifications: {}", err);
							}
						}
						chn.write().await.send(notification).await?;
					} else {
						tx.putc_tbnt(
							opt.ns(),
							opt.db(),
							&self.id.unwrap().tb,
							lv.id.clone(),
							ts,
							not_id,
							notification,
							None,
						)
						.await?;
					}
				};
			}
		}
		// Carry on
		Ok(())
	}
	/// Check the WHERE clause for a LIVE query
	async fn lq_check(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		doc: &CursorDoc<'_>,
	) -> Result<(), Error> {
		// Check where condition
		if let Some(cond) = stm.conds() {
			// Check if the expression is truthy
			if !cond.compute(ctx, opt, txn, Some(doc)).await?.is_truthy() {
				// Ignore this document
				return Err(Error::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
	/// Check any PERRMISSIONS for a LIVE query
	async fn lq_allow(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
		doc: &CursorDoc<'_>,
	) -> Result<(), Error> {
		// Should we run permissions checks?
		if opt.check_perms(stm.into()) {
			// Get the table
			let tb = self.tb(opt, txn).await?;
			// Process the table permissions
			match &tb.permissions.select {
				Permission::None => return Err(Error::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !e.compute(ctx, opt, txn, Some(doc)).await?.is_truthy() {
						return Err(Error::Ignore);
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
