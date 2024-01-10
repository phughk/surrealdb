use crate::dbs::Response;
use crate::err::Error;
use crate::kvs::bootstrap::TxRequestOneshot;
use crate::kvs::Datastore;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::Write;
use crate::sql;
use std::sync::Arc;
use tokio::sync::mpsc;

/// A task that always gives transactions upon request
/// Task terminates once the channel is closed on the other end
pub(crate) async fn always_give_tx(
	ds: Arc<Datastore>,
	mut tx_req_channel: mpsc::Receiver<TxRequestOneshot>,
) -> Result<u32, Error> {
	let mut count = 0 as u32;
	loop {
		let req = tx_req_channel.recv().await;
		match req {
			None => break,
			Some(r) => {
				count += 1;
				let tx = ds.transaction(Write, Optimistic).await?;
				if let Err(mut tx) = r.send(tx) {
					// The other side of the channel was probably closed
					// Do not reduce count, because it was requested
					tx.cancel().await?;
				}
			}
		}
	}
	Ok(count)
}

/// Cast a response vector to a single uuid to avoid boilerplate
pub(crate) fn as_uuid(mut responses: Vec<Response>) -> sql::Uuid {
	assert_eq!(responses.len(), 1);
	let resp = responses.pop().unwrap().result;
	assert!(resp.is_ok());
	let val = resp.unwrap();
	match val {
		sql::Value::Uuid(u) => u,
		_ => panic!("Expected a Uuid"),
	}
}