use crate::kvs::{LqValue, NO_LIMIT};

#[tokio::test]
#[serial]
async fn write_scan_ndlq() {
	// TODO this test much change
	let nd = Uuid::from_str("7a17446f-721f-4855-8fc7-81086752ca44").unwrap();
	let clock = Arc::new(RwLock::new(SizedClock::Fake(FakeClock::new(Timestamp::default()))));
	let test = init(nd, clock).await.unwrap();

	// Write some data
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let ns = "namespace";
	let db = "database";
	let tb = "table";
	let lq = Uuid::from_str("4c3dca4b-ec08-4e3e-b23a-6b03b5cdc3fc").unwrap();
	tx.putc_ndlq(*nd, *lq, ns, db, tb, None).await.unwrap();
	tx.commit().await.unwrap();

	// Verify scan
	let mut tx = test.db.transaction(Write, Optimistic).await.unwrap();
	let page = NodeScanPage::new(&nd);
	let res_lim = tx.scan_ndlq(page, 100).await.unwrap().0;
	let res_no_lim = tx.scan_ndlq(page, 100).await.unwrap().0;
	tx.commit().await.unwrap();
	assert_eq!(
		res_lim,
		vec![LqValue {
			nd,
			ns: ns.to_string(),
			db: db.to_string(),
			tb: tb.to_string(),
			lq
		}]
	);
	assert_eq!(res_lim, res_no_lim);
}
