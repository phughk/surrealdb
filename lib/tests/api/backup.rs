// Tests for exporting and importing data
// Supported by the storage engines and the HTTP protocol

use tokio::fs::remove_file;

#[test_log::test(tokio::test)]
async fn export_import() {
	let (permit, db) = new_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	for i in 0..10 {
		let _: Vec<RecordId> = db
			.create("user")
			.content(Record {
				name: &format!("User {i}"),
			})
			.await
			.unwrap();
	}
	drop(permit);
	let file = format!("{db_name}.sql");

	let res = async {
		db.export(&file).await?;
		db.import(&file).await?;
		Result::<(), Error>::Ok(())
	}
	.await;
	remove_file(file).await.unwrap();
	res.unwrap();
}

async fn export_import_credentials() {
	let (permit, db) = new_db().await;
	let db_name = Ulid::new().to_string();

	// TODO: confirm this works because db is already signed in
	// Create users on ns/db/sc
	let ns_creds = Namespace {
		namespace: NS,
		username: "ns-user",
		password: "ns-pass",
	};
	db.signup(ns_creds).await.unwrap();
	let db_creds = Database {
		namespace: NS,
		database: &db_name,
		username: "db-user",
		password: "db-pass",
	};
	db.signup(db_creds).await.unwrap();
	let sc_creds = Scope {
		namespace: NS,
		database: &db_name,
		scope: "scope",
		params: AuthParams {
			email: "sc@email.com",
			pass: "sc-pass",
		},
	};
	db.signup(sc_creds.clone()).await.unwrap();

	// Create some data
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	for i in 0..10 {
		let _: Vec<RecordId> = db
			.create("user")
			.content(Record {
				name: &format!("User {i}"),
			})
			.await
			.unwrap();
	}
	drop(permit);
	let file = format!("{db_name}.sql");

	let res = async {
		db.export(&file).await?;
		db.import(&file).await?;
		db.signin(ns_creds).await.unwrap();
		db.signin(db_creds).await.unwrap();
		db.signin(sc_creds).await.unwrap();
		Result::<(), Error>::Ok(())
	}
	.await;
	remove_file(file).await.unwrap();
	res.unwrap();
}

#[test_log::test(tokio::test)]
#[cfg(feature = "ml")]
async fn ml_export_import() {
	let (permit, db) = new_db().await;
	let db_name = Ulid::new().to_string();
	db.use_ns(NS).use_db(&db_name).await.unwrap();
	db.import("../tests/linear_test.surml").ml().await.unwrap();
	drop(permit);
	let file = format!("{db_name}.surml");
	db.export(&file).ml("Prediction", Version::new(0, 0, 1)).await.unwrap();
	db.import(&file).ml().await.unwrap();
	remove_file(file).await.unwrap();
}
