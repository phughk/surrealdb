mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
#[serial]
async fn datetimes_conversion() -> Result<(), Error> {
	let sql = r#"
		SELECT * FROM "2012-01-01";
		SELECT * FROM <datetime> "2012-01-01";
		SELECT * FROM <string> "2012-01-01T08:00:00Z" + "-test";
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			'2012-01-01'
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			'2012-01-01T00:00:00Z'
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			'2012-01-01T08:00:00Z-test'
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
