use crate::dbs::node::Timestamp;
use crate::err::Error;
use crate::kvs::clock::FakeClock;
use tokio::sync::RwLock;

pub struct TestContext {
	pub(crate) db: Datastore,
	// A string identifier for this context.
	// It will usually be a uuid or combination of uuid and fixed string identifier.
	// It is useful for separating test setups when environments are shared.
	pub(crate) context_id: String,
	// The clock used to control the time available in transactions
	pub(crate) clock: Arc<RwLock<FakeClock>>,
}

/// TestContext is a container for an initialised test context
/// Anything stateful (such as storage layer and logging) can be tied with this
impl TestContext {
	// Use this to generate strings that have the test uuid associated with it
	pub fn test_str(&self, prefix: &str) -> String {
		return format!("{}-{}", prefix, self.context_id);
	}
}

/// Initialise logging and prepare a useable datastore
/// In the future it would be nice to handle multiple datastores
pub(crate) async fn init(node_id: Uuid, now: Timestamp) -> Result<TestContext, Error> {
	let db = new_ds(node_id).await;
	return Ok(TestContext {
		db,
		context_id: node_id.to_string(), // The context does not always have to be a uuid
		clock: Arc::new(RwLock::new(FakeClock::new(now))),
	});
}

/// Scan the entire storage layer displaying keys
/// Useful to debug scans ;)
async fn _debug_scan(tx: &mut Transaction, message: &str) {
	let r = tx.scan(vec![0]..vec![u8::MAX], 100000).await.unwrap();
	println!("START OF RANGE SCAN - {}", message);
	for (k, _v) in r.iter() {
		println!("{}", crate::key::debug::sprint_key(k.as_ref()));
	}
	println!("END OF RANGE SCAN - {}", message);
}
