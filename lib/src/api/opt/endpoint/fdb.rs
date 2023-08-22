use crate::api::engine::local::Db;
use crate::api::engine::local::FDb;
use crate::api::err::Error;
use crate::api::opt::Config;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::Result;
use std::path::Path;
use std::path::PathBuf;
use url::Url;

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<FDb> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = super::make_url("fdb", self);
					Ok(Endpoint {
						endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
						config: Default::default(),
					})
				}
			}

			impl IntoEndpoint<FDb> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = IntoEndpoint::<FDb>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, &Path, PathBuf);
