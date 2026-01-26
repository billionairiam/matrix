use std::error::Error;

use reqwest::Client;
use tracing::{info, instrument};

pub struct SetHttpClientResult {
    /// Used Box<dyn Error ...> to store any type of error dynamically
    pub err: Option<Box<dyn Error + Send + Sync>>,

    /// reqwest::Client is internally reference counted (Arc), so it is cheap to clone/share.
    pub client: Option<Client>,
}

impl SetHttpClientResult {
    /// Constructor (Optional, for convenience)
    pub fn new(client: Option<Client>, err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self { client, err }
    }

    #[instrument(skip(self))]
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        if let Some(e) = &self.err {
            info!("⚠️ Error executing SetHttpClientResult: {}", e);
            return Some(e.as_ref());
        }
        None
    }

    pub fn get_result(&self) -> Option<Client> {
        let _ = self.error();

        self.client.clone()
    }
}
