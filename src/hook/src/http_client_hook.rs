use reqwest::Client;
use std::error::Error;

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

    /// Equivalent to: func (r *SetHttpClientResult) Error() error
    /// Checks for error and logs it if present.
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        if let Some(e) = &self.err {
            // Using println! here, but in production use the `log` crate: log::error!(...)
            log::info!("⚠️ Error executing SetHttpClientResult: {}", e);
            return Some(e.as_ref());
        }
        None
    }

    /// Equivalent to: func (r *SetHttpClientResult) GetResult() *http.Client
    /// Calls Error() for the logging side-effect, then returns the client.
    pub fn get_result(&self) -> Option<Client> {
        // Trigger the side-effect (logging) just like the Go code
        let _ = self.error();

        // return the client. reqwest::Client is cheap to clone.
        self.client.clone()
    }
}
