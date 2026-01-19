use binance::futures::account::FuturesAccount;
use reqwest::Client as HttpClient;
use std::error::Error;

pub struct NewBinanceTraderResult {
    /// Go: Err error
    /// We use Box<dyn Error...> to catch any error type (http, binance api, etc)
    pub err: Option<Box<dyn Error + Send + Sync>>,

    /// Go: Client *futures.Client
    /// In the 'binance' crate, trading operations are in `FuturesAccount`.
    /// Note: Unlike Go, this struct is not a pointer, but it is cheap to move.
    pub client: Option<FuturesAccount>,
}

impl NewBinanceTraderResult {
    /// Helper constructor
    pub fn new(client: Option<FuturesAccount>, err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self { client, err }
    }

    /// Equivalent to: func (r *NewBinanceTraderResult) Error() error
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        if let Some(e) = &self.err {
            // Log the error (mimicking log.Printf)
            // In a real app, use: log::error!("⚠️ Error executing NewBinanceTraderResult: {}", e);
            println!("⚠️ Error executing NewBinanceTraderResult: {}", e);
            return Some(e.as_ref());
        }
        None
    }

    /// Equivalent to: func (r *NewBinanceTraderResult) GetResult() *futures.Client
    /// Returns Option<FuturesAccount>.
    /// The caller gets ownership of the client (which is fine as it's designed to be moved/cloned).
    pub fn get_result(&self) -> Option<FuturesAccount> {
        // Trigger logging side-effect
        let _ = self.error();

        // In Rust, we usually return a clone if we want to keep the Result struct valid,
        // or we consume the Result. Here we clone (FuturesAccount is cloneable).
        self.client.clone()
    }
}

pub struct NewAsterTraderResult {
    /// Go: Err error
    pub err: Option<Box<dyn Error + Send + Sync>>,

    /// Go: Client *http.Client
    pub client: Option<HttpClient>,
}

impl NewAsterTraderResult {
    pub fn new(client: Option<HttpClient>, err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self { client, err }
    }

    /// Equivalent to: func (r *NewAsterTraderResult) Error() error
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        if let Some(e) = &self.err {
            println!("⚠️ Error executing NewAsterTraderResult: {}", e);
            return Some(e.as_ref());
        }
        None
    }

    /// Equivalent to: func (r *NewAsterTraderResult) GetResult() *http.Client
    pub fn get_result(&self) -> Option<HttpClient> {
        // Trigger logging side-effect
        let _ = self.error();

        // reqwest::Client uses an internal Arc, so cloning is cheap.
        self.client.clone()
    }
}
