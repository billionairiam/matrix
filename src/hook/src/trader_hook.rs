use std::error::Error;

use binance::futures::account::FuturesAccount;
use reqwest::Client as HttpClient;
use tracing::{info, instrument, warn};

pub struct NewBinanceTraderResult {
    pub err: Option<Box<dyn Error + Send + Sync>>,
    pub client: Option<FuturesAccount>,
}

impl NewBinanceTraderResult {
    /// Helper constructor
    pub fn new(client: Option<FuturesAccount>, err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self { client, err }
    }

    #[instrument(skip_all)]
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        if let Some(e) = &self.err {
            info!("⚠️ Error executing NewBinanceTraderResult: {}", e);
            return Some(e.as_ref());
        }
        None
    }

    pub fn get_result(&self) -> Option<FuturesAccount> {
        // Trigger logging side-effect
        let _ = self.error();

        self.client.clone()
    }
}

pub struct NewAsterTraderResult {
    pub err: Option<Box<dyn Error + Send + Sync>>,
    pub client: Option<HttpClient>,
}

impl NewAsterTraderResult {
    pub fn new(client: Option<HttpClient>, err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self { client, err }
    }

    #[instrument(skip_all)]
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        if let Some(e) = &self.err {
            warn!("⚠️ Error executing NewAsterTraderResult: {}", e);
            return Some(e.as_ref());
        }
        None
    }

    pub fn get_result(&self) -> Option<HttpClient> {
        let _ = self.error();

        self.client.clone()
    }
}
