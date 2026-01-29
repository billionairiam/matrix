use std::sync::Arc;

use crate::{Provider, client::Client};
use anyhow::Result;

pub const PROVIDER_CUSTOM: &str = "CUSTOM";
pub const DEFAULT_CUSTOM_BASE_URL: &str = "";
pub const DEFAULT_CUSTOM_MODEL: &str = "CUSTOM";

#[derive(Debug, Clone, Copy, Default)]
pub struct CustomProvider;

impl Provider for CustomProvider {
    fn name(&self) -> &str {
        PROVIDER_CUSTOM
    }

    fn default_base_url(&self) -> &str {
        DEFAULT_CUSTOM_BASE_URL
    }

    fn default_model(&self) -> &str {
        DEFAULT_CUSTOM_MODEL
    }
}

pub fn new_custom_client(api_key: &str, base_url: &str, model_name: &str) -> Result<Client> {
    let dp = CustomProvider;

    let client = Client::builder(Arc::new(dp))
        .with_api_key(api_key)
        .with_base_url(base_url)
        .with_model(model_name)
        .build();

    Ok(client)
}

