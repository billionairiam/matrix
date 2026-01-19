use anyhow::Result;

use std::sync::Arc;

use crate::{Provider, client::Client};

pub const PROVIDER_DEEPSEEK: &str = "deepseek";
pub const DEFAULT_DEEPSEEK_BASE_URL: &str = "https://api.deepseek.com";
pub const DEFAULT_DEEPSEEK_MODEL: &str = "deepseek-chat";

#[derive(Debug, Clone, Copy, Default)]
pub struct DeepseekProvider;

impl Provider for DeepseekProvider {
    fn name(&self) -> &str {
        PROVIDER_DEEPSEEK
    }

    fn default_base_url(&self) -> &str {
        DEFAULT_DEEPSEEK_BASE_URL
    }

    fn default_model(&self) -> &str {
        DEFAULT_DEEPSEEK_MODEL
    }
}

pub fn new_deepseek_client(api_key: &str) -> Result<Client> {
    let dp = DeepseekProvider;

    let client = Client::builder(Arc::new(dp)).with_api_key(api_key).build();

    Ok(client)
}
