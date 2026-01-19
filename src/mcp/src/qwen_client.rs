use anyhow::Result;

use crate::{Provider, client::Client};

use std::sync::Arc;

pub const PROVIDER_QWEN: &str = "qwen";
pub const DEFAULT_QWEN_BASE_URL: &str = "https://dashscope.aliyuncs.com/compatible-mode/v1";
pub const DEFAULT_QWEN_MODEL: &str = "qwen3-max";

#[derive(Debug, Clone, Copy, Default)]
pub struct QwenProvider;

impl Provider for QwenProvider {
    fn name(&self) -> &str {
        PROVIDER_QWEN
    }

    fn default_base_url(&self) -> &str {
        DEFAULT_QWEN_BASE_URL
    }

    fn default_model(&self) -> &str {
        DEFAULT_QWEN_MODEL
    }
}

pub fn new_qwen_client(api_key: &str) -> Result<Client> {
    let dp = QwenProvider;

    let client = Client::builder(Arc::new(dp)).with_api_key(api_key).build();

    Ok(client)
}
