use std::sync::Arc;

use anyhow::{Result, anyhow};
use mcp::Provider;
use mcp::client::Client;
use mcp::deepseek_client::new_deepseek_client;
use mcp::qwen_client::new_qwen_client;

use crate::config::BacktestConfig;

pub fn configure_mcp_client(
    cfg: &BacktestConfig,
    base: Option<Arc<dyn Provider>>,
) -> Option<Client> {
    let provider = cfg.ai_cfg.provider.trim().to_lowercase();

    match provider.as_str() {
        "" | "inherit" | "default" => {
            // Only update if fields are provided
            if cfg.ai_cfg.api_key.is_empty()
                || cfg.ai_cfg.base_url.is_empty()
                || cfg.ai_cfg.model.is_empty()
                || base.is_none()
            {
                return None;
            }
            let client = Client::builder(base.unwrap())
                .with_api_key(cfg.ai_cfg.api_key.as_str())
                .with_base_url(cfg.ai_cfg.base_url.as_str())
                .with_model(cfg.ai_cfg.model.as_str())
                .build();
            Some(client)
        }
        "deepseek" => {
            if cfg.ai_cfg.api_key.is_empty() {
                return None;
            }

            let client = new_deepseek_client(cfg.ai_cfg.api_key.as_str()).unwrap();
            Some(client)
        }
        "qwen" => {
            if cfg.ai_cfg.api_key.is_empty() {
                return None;
            }

            let client = new_qwen_client(cfg.ai_cfg.api_key.as_str()).unwrap();
            Some(client)
        }
        "custom" => {
            if cfg.ai_cfg.base_url.is_empty()
                || cfg.ai_cfg.api_key.is_empty()
                || cfg.ai_cfg.model.is_empty()
            {
                return None;
            }
            let client = Client::builder(base.unwrap())
                .with_api_key(cfg.ai_cfg.api_key.as_str())
                .with_base_url(cfg.ai_cfg.base_url.as_str())
                .with_model(cfg.ai_cfg.model.as_str())
                .build();
            Some(client)
        }
        _ => None,
    }
}
