use serde_json::Value;
use std::fmt::Debug;

use client::{ChatRequest, McpError};
use config::Config;
use request::Message;

pub mod builder;
pub mod client;
pub mod config;
pub mod custom_client;
pub mod deepseek_client;
pub mod qwen_client;
pub mod request;
pub mod request_builder;

pub trait Provider: Send + Sync + Debug {
    fn name(&self) -> &str;

    fn default_base_url(&self) -> &str;

    fn default_model(&self) -> &str;

    fn build_url(&self, base_url: &str) -> String {
        format!("{}/chat/completions", base_url.trim_end_matches('/'))
    }

    fn set_auth_header(
        &self,
        builder: reqwest::RequestBuilder,
        api_key: &str,
    ) -> reqwest::RequestBuilder {
        builder.header("Authorization", format!("Bearer {}", api_key))
    }

    fn build_request_body(&self, config: &Config, messages: Vec<Message>) -> Value {
        let req = ChatRequest {
            model: config.model.clone(),
            messages,
            temperature: Some(0.5), // 这里可以从 Config 获取
            max_tokens: Some(2048),
        };
        serde_json::to_value(req).unwrap_or(Value::Null)
    }

    fn parse_response(&self, body: &Value) -> Result<String, McpError> {
        body.get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                McpError::ApiError("Invalid response format or empty choices".to_string())
            })
    }

    fn is_retryable(&self, err: &reqwest::Error) -> bool {
        err.is_timeout()
            || err.is_connect()
            || err.status().map(|s| s.is_server_error()).unwrap_or(false)
    }
}
