use std::sync::Arc;

use crate::Provider;
use crate::builder::ClientBuilder;
use crate::config::Config;
use crate::request::Message;
use serde::Serialize;
use serde_json::Value;
use tracing::{info, warn, instrument};

#[derive(Debug, Serialize, Clone)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

#[derive(thiserror::Error, Debug)]
pub enum McpError {
    #[error("HTTP request failed: {0}")]
    RequestError(#[from] reqwest::Error),
    #[error("Serialization failed: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("API Error: {0}")]
    ApiError(String),
    #[error("Configuration Error: {0}")]
    ConfigError(String),
    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
}

pub struct Client {
    pub config: Config,
    pub http_client: reqwest::Client,
    pub provider: Arc<dyn Provider>,
}

impl Client {
    pub fn new(provider: Arc<dyn Provider>, api_key: String) -> Self {
        let mut config = Config::default();
        config.api_key = api_key;
        config.base_url = provider.default_base_url().to_string();
        config.model = provider.default_model().to_string();

        let http_client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .unwrap_or_default();

        Self {
            provider,
            config,
            http_client,
        }
    }

    // 1. Basic
    // let client = Client::builder(DeepSeekProvider)
    //     .with_deepseek_config("sk-xxx") // 或者直接 .with_api_key(...) 如果 Provider 已设定默认值
    //     .with_timeout(Duration::from_secs(60))
    //     .build();

    // // 2. Custom (假设有一个 GenericProvider 或者 CustomProvider)
    // let client = Client::builder(CustomProvider)
    //     .with_api_key("sk-yyy")
    //     .with_base_url("https://custom.api/v1")
    //     // .with_logger(Box::new(MyLogger)) // 如果开启了 logger feature
    //     .build();
    pub fn builder(provider: Arc<dyn Provider>) -> ClientBuilder {
        ClientBuilder::new(provider)
    }

    /// 设置 BaseURL (Options 模式的替代)
    pub fn with_base_url(mut self, url: &str) -> Self {
        self.config.base_url = url.to_string();
        self
    }

    /// 设置 Model
    pub fn with_model(mut self, model: &str) -> Self {
        self.config.model = model.to_string();
        self
    }

    pub fn with_api_key(mut self, api_key: &str) -> Self {
        self.config.api_key = api_key.to_string();
        self
    }

    pub async fn call_with_messages(
        &self,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<String, McpError> {
        let messages = vec![
            Message {
                role: "system".to_string(),
                content: system_prompt.to_string(),
            },
            Message {
                role: "user".to_string(),
                content: user_prompt.to_string(),
            },
        ];

        self.execute_with_retry(messages).await
    }

    #[instrument(skip_all)]
    async fn execute_with_retry(&self, messages: Vec<Message>) -> Result<String, McpError> {
        let mut last_error = None;

        for attempt in 1..=self.config.max_retries {
            if attempt > 1 {
                warn!(
                    "⚠️ Attempt {}/{} failed, retrying...",
                    attempt - 1,
                    self.config.max_retries
                );
                tokio::time::sleep(self.config.retry_wait_base * (attempt - 1)).await;
            }

            match self.execute_single_request(&messages).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    if let McpError::RequestError(ref req_err) = e {
                        if !self.provider.is_retryable(req_err) {
                            return Err(e);
                        }
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or(McpError::MaxRetriesExceeded))
    }

    #[instrument(skip_all)]
    async fn execute_single_request(&self, messages: &[Message]) -> Result<String, McpError> {
        let url = self.provider.build_url(&self.config.base_url);
        info!("📡 Requesting [{}] URL: {}, api_key: {}", self.provider.name(), url, self.config.api_key);

        let body_json = self
            .provider
            .build_request_body(&self.config, messages.to_vec());

        let mut req_builder = self.http_client.post(&url).json(&body_json);
        req_builder = self
            .provider
            .set_auth_header(req_builder, &self.config.api_key);

        let resp = req_builder.send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(McpError::ApiError(format!("Status {}: {}", status, text)));
        }

        let resp_json: Value = resp.json().await?;
        self.provider.parse_response(&resp_json)
    }
}
