use crate::Provider;
use crate::client::Client;
use crate::config::Config;
use crate::deepseek_client::{
    DEFAULT_DEEPSEEK_BASE_URL, DEFAULT_DEEPSEEK_MODEL, PROVIDER_DEEPSEEK,
};
use crate::qwen_client::{DEFAULT_QWEN_BASE_URL, DEFAULT_QWEN_MODEL, PROVIDER_QWEN};

use std::sync::Arc;

use reqwest::Client as HttpClient;
use std::time::Duration;

pub struct ClientBuilder {
    config: Config,
    custom_http_client: Option<HttpClient>,
    provider: Arc<dyn Provider>,
}

impl ClientBuilder {
    pub fn new(provider: Arc<dyn Provider>) -> Self {
        let mut config = Config::default();
        config.provider = provider.name().to_string();
        config.base_url = provider.default_base_url().to_string();
        config.model = provider.default_model().to_string();

        Self {
            provider,
            config,
            custom_http_client: None,
        }
    }

    pub fn with_http_client(mut self, client: HttpClient) -> Self {
        self.custom_http_client = Some(client);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    pub fn with_retry_wait_base(mut self, wait_time: Duration) -> Self {
        self.config.retry_wait_base = wait_time;
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.config.max_tokens = max_tokens;
        self
    }

    pub fn with_temperature(mut self, temperature: f32) -> Self {
        self.config.temperature = temperature;
        self
    }

    pub fn with_api_key(mut self, api_key: &str) -> Self {
        self.config.api_key = api_key.to_string();
        self
    }

    pub fn with_base_url(mut self, base_url: &str) -> Self {
        self.config.base_url = base_url.to_string();
        self
    }

    pub fn with_model(mut self, model: &str) -> Self {
        self.config.model = model.to_string();
        self
    }

    pub fn with_use_full_url(mut self, use_full_url: bool) -> Self {
        self.config.use_full_url = use_full_url;
        self
    }

    pub fn with_deepseek_config(mut self, api_key: &str) -> Self {
        self.config.provider = PROVIDER_DEEPSEEK.to_string();
        self.config.api_key = api_key.to_string();
        self.config.base_url = DEFAULT_DEEPSEEK_BASE_URL.to_string();
        self.config.model = DEFAULT_DEEPSEEK_MODEL.to_string();
        self
    }

    pub fn with_qwen_config(mut self, api_key: &str) -> Self {
        self.config.provider = PROVIDER_QWEN.to_string();
        self.config.api_key = api_key.to_string();
        self.config.base_url = DEFAULT_QWEN_BASE_URL.to_string();
        self.config.model = DEFAULT_QWEN_MODEL.to_string();
        self
    }

    pub fn build(self) -> Client {
        let http_client = if let Some(client) = self.custom_http_client {
            client
        } else {
            HttpClient::builder()
                .timeout(self.config.timeout)
                .build()
                .unwrap_or_default()
        };

        Client {
            config: self.config,
            http_client: http_client,
            provider: self.provider,
        }
    }
}
