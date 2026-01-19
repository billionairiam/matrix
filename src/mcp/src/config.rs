use std::env;
use std::time::Duration;

// 常量定义
const DEFAULT_TIMEOUT_SECS: u64 = 120;
const DEFAULT_RETRY_WAIT_SECS: u64 = 2;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_TEMPERATURE: f32 = 0.5;

// Config client configuration (centralized management of all configurations)
#[derive(Debug, Clone)]
pub struct Config {
    // Provider configuration
    pub provider: String,
    pub api_key: String,
    pub base_url: String,
    pub model: String,

    // Behavior configuration
    pub max_tokens: u32,
    pub temperature: f32,
    pub use_full_url: bool,

    // Retry configuration
    pub max_retries: u32,
    pub retry_wait_base: Duration,
    pub retryable_errors: Vec<String>,

    // Timeout configuration
    pub timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);

        let retryable_errors = vec![
            "EOF".to_string(),
            "timeout".to_string(),
            "connection reset".to_string(),
            "connection refused".to_string(),
            "temporary failure".to_string(),
            "no such host".to_string(),
            "stream error".to_string(),
            "INTERNAL_ERROR".to_string(),
        ];

        Self {
            // Provider defaults (empty by default, to be set later or via env)
            provider: String::new(),
            api_key: String::new(),
            base_url: String::new(),
            model: String::new(),

            // Environment variable lookups
            max_tokens: get_env_u32("AI_MAX_TOKENS", 2000),

            // Constants
            temperature: DEFAULT_TEMPERATURE,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_wait_base: Duration::from_secs(DEFAULT_RETRY_WAIT_SECS),
            use_full_url: false,

            retryable_errors,
            timeout,
        }
    }
}

fn get_env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[allow(dead_code)]
fn get_env_string(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
