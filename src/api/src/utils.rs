use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    pub enabled: bool,
    pub api_key: String,
    pub custom_api_url: String,
    pub custom_model_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub enabled: bool,
    pub api_key: String,
    pub secret_key: String,
    pub testnet: bool,
    pub hyperliquid_wallet_addr: String,
    pub aster_user: String,
    pub aster_signer: String,
    pub aster_private_key: String,
    pub lighter_wallet_addr: String,
    pub lighter_private_key: String,
}

/// Mask sensitive strings, showing only first 4 and last 4 characters.
/// Used to mask API Key, Secret Key, Private Key, etc.
pub fn mask_sensitive_string(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }

    // Note: len() returns byte length.
    // Assuming API keys are ASCII-compatible. If using emojis/multibyte chars,
    // this logic needs 'chars().count()', but for keys byte-len is standard.
    let len = s.len();

    if len <= 8 {
        return "****".to_string(); // String too short, hide everything
    }

    format!("{}****{}", &s[0..4], &s[len - 4..])
}

/// Mask email address, keeping first 2 characters and domain part
pub fn mask_email(email: &str) -> String {
    if email.is_empty() {
        return String::new();
    }

    // split_once is more efficient/safer than collecting a split vector
    match email.split_once('@') {
        Some((username, domain)) => {
            if username.len() <= 2 {
                format!("**@{}", domain)
            } else {
                format!("{}****@{}", &username[0..2], domain)
            }
        }
        None => "****".to_string(), // Incorrect format
    }
}

/// Sanitize model configuration for log output
pub fn sanitize_model_config_for_log(models: &HashMap<String, ModelConfig>) -> Value {
    let mut safe_map = Map::new();

    for (model_id, cfg) in models {
        let entry = json!({
            "enabled": cfg.enabled,
            "api_key": mask_sensitive_string(&cfg.api_key),
            "custom_api_url": cfg.custom_api_url,
            "custom_model_name": cfg.custom_model_name,
        });
        safe_map.insert(model_id.clone(), entry);
    }

    Value::Object(safe_map)
}

/// Sanitize exchange configuration for log output
pub fn sanitize_exchange_config_for_log(exchanges: &HashMap<String, ExchangeConfig>) -> Value {
    let mut safe_map = Map::new();

    for (exchange_id, cfg) in exchanges {
        // Base structure
        let mut safe_exchange = Map::new();
        safe_exchange.insert("enabled".to_string(), json!(cfg.enabled));
        safe_exchange.insert("testnet".to_string(), json!(cfg.testnet));

        // Mask Sensitive fields
        if !cfg.api_key.is_empty() {
            safe_exchange.insert(
                "api_key".to_string(),
                json!(mask_sensitive_string(&cfg.api_key)),
            );
        }
        if !cfg.secret_key.is_empty() {
            safe_exchange.insert(
                "secret_key".to_string(),
                json!(mask_sensitive_string(&cfg.secret_key)),
            );
        }
        if !cfg.aster_private_key.is_empty() {
            safe_exchange.insert(
                "aster_private_key".to_string(),
                json!(mask_sensitive_string(&cfg.aster_private_key)),
            );
        }
        if !cfg.lighter_private_key.is_empty() {
            safe_exchange.insert(
                "lighter_private_key".to_string(),
                json!(mask_sensitive_string(&cfg.lighter_private_key)),
            );
        }

        // Add non-sensitive fields directly
        if !cfg.hyperliquid_wallet_addr.is_empty() {
            safe_exchange.insert(
                "hyperliquid_wallet_addr".to_string(),
                json!(cfg.hyperliquid_wallet_addr),
            );
        }
        if !cfg.aster_user.is_empty() {
            safe_exchange.insert("aster_user".to_string(), json!(cfg.aster_user));
        }
        if !cfg.aster_signer.is_empty() {
            safe_exchange.insert("aster_signer".to_string(), json!(cfg.aster_signer));
        }
        if !cfg.lighter_wallet_addr.is_empty() {
            safe_exchange.insert(
                "lighter_wallet_addr".to_string(),
                json!(cfg.lighter_wallet_addr),
            );
        }

        safe_map.insert(exchange_id.clone(), Value::Object(safe_exchange));
    }

    Value::Object(safe_map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_sensitive_string() {
        assert_eq!(mask_sensitive_string(""), "");
        assert_eq!(mask_sensitive_string("123"), "****");
        assert_eq!(mask_sensitive_string("12345678"), "****");
        assert_eq!(mask_sensitive_string("1234567890"), "1234****7890");
        assert_eq!(
            mask_sensitive_string("abcdef1234567890abcdef"),
            "abcd****cdef"
        );
    }

    #[test]
    fn test_mask_email() {
        assert_eq!(mask_email(""), "");
        assert_eq!(mask_email("invalid"), "****");
        assert_eq!(mask_email("ab@domain.com"), "**@domain.com");
        assert_eq!(mask_email("alice@domain.com"), "al****@domain.com");
    }
}
