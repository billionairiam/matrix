use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}
