use std::{env, sync::OnceLock};

use dotenvy::dotenv;

// Global configuration instance
static GLOBAL_CONFIG: OnceLock<Config> = OnceLock::new();

// Config is the global configuration (loaded from .env)
// Only contains truly global config, trading related config is at trader/strategy level
#[derive(Debug)]
pub struct Config {
    pub api_server_port: u16,
    pub jwt_secret: String,
    pub registration_enabled: bool,
}

// init initializes global configuration (from .env)
impl Config {
    pub fn init() -> Self {
        let _ = dotenv();

        let api_server_port = env::var("API_SERVER_PORT")
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(8080);

        let registration_enabled = env::var("REGISTRATION_ENABLED")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(true);

        let jwt_secret = env::var("JWT_SECRET")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "default-jwt-secret-change-in-production".to_string());

        Config {
            api_server_port,
            jwt_secret,
            registration_enabled,
        }
    }
}

// get returns the global configuration
pub fn get() -> &'static Config {
    GLOBAL_CONFIG.get_or_init(Config::init)
}
