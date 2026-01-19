use crate::Provider;

pub const PROVIDER_CUSTOM: &str = "CUSTOM";
pub const DEFAULT_CUSTOM_BASE_URL: &str = "";
pub const DEFAULT_CUSTOM_MODEL: &str = "CUSTOM";

#[derive(Debug, Clone, Copy, Default)]
pub struct CustomProvider;

impl Provider for CustomProvider {
    fn name(&self) -> &str {
        PROVIDER_CUSTOM
    }

    fn default_base_url(&self) -> &str {
        DEFAULT_CUSTOM_BASE_URL
    }

    fn default_model(&self) -> &str {
        DEFAULT_CUSTOM_MODEL
    }
}
