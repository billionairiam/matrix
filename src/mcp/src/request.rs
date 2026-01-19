use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Message represents a conversation message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
}

impl Message {
    /// Creates a new message with custom role
    pub fn new(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: content.into(),
        }
    }

    /// Creates a system message
    pub fn system(content: impl Into<String>) -> Self {
        Self::new("system", content)
    }

    /// Creates a user message
    pub fn user(content: impl Into<String>) -> Self {
        Self::new("user", content)
    }

    /// Creates an assistant message
    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new("assistant", content)
    }
}

/// Tool represents a tool/function that AI can call
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    /// Usually "function"
    /// 'type' is a Rust keyword, so we use raw identifier r#type and rename for JSON
    #[serde(rename = "type")]
    pub r#type: String,

    /// Function definition
    pub function: FunctionDef,
}

impl Tool {
    pub fn function(def: FunctionDef) -> Self {
        Self {
            r#type: "function".to_string(),
            function: def,
        }
    }
}

/// FunctionDef function definition
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FunctionDef {
    /// Function name
    pub name: String,

    /// Function description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Parameter schema (JSON Schema)
    /// Go: map[string]any -> Rust: serde_json::Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
}

/// Request AI API request
/// Supports fluent builder pattern via methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    // Basic fields
    pub model: String,
    pub messages: Vec<Message>,

    #[serde(skip_serializing_if = "is_false")] // Skip if false
    pub stream: bool,

    // Optional parameters (using Option to handle null/omitempty)
    /// Temperature (0-2), controls randomness
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    /// Maximum token count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    /// Nucleus sampling parameter (0-1)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,

    /// Frequency penalty (-2 to 2)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,

    /// Presence penalty (-2 to 2)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,

    /// Stop sequences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,

    // Advanced features
    /// Available tools list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,

    /// Tool choice strategy
    /// Can be a string ("auto", "none") or a JSON object, so we use Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
}

/// Helper function for serde to skip 'stream' if false
fn is_false(b: &bool) -> bool {
    !*b
}

impl Request {
    /// Create a new basic request
    pub fn new(model: impl Into<String>, messages: Vec<Message>) -> Self {
        Self {
            model: model.into(),
            messages,
            stream: false,
            temperature: None,
            max_tokens: None,
            top_p: None,
            frequency_penalty: None,
            presence_penalty: None,
            stop: None,
            tools: None,
            tool_choice: None,
        }
    }
}
