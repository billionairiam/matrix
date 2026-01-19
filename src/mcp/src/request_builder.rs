use crate::request::{FunctionDef, Message, Request, Tool};
use serde_json::Value;

/// RequestBuilder 构造器
/// 对应 Go 中的 RequestBuilder
#[derive(Debug, Clone, Default)]
pub struct RequestBuilder {
    model: String,
    messages: Vec<Message>,
    stream: bool,
    temperature: Option<f64>,
    max_tokens: Option<u32>,
    top_p: Option<f64>,
    frequency_penalty: Option<f64>,
    presence_penalty: Option<f64>,
    stop: Vec<String>,
    tools: Vec<Tool>,
    tool_choice: Option<Value>,
}

impl RequestBuilder {
    /// 创建新的 Builder
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置模型名称
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    /// 设置是否流式响应
    pub fn with_stream(mut self, stream: bool) -> Self {
        self.stream = stream;
        self
    }

    /// 添加 System Prompt (便捷方法)
    pub fn with_system_prompt(mut self, prompt: &str) -> Self {
        if !prompt.is_empty() {
            self.messages.push(Message::system(prompt));
        }
        self
    }

    /// 添加 User Prompt (便捷方法)
    pub fn with_user_prompt(mut self, prompt: &str) -> Self {
        if !prompt.is_empty() {
            self.messages.push(Message::user(prompt));
        }
        self
    }

    /// 添加 System 消息
    pub fn add_system_message(self, content: &str) -> Self {
        self.with_system_prompt(content)
    }

    /// 添加 User 消息
    pub fn add_user_message(self, content: &str) -> Self {
        self.with_user_prompt(content)
    }

    /// 添加 Assistant 消息 (用于多轮对话上下文)
    pub fn add_assistant_message(mut self, content: &str) -> Self {
        if !content.is_empty() {
            self.messages.push(Message::assistant(content));
        }
        self
    }

    /// 添加自定义角色的消息
    pub fn add_message(mut self, role: &str, content: &str) -> Self {
        if !content.is_empty() {
            self.messages.push(Message::new(role, content));
        }
        self
    }

    /// 批量添加消息
    pub fn add_messages(mut self, messages: Vec<Message>) -> Self {
        self.messages.extend(messages);
        self
    }

    /// 添加对话历史
    pub fn add_conversation_history(mut self, history: Vec<Message>) -> Self {
        self.messages.extend(history);
        self
    }

    /// 清空消息
    pub fn clear_messages(mut self) -> Self {
        self.messages.clear();
        self
    }

    /// 设置温度参数 (0.0 - 2.0)
    pub fn with_temperature(mut self, t: f64) -> Self {
        // Rust 的 clamp 方法非常方便
        self.temperature = Some(t.clamp(0.0, 2.0));
        self
    }

    /// 设置最大 Token 数
    pub fn with_max_tokens(mut self, tokens: u32) -> Self {
        if tokens > 0 {
            self.max_tokens = Some(tokens);
        }
        self
    }

    /// 设置 Nucleus Sampling 参数 (0.0 - 1.0)
    pub fn with_top_p(mut self, p: f64) -> Self {
        self.top_p = Some(p.clamp(0.0, 1.0));
        self
    }

    /// 设置频率惩罚 (-2.0 - 2.0)
    pub fn with_frequency_penalty(mut self, penalty: f64) -> Self {
        self.frequency_penalty = Some(penalty.clamp(-2.0, 2.0));
        self
    }

    /// 设置存在惩罚 (-2.0 - 2.0)
    pub fn with_presence_penalty(mut self, penalty: f64) -> Self {
        self.presence_penalty = Some(penalty.clamp(-2.0, 2.0));
        self
    }

    /// 设置停止序列 (覆盖)
    pub fn with_stop_sequences(mut self, sequences: Vec<String>) -> Self {
        self.stop = sequences;
        self
    }

    /// 添加单个停止序列
    pub fn add_stop_sequence(mut self, sequence: &str) -> Self {
        if !sequence.is_empty() {
            self.stop.push(sequence.to_string());
        }
        self
    }

    /// 添加工具
    pub fn add_tool(mut self, tool: Tool) -> Self {
        self.tools.push(tool);
        self
    }

    /// 添加函数 (便捷方法)
    pub fn add_function(mut self, name: &str, description: &str, parameters: Value) -> Self {
        let tool = Tool::function(FunctionDef {
            name: name.to_string(),
            description: Some(description.to_string()),
            parameters: Some(parameters),
        });
        self.tools.push(tool);
        self
    }

    /// 设置工具选择策略
    /// 支持 "auto", "none" 字符串，或者 JSON 对象
    pub fn with_tool_choice(mut self, choice: impl Into<Value>) -> Self {
        self.tool_choice = Some(choice.into());
        self
    }

    /// 构建 Request 对象
    pub fn build(self) -> Result<Request, String> {
        // 校验：至少需要一条消息
        if self.messages.is_empty() {
            return Err("At least one message is required".to_string());
        }

        // 构建 Request (使用 model.rs 中的 struct)
        // 注意：Request::new 是我们在 model.rs 实现的构造函数，也可以手动构造 struct
        let mut req = Request::new(self.model, self.messages);

        req.stream = self.stream;
        req.temperature = self.temperature;
        req.max_tokens = self.max_tokens;
        req.top_p = self.top_p;
        req.frequency_penalty = self.frequency_penalty;
        req.presence_penalty = self.presence_penalty;

        if !self.stop.is_empty() {
            req.stop = Some(self.stop);
        }

        if !self.tools.is_empty() {
            req.tools = Some(self.tools);
        }

        req.tool_choice = self.tool_choice;

        Ok(req)
    }

    /// 构建 Request 对象，失败则 Panic
    /// 对应 Go 的 MustBuild
    pub fn must_build(self) -> Request {
        match self.build() {
            Ok(req) => req,
            Err(e) => panic!("Failed to build request: {}", e),
        }
    }
}

impl RequestBuilder {
    /// 创建聊天场景的 Builder (预设了合理的参数)
    pub fn for_chat() -> Self {
        Self::new().with_temperature(0.7).with_max_tokens(2000)
    }

    /// 创建代码生成场景的 Builder (低温度，更确定)
    pub fn for_code_generation() -> Self {
        Self::new()
            .with_temperature(0.2)
            .with_max_tokens(2000)
            .with_top_p(0.1)
    }

    /// 创建创意写作场景的 Builder (高温度，更随机)
    pub fn for_creative_writing() -> Self {
        Self::new()
            .with_temperature(1.2)
            .with_max_tokens(4000)
            .with_top_p(0.95)
            .with_presence_penalty(0.6)
            .with_frequency_penalty(0.5)
    }
}
