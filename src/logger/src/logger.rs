use std::fmt;
use std::path::Path;
use std::sync::Once;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::{
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    registry::LookupSpan,
};

pub use tracing;

use crate::config::Config;

// Global initialization lock
static INIT: Once = Once::new();

struct CompactFormatter;

impl<S, N> FormatEvent<S, N> for CompactFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta: &Metadata = event.metadata();

        // 1. Format Level (Upper 4 chars: INFO, WARN)
        let level_str = meta.level().as_str();
        let level_short = if level_str.len() > 4 {
            &level_str[0..4]
        } else {
            level_str
        };

        // 2. Format Caller (pkg/file:line)
        let file_path = meta.file().unwrap_or("unknown");
        let line = meta.line().unwrap_or(0);
        let caller = format_caller_path(file_path, line);

        // 3. Write output preamble: [INFO] pkg/file:line
        write!(writer, "[{}] {} ", level_short, caller)?;

        // 4. Write the actual message/fields
        // FIX: Use writer.by_ref() so 'writer' isn't moved, allowing us to use it again for the newline
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        // 5. Append newline
        writeln!(writer)
    }
}

/// Helper to mimic the Go path formatting: "dir/filename:line"
fn format_caller_path(path_str: &str, line: u32) -> String {
    let path = Path::new(path_str);

    // Get filename (e.g., "logger.rs")
    let filename = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    // Get parent dir name (e.g., "src" or "manager")
    let parent = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .unwrap_or("");

    if parent.is_empty() {
        format!("{}:{}", filename, line)
    } else {
        format!("{}/{}:{}", parent, filename, line)
    }
}

// ============================================================================
// Initialization functions
// ============================================================================

/// Init initializes the global logger
pub fn init(cfg: Option<&Config>) {
    INIT.call_once(|| {
        let default_cfg = Config::default();
        let cfg = cfg.unwrap_or(&default_cfg);

        // Parse level string
        let level = match cfg.level.to_lowercase().as_str() {
            "debug" => Level::DEBUG,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            "trace" => Level::TRACE,
            _ => Level::INFO,
        };

        // Set up subscriber with CompactFormatter
        let subscriber = tracing_subscriber::fmt()
            .event_format(CompactFormatter)
            .with_max_level(level)
            .with_writer(std::io::stdout)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    });
}

pub fn init_with_simple_config(level: &str) {
    init(Some(&Config {
        level: level.to_string(),
    }));
}

#[macro_export]
macro_rules! debug {
    // 使用 $crate::logger::tracing 确保无论在哪调用，都能找到依赖
    ($($arg:tt)*) => { $crate::logger::tracing::debug!($($arg)*) };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => { $crate::logger::tracing::info!($($arg)*) };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::logger::tracing::warn!($($arg)*) };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => { $crate::logger::tracing::error!($($arg)*) };
}

#[macro_export]
macro_rules! fatal {
    ($($arg:tt)*) => {
        {
            $crate::logger::tracing::error!($($arg)*);
            std::process::exit(1);
        }
    };
}

// ============================================================================
// MCP Logger adapter
// ============================================================================

/// MCPLogger adapter that allows MCP package to use the global logger
#[derive(Clone)]
pub struct MCPLogger;

impl MCPLogger {
    pub fn new() -> Self {
        Self {}
    }

    pub fn debugf(&self, args: fmt::Arguments) {
        tracing::debug!("{}", args);
    }

    pub fn infof(&self, args: fmt::Arguments) {
        tracing::info!("{}", args);
    }

    pub fn warnf(&self, args: fmt::Arguments) {
        tracing::warn!("{}", args);
    }

    pub fn errorf(&self, args: fmt::Arguments) {
        tracing::error!("{}", args);
    }
}

// Helper macro for MCP logger
#[macro_export]
macro_rules! mcp_info {
    ($logger:expr, $($arg:tt)*) => {
        $logger.infof(format_args!($($arg)*))
    };
}
