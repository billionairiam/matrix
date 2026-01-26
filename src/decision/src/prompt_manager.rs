use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{OnceLock, RwLock};

// Constant for the directory
const PROMPTS_DIR: &str = "prompts";

/// PromptTemplate system prompt template
#[derive(Debug, Clone)]
pub struct PromptTemplate {
    pub name: String,    // Template name (filename without extension)
    pub content: String, // Template content
}

/// PromptManager prompt manager
pub struct PromptManager {
    // RwLock allows multiple readers or one writer.
    // We wrap the HashMap directly.
    templates: RwLock<HashMap<String, PromptTemplate>>,
}

static GLOBAL_PROMPT_MANAGER: OnceLock<PromptManager> = OnceLock::new();

impl PromptManager {
    /// Creates a new prompt manager
    pub fn new() -> Self {
        Self {
            templates: RwLock::new(HashMap::new()),
        }
    }

    /// LoadTemplates loads all prompt templates from specified directory
    pub fn load_templates<P: AsRef<Path>>(&self, dir: P) -> io::Result<()> {
        let dir_path = dir.as_ref();

        // Check if directory exists
        if !dir_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("prompt directory does not exist: {}", dir_path.display()),
            ));
        }

        // Scan directory
        let entries = fs::read_dir(dir_path).map_err(|e| {
            io::Error::new(e.kind(), format!("failed to scan prompt directory: {}", e))
        })?;

        let mut loaded_count = 0;
        let mut buffer_map = HashMap::new();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            // Check for .txt extension
            if path.is_file() && path.extension().map_or(false, |ext| ext == "txt") {
                // Read file content
                let content = match fs::read_to_string(&path) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("⚠️  Failed to read prompt file {}: {}", path.display(), e);
                        continue;
                    }
                };

                // Extract filename without extension
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .to_string();

                let template_name = path
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .to_string();

                // Store in buffer
                buffer_map.insert(
                    template_name.clone(),
                    PromptTemplate {
                        name: template_name.clone(),
                        content,
                    },
                );

                println!(
                    "  📄 Loaded prompt template: {} ({})",
                    template_name, file_name
                );
                loaded_count += 1;
            }
        }

        if loaded_count == 0 {
            println!(
                "⚠️  No .txt files found in prompt directory {}",
                dir_path.display()
            );
        }

        // Acquire Write Lock and update map
        let mut templates_guard = self.templates.write().unwrap();
        for (k, v) in buffer_map {
            templates_guard.insert(k, v);
        }

        Ok(())
    }

    /// GetTemplate gets prompt template by name
    pub fn get_template(&self, name: &str) -> Option<PromptTemplate> {
        let templates_guard = self.templates.read().unwrap();
        templates_guard.get(name).cloned()
    }

    /// GetAllTemplateNames gets all template names list
    pub fn get_all_template_names(&self) -> Vec<String> {
        let templates_guard = self.templates.read().unwrap();
        templates_guard.keys().cloned().collect()
    }

    /// GetAllTemplates gets all templates
    pub fn get_all_templates(&self) -> Vec<PromptTemplate> {
        let templates_guard = self.templates.read().unwrap();
        templates_guard.values().cloned().collect()
    }

    /// ReloadTemplates reloads all templates
    pub fn reload_templates<P: AsRef<Path>>(&self, dir: P) -> io::Result<()> {
        // Clear existing templates
        {
            let mut templates_guard = self.templates.write().unwrap();
            templates_guard.clear();
        } // Lock drops here

        // Load again
        self.load_templates(dir)
    }
}

/// Helper to get or initialize the global manager.
fn get_global_instance() -> &'static PromptManager {
    GLOBAL_PROMPT_MANAGER.get_or_init(|| {
        let pm = PromptManager::new();
        // Try to load default directory
        if let Err(e) = pm.load_templates(PROMPTS_DIR) {
            eprintln!("⚠️  Failed to load prompt templates: {}", e);
        } else {
            // We need to count them slightly differently since we don't have the map here easily without locking
            // keeping it simple for the singleton init
            let count = pm.templates.read().unwrap().len();
            println!("✓ Loaded {} system prompt templates", count);
        }
        pm
    })
}

/// Initialize the global manager explicitly (Optional, but good practice in Rust main)
pub fn init() {
    get_global_instance();
}

/// GetPromptTemplate gets prompt template by name (global function)
pub fn get_prompt_template(name: &str) -> Result<PromptTemplate, String> {
    let pm = get_global_instance();
    pm.get_template(name)
        .ok_or_else(|| format!("prompt template does not exist: {}", name))
}

/// GetAllPromptTemplateNames gets all template names (global function)
pub fn get_all_prompt_template_names() -> Vec<String> {
    get_global_instance().get_all_template_names()
}

/// GetAllPromptTemplates gets all templates (global function)
pub fn get_all_prompt_templates() -> Vec<PromptTemplate> {
    get_global_instance().get_all_templates()
}

/// ReloadPromptTemplates reloads all templates (global function)
pub fn reload_prompt_templates() -> io::Result<()> {
    get_global_instance().reload_templates(PROMPTS_DIR)
}

// === Example Usage (main) ===
#[cfg(test)]
mod tests {
    use super::*;

    // To run this test, ensure you have a "prompts" folder with .txt files
    #[test]
    fn test_manager() {
        // Create dummy directory for test
        let _ = fs::create_dir("prompts");
        let _ = fs::write("prompts/test_prompt.txt", "This is a test prompt");

        // Explicit init (optional, but mimics Go init)
        init();

        // Get Names
        let names = get_all_prompt_template_names();
        println!("Names: {:?}", names);

        // Get Template
        match get_prompt_template("test_prompt") {
            Ok(t) => println!("Found: {} -> {}", t.name, t.content),
            Err(e) => println!("Error: {}", e),
        }

        // Clean up
        let _ = fs::remove_file("prompts/test_prompt.txt");
        let _ = fs::remove_dir("prompts");
    }
}
