use std::sync::Arc;

use super::CryptoProvider;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, sqlite::SqlitePool};
use tracing::{info, instrument, warn};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct AIModel {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub provider: String,
    pub enabled: bool,
    #[serde(rename = "apiKey")]
    pub api_key: String,
    #[serde(rename = "customApiUrl")]
    pub custom_api_url: String,
    #[serde(rename = "customModelName")]
    pub custom_model_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct AIModelStore {
    pub pool: SqlitePool,
    pub provider: Arc<dyn CryptoProvider>,
}

impl AIModelStore {
    pub fn new(pool: SqlitePool, provider: Arc<dyn CryptoProvider>) -> Self {
        Self { pool, provider }
    }

    pub async fn init_tables(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ai_models (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL DEFAULT 'default',
                name TEXT NOT NULL,
                provider TEXT NOT NULL,
                enabled BOOLEAN DEFAULT 0,
                api_key TEXT DEFAULT '',
                custom_api_url TEXT DEFAULT '',
                custom_model_name TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Trigger
        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS update_ai_models_updated_at
            AFTER UPDATE ON ai_models
            BEGIN
                UPDATE ai_models SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
            END
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Backward compatibility: add potentially missing columns
        let _ = sqlx::query("ALTER TABLE ai_models ADD COLUMN custom_api_url TEXT DEFAULT ''")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("ALTER TABLE ai_models ADD COLUMN custom_model_name TEXT DEFAULT ''")
            .execute(&self.pool)
            .await;

        Ok(())
    }

    pub async fn init_default_data(&self) -> Result<()> {
        Ok(())
    }

    fn encrypt(&self, plaintext: &str) -> String {
        self.provider.encrypt(plaintext)
    }

    fn decrypt(&self, encrypted: &str) -> String {
        self.provider.decrypt(encrypted)
    }

    // List retrieves user's AI model list
    pub async fn list(&self, user_id: &str) -> Result<Vec<AIModel>> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, name, provider, enabled, api_key,
                   COALESCE(custom_api_url, '') as custom_api_url,
                   COALESCE(custom_model_name, '') as custom_model_name,
                   created_at, updated_at
            FROM ai_models WHERE user_id = ? ORDER BY id
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        let mut models = Vec::new();
        for row in rows {
            let mut model = AIModel {
                id: row.try_get("id")?,
                user_id: row.try_get("user_id")?,
                name: row.try_get("name")?,
                provider: row.try_get("provider")?,
                enabled: row.try_get("enabled")?,
                api_key: row.try_get("api_key")?,
                custom_api_url: row.try_get("custom_api_url")?,
                custom_model_name: row.try_get("custom_model_name")?,
                created_at: row.try_get("created_at")?, // sqlx 自动处理 DATETIME -> NaiveDateTime
                updated_at: row.try_get("updated_at")?,
            };

            model.api_key = self.decrypt(&model.api_key);
            models.push(model);
        }

        Ok(models)
    }

    // Get retrieves a single AI model
    pub async fn get(&self, user_id: &str, model_id: &str) -> Result<AIModel> {
        if model_id.is_empty() {
            return Err(anyhow!("model ID cannot be empty"));
        }

        let mut candidates = Vec::new();
        if !user_id.is_empty() {
            candidates.push(user_id);
        }
        if user_id != "default" {
            candidates.push("default");
        }
        if candidates.is_empty() {
            candidates.push("default");
        }

        for uid in candidates {
            let result = sqlx::query_as::<_, AIModel>(
                r#"
                SELECT id, user_id, name, provider, enabled, api_key,
                       COALESCE(custom_api_url, '') as custom_api_url,
                       COALESCE(custom_model_name, '') as custom_model_name,
                       created_at, updated_at
                FROM ai_models WHERE user_id = ? AND id = ? LIMIT 1
                "#,
            )
            .bind(uid)
            .bind(model_id)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(mut model) = result {
                model.api_key = self.decrypt(&model.api_key);
                return Ok(model);
            }
        }

        Err(anyhow!("sql: no rows in result set"))
    }

    // GetDefault retrieves the default enabled AI model
    pub async fn get_default(&self, user_id: &str) -> Result<AIModel> {
        let uid = if user_id.is_empty() {
            "default"
        } else {
            user_id
        };

        if let Ok(model) = self.first_enabled(uid).await {
            return Ok(model);
        }

        if uid != "default" {
            return self.first_enabled("default").await;
        }

        Err(anyhow!(
            "please configure an available AI model in the system first"
        ))
    }

    async fn first_enabled(&self, user_id: &str) -> Result<AIModel> {
        let result = sqlx::query_as::<_, AIModel>(
            r#"
            SELECT id, user_id, name, provider, enabled, api_key,
                   COALESCE(custom_api_url, '') as custom_api_url,
                   COALESCE(custom_model_name, '') as custom_model_name,
                   created_at, updated_at
            FROM ai_models WHERE user_id = ? AND enabled = 1
            ORDER BY datetime(updated_at) DESC, id ASC LIMIT 1
            "#,
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        match result {
            Some(mut model) => {
                model.api_key = self.decrypt(&model.api_key);
                Ok(model)
            }
            None => Err(anyhow!("sql: no rows in result set")),
        }
    }

    // Update updates AI model, creates if not exists
    #[instrument(skip_all)]
    pub async fn update(
        &self,
        user_id: &str,
        id: &str,
        enabled: bool,
        api_key: &str,
        custom_api_url: &str,
        custom_model_name: &str,
    ) -> Result<()> {
        // 1. Try exact ID match first
        let existing_id: Option<String> =
            sqlx::query_scalar("SELECT id FROM ai_models WHERE user_id = ? AND id = ? LIMIT 1")
                .bind(user_id)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

        if let Some(eid) = existing_id {
            let encrypted_api_key = self.encrypt(api_key);
            sqlx::query(
                r#"
                UPDATE ai_models 
                SET enabled = ?, api_key = ?, custom_api_url = ?, custom_model_name = ?, updated_at = datetime('now')
                WHERE id = ? AND user_id = ?
                "#,
            )
            .bind(enabled)
            .bind(encrypted_api_key)
            .bind(custom_api_url)
            .bind(custom_model_name)
            .bind(eid)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
            return Ok(());
        }

        // 2. Try legacy logic compatibility: use id as provider to search
        let provider_key = id;
        let legacy_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM ai_models WHERE user_id = ? AND provider = ? LIMIT 1",
        )
        .bind(user_id)
        .bind(provider_key)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(lid) = legacy_id {
            warn!(
                "⚠️ Using legacy provider matching to update model: {} -> {}",
                provider_key, lid
            );
            let encrypted_api_key = self.encrypt(api_key);
            sqlx::query(
                r#"
                UPDATE ai_models 
                SET enabled = ?, api_key = ?, custom_api_url = ?, custom_model_name = ?, updated_at = datetime('now')
                WHERE id = ? AND user_id = ?
                "#,
            )
            .bind(enabled)
            .bind(encrypted_api_key)
            .bind(custom_api_url)
            .bind(custom_model_name)
            .bind(lid)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
            return Ok(());
        }

        // 3. Create new record logic
        let mut final_provider = provider_key.to_string();
        if provider_key == id && (provider_key == "deepseek" || provider_key == "qwen") {
            // keep as is
        } else {
            let parts: Vec<&str> = id.split('_').collect();
            if parts.len() >= 2 {
                final_provider = parts.last().unwrap().to_string();
            } else {
                final_provider = id.to_string();
            }
        }

        // Get Name based on provider
        let db_name: Option<String> =
            sqlx::query_scalar("SELECT name FROM ai_models WHERE provider = ? LIMIT 1")
                .bind(&final_provider)
                .fetch_optional(&self.pool)
                .await?;

        let name = db_name.unwrap_or_else(|| {
            if final_provider == "deepseek" {
                "DeepSeek AI".to_string()
            } else if final_provider == "qwen" {
                "Qwen AI".to_string()
            } else {
                format!("{} AI", final_provider)
            }
        });

        let mut new_model_id = id.to_string();
        if id == final_provider {
            new_model_id = format!("{}_{}", user_id, final_provider);
        }

        info!(
            "✓ Creating new AI model configuration: ID={}, Provider={}, Name={}",
            new_model_id, final_provider, name
        );

        let encrypted_api_key = self.encrypt(api_key);
        sqlx::query(
            r#"
            INSERT INTO ai_models (id, user_id, name, provider, enabled, api_key, custom_api_url, custom_model_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            "#,
        )
        .bind(new_model_id)
        .bind(user_id)
        .bind(name)
        .bind(final_provider)
        .bind(enabled)
        .bind(encrypted_api_key)
        .bind(custom_api_url)
        .bind(custom_model_name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // Create creates an AI model
    pub async fn create(
        &self,
        user_id: &str,
        id: &str,
        name: &str,
        provider: &str,
        enabled: bool,
        api_key: &str,
        custom_api_url: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO ai_models (id, user_id, name, provider, enabled, api_key, custom_api_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(id)
        .bind(user_id)
        .bind(name)
        .bind(provider)
        .bind(enabled)
        .bind(api_key)
        .bind(custom_api_url)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
