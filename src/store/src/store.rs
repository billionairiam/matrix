use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use super::CryptoProvider;
use crate::ai_model::AIModelStore;
use crate::backtest::BacktestStore;
use crate::decision::DecisionStore;
use crate::equity::EquityStore;
use crate::exchange::ExchangeStore;
use crate::order::OrderStore;
use crate::position::PositionStore;
use crate::strategy::StrategyStore;
use crate::trader::TraderStore;
use crate::user::UserStore;
use anyhow::{Context, Result};
use sqlx::{
    ConnectOptions, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::sync::RwLock;
use tracing::{info, instrument, warn};

struct NoOpCrypto;
impl CryptoProvider for NoOpCrypto {
    fn encrypt(&self, i: &str) -> String {
        i.to_string()
    }
    fn decrypt(&self, i: &str) -> String {
        i.to_string()
    }
}

#[derive(Clone)]
pub struct Store {
    pub pool: SqlitePool,

    crypto: Arc<RwLock<Arc<dyn CryptoProvider>>>,
}

impl Store {
    /// New creates new Store instance
    #[instrument]
    pub async fn new(db_path: &str) -> Result<Self> {
        // SQLite configuration matching Go's settings
        let options = SqliteConnectOptions::from_str(db_path)
            .context("Invalid database URL")?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Delete)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_millis(5000))
            .foreign_keys(true)
            .disable_statement_logging();

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .connect_with(options)
            .await
            .context("Failed to open database")?;

        let store = Self {
            pool,
            crypto: Arc::new(RwLock::new(Arc::new(NoOpCrypto))),
        };

        // Initialize all table structures
        store
            .init_tables()
            .await
            .context("Failed to initialize table structure")?;

        // Initialize default data
        store
            .init_default_data()
            .await
            .context("Failed to initialize default data")?;

        info!("✅ Database enabled DELETE mode and FULL sync");
        Ok(store)
    }

    /// Creates Store from an existing connection pool (Equivalent to NewFromDB)
    pub fn new_from_pool(pool: SqlitePool) -> Self {
        Self {
            pool,
            crypto: Arc::new(RwLock::new(Arc::new(NoOpCrypto))),
        }
    }

    /// SetCryptoFuncs sets encryption/decryption functions
    pub async fn set_provider(&self, provider: impl CryptoProvider) {
        let mut guard = self.crypto.write().await;
        *guard = Arc::new(provider);
    }

    async fn init_tables(&self) -> Result<()> {
        // Initialize in dependency order
        self.user()
            .init_tables()
            .await
            .context("Failed to initialize user tables")?;
        self.ai_model()
            .await
            .init_tables()
            .await
            .context("Failed to initialize AI model tables")?;
        self.exchange()
            .await
            .init_tables()
            .await
            .context("Failed to initialize exchange tables")?;
        self.trader()
            .await
            .init_tables()
            .await
            .context("Failed to initialize trader tables")?;
        self.decision()
            .init_tables()
            .await
            .context("Failed to initialize decision log tables")?;
        self.backtest()
            .init_tables()
            .await
            .context("Failed to initialize backtest tables")?;
        self.order()
            .init_tables()
            .await
            .context("Failed to initialize order tables")?;
        self.position()
            .init_tables()
            .await
            .context("Failed to initialize position tables")?;
        self.strategy()
            .init_tables()
            .await
            .context("Failed to initialize strategy tables")?;
        self.equity()
            .init_tables()
            .await
            .context("Failed to initialize equity tables")?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn init_default_data(&self) -> Result<()> {
        self.ai_model().await.init_default_data().await?;
        self.exchange().await.init_default_data().await?;
        self.strategy().init_default_data().await?;

        // Migrate old decision_account_snapshots data to new trader_equity_snapshots table
        match self.equity().migrate_from_decision().await {
            Ok(migrated) if migrated > 0 => {
                info!("✅ Migrated {} equity records to new table", migrated);
            }
            Ok(_) => {} // 0 records, do nothing
            Err(e) => {
                warn!("failed to migrate equity data: {:?}", e);
            }
        }
        Ok(())
    }

    pub fn user(&self) -> UserStore {
        UserStore::new(self.pool.clone())
    }

    pub async fn ai_model(&self) -> AIModelStore {
        let provider = self.crypto.read().await.clone();
        AIModelStore {
            pool: self.pool.clone(),
            provider,
        }
    }

    pub async fn exchange(&self) -> ExchangeStore {
        let provider = self.crypto.read().await.clone();

        ExchangeStore::new(self.pool.clone(), provider)
    }

    pub async fn trader(&self) -> TraderStore {
        let provider = self.crypto.read().await.clone();

        TraderStore::new(self.pool.clone(), provider)
    }

    pub fn decision(&self) -> DecisionStore {
        DecisionStore::new(self.pool.clone())
    }

    pub fn backtest(&self) -> BacktestStore {
        BacktestStore::new(self.pool.clone())
    }

    pub fn order(&self) -> OrderStore {
        OrderStore::new(self.pool.clone())
    }

    pub fn position(&self) -> PositionStore {
        PositionStore::new(self.pool.clone())
    }

    pub fn strategy(&self) -> StrategyStore {
        StrategyStore::new(self.pool.clone())
    }

    pub fn equity(&self) -> EquityStore {
        EquityStore::new(self.pool.clone())
    }

    /// Close closes database connection
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// DB gets underlying database connection pool
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Transaction executes a closure within a database transaction
    pub async fn transaction<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(sqlx::Transaction<'_, sqlx::Sqlite>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let tx = self
            .pool
            .begin()
            .await
            .context("Failed to begin transaction")?;

        match f(tx).await {
            Ok(result) => Ok(result),
            Err(e) => Err(e),
        }
    }
}
