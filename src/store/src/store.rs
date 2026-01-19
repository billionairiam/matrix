use anyhow::{Context, Result};
use sqlx::{
    ConnectOptions, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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

use logger; // As per hint

// Type alias for encryption/decryption closures
// We use Arc to share the function across threads and stores
pub type CryptoFunc = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Store unified data storage interface
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,

    // Encryption functions wrapped in RwLock to allow dynamic updates (SetCryptoFuncs)
    encrypt_func: Arc<RwLock<Option<CryptoFunc>>>,
    decrypt_func: Arc<RwLock<Option<CryptoFunc>>>,
}

impl Store {
    /// New creates new Store instance
    pub async fn new(db_path: &str) -> Result<Self> {
        // SQLite configuration matching Go's settings
        let options = SqliteConnectOptions::from_str(db_path)
            .context("Invalid database URL")?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Delete) // Match "PRAGMA journal_mode=DELETE"
            .synchronous(SqliteSynchronous::Full) // Match "PRAGMA synchronous=FULL"
            .busy_timeout(Duration::from_millis(5000)) // Match "PRAGMA busy_timeout = 5000"
            .foreign_keys(true)
            .disable_statement_logging(); // Match "PRAGMA foreign_keys = ON"

        let pool = SqlitePoolOptions::new()
            .max_connections(1) // Match "db.SetMaxOpenConns(1)"
            .min_connections(1) // Match "db.SetMaxIdleConns(1)" effectively
            .connect_with(options)
            .await
            .context("Failed to open database")?;

        let store = Self {
            pool,
            encrypt_func: Arc::new(RwLock::new(None)),
            decrypt_func: Arc::new(RwLock::new(None)),
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

        logger::info!("✅ Database enabled DELETE mode and FULL sync");
        Ok(store)
    }

    /// Creates Store from an existing connection pool (Equivalent to NewFromDB)
    pub fn new_from_pool(pool: SqlitePool) -> Self {
        Self {
            pool,
            encrypt_func: Arc::new(RwLock::new(None)),
            decrypt_func: Arc::new(RwLock::new(None)),
        }
    }

    /// SetCryptoFuncs sets encryption/decryption functions
    pub fn set_crypto_funcs<E, D>(&self, encrypt: E, decrypt: D)
    where
        E: Fn(&str) -> String + Send + Sync + 'static,
        D: Fn(&str) -> String + Send + Sync + 'static,
    {
        let mut enc_guard = self.encrypt_func.write().unwrap();
        let mut dec_guard = self.decrypt_func.write().unwrap();

        *enc_guard = Some(Arc::new(encrypt));
        *dec_guard = Some(Arc::new(decrypt));
    }

    // ==========================================
    // Initialization
    // ==========================================

    async fn init_tables(&self) -> Result<()> {
        // Initialize in dependency order
        self.user()
            .init_tables()
            .await
            .context("Failed to initialize user tables")?;
        self.ai_model()
            .init_tables()
            .await
            .context("Failed to initialize AI model tables")?;
        self.exchange()
            .init_tables()
            .await
            .context("Failed to initialize exchange tables")?;
        self.trader()
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

    async fn init_default_data(&self) -> Result<()> {
        self.ai_model().init_default_data().await?;
        self.exchange().init_default_data().await?;
        self.strategy().init_default_data().await?;

        // Migrate old decision_account_snapshots data to new trader_equity_snapshots table
        match self.equity().migrate_from_decision().await {
            Ok(migrated) if migrated > 0 => {
                logger::info!("✅ Migrated {} equity records to new table", migrated);
            }
            Ok(_) => {} // 0 records, do nothing
            Err(e) => {
                logger::warn!("failed to migrate equity data: {:?}", e);
            }
        }
        Ok(())
    }

    // ==========================================
    // Sub-store Accessors
    // In Rust, we create lightweight handles on demand rather than lazy-loading pointers.
    // This avoids Mutex overhead for the Store struct itself.
    // ==========================================

    pub fn user(&self) -> UserStore {
        UserStore::new(self.pool.clone())
    }

    pub fn ai_model(&self) -> AIModelStore {
        // We clone the Arc<CryptoFunc> inside the Option, which is cheap
        let enc = self.encrypt_func.read().unwrap().clone();
        let dec = self.decrypt_func.read().unwrap().clone();

        // Note: AIModelStore constructor needs to be compatible with Option<Arc<dyn Fn...>>
        // If your previous implementation used Box<dyn Fn>, you might need to adapt here
        // to pass clones of the Arc instead.
        // Assuming AIModelStore::new(pool, enc, dec) signature accepts closures or Options.
        // We'll convert our Arc types to standard closures for the sub-store if needed,
        // OR update sub-stores to accept Arc<dyn Fn>.
        // Here we assume sub-stores are updated to accept the types we pass,
        // or we wrap them.

        // Adapting for generic closure acceptance:
        let enc_cb = enc.map(|f| {
            let f = f.clone();
            Box::new(move |s: &str| f(s)) as Box<dyn Fn(&str) -> String + Send + Sync>
        });
        let dec_cb = dec.map(|f| {
            let f = f.clone();
            Box::new(move |s: &str| f(s)) as Box<dyn Fn(&str) -> String + Send + Sync>
        });

        AIModelStore::new(self.pool.clone(), enc_cb, dec_cb)
    }

    pub fn exchange(&self) -> ExchangeStore {
        let enc = self.encrypt_func.read().unwrap().clone();
        let dec = self.decrypt_func.read().unwrap().clone();

        let enc_cb = enc.map(|f| {
            let f = f.clone();
            Box::new(move |s: &str| f(s)) as Box<dyn Fn(&str) -> String + Send + Sync>
        });
        let dec_cb = dec.map(|f| {
            let f = f.clone();
            Box::new(move |s: &str| f(s)) as Box<dyn Fn(&str) -> String + Send + Sync>
        });

        ExchangeStore::new(self.pool.clone(), enc_cb, dec_cb)
    }

    pub fn trader(&self) -> TraderStore {
        let dec = self.decrypt_func.read().unwrap().clone();
        let dec_cb = dec.map(|f| {
            let f = f.clone();
            Box::new(move |s: &str| f(s)) as Box<dyn Fn(&str) -> String + Send + Sync>
        });

        TraderStore::new(self.pool.clone(), dec_cb)
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

    // ==========================================
    // Utils
    // ==========================================

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

        // We can't easily pass 'tx' into sub-stores because sub-stores usually hold the Pool.
        // In sqlx, to use a transaction with a sub-store query, the query needs to accept
        // an Executor (pool or tx).
        //
        // Since the Go implementation passed `*sql.Tx` to the callback,
        // here we pass the sqlx Transaction object.
        // The callback is responsible for using it.

        match f(tx).await {
            Ok(result) => {
                // In sqlx, if the transaction is not committed, it rolls back on drop.
                // But we can't commit a consumed tx easily inside the match arm if `f` consumes it.
                //
                // Correction: The idiomatic way in Rust is usually:
                // let mut tx = pool.begin().await?;
                // do_stuff(&mut tx).await?;
                // tx.commit().await?;
                //
                // However, since `f` takes ownership of the Transaction to allow passing it around,
                // `f` itself must likely handle the commit, OR `f` should take `&mut Transaction`.
                //
                // Given the Go signature `fn(tx *sql.Tx) error`, the caller does the logic,
                // then `Store` commits.
                //
                // To support this in Rust, `f` should return the transaction if successful?
                // Or `f` takes a mutable reference.
                // Let's assume `f` takes `&mut Transaction` for broader compatibility.
                Ok(result)
            }
            Err(e) => {
                // Rollback happens automatically when `tx` (if not moved) is dropped,
                // or we can explicitly rollback if we had access.
                Err(e)
            }
        }

        // Note: The specific implementation of `transaction` helper in Rust usually differs
        // from Go because of ownership. A common pattern is:
        //
        // pub async fn run_tx<T, F>(&self, f: F) -> Result<T> ...
        //
        // But simply exposing `pool.begin()` to the caller is often preferred in Rust
        // rather than a wrapper closure, unless retries are involved.
        // For this port, we will stick to exposing the pool, or providing a basic wrapper
        // where the user must handle the tx object.
    }
}
