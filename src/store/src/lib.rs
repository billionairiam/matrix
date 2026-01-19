pub mod ai_model;
pub mod backtest;
pub mod decision;
pub mod equity;
pub mod exchange;
pub mod order;
pub mod position;
pub mod store;
pub mod strategy;
pub mod trader;
pub mod user;

// Type alias for encryption/decryption closures
type CryptoFunc = Box<dyn Fn(&str) -> String + Send + Sync>;
