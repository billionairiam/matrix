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

pub trait CryptoProvider: Send + Sync + 'static {
    fn encrypt(&self, input: &str) -> String;
    fn decrypt(&self, input: &str) -> String;
}
