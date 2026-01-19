use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Map, Value};

pub mod aster_trader;
pub mod auto_trader;
pub mod binance_futures;
pub mod hyperliquid_trader;

#[async_trait]
pub trait Trader: Send + Sync {
    /// Get account balance
    async fn get_balance(&self) -> Result<Map<String, Value>>;

    /// Get all positions
    async fn get_positions(&self) -> Result<Vec<Map<String, Value>>>;

    /// Open long position
    async fn open_long(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value>;

    /// Open short position
    async fn open_short(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value>;

    /// Close long position (quantity=0.0 means close all)
    async fn close_long(&self, symbol: &str, quantity: f64) -> Result<Value>;

    /// Close short position (quantity=0.0 means close all)
    async fn close_short(&self, symbol: &str, quantity: f64) -> Result<Value>;

    /// Set leverage
    async fn set_leverage(&self, symbol: &str, leverage: i32) -> Result<()>;

    /// Set position mode (true=cross margin, false=isolated margin)
    async fn set_margin_mode(&self, symbol: &str, is_cross_margin: bool) -> Result<()>;

    /// Get market price
    async fn get_market_price(&self, symbol: &str) -> Result<f64>;

    /// Set stop-loss order
    async fn set_stop_loss(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        stop_price: f64,
    ) -> Result<()>;

    /// Set take-profit order
    async fn set_take_profit(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        take_profit_price: f64,
    ) -> Result<()>;

    /// Cancel only stop-loss orders
    async fn cancel_stop_loss_orders(&self, symbol: &str) -> Result<()>;

    /// Cancel only take-profit orders
    async fn cancel_take_profit_orders(&self, symbol: &str) -> Result<()>;

    /// Cancel all pending orders for this symbol
    async fn cancel_all_orders(&self, symbol: &str) -> Result<()>;

    /// Cancel stop-loss/take-profit orders for this symbol
    async fn cancel_stop_orders(&self, symbol: &str) -> Result<()>;

    /// Format quantity to correct precision (Returns String to preserve formatting)
    async fn format_quantity(&self, symbol: &str, quantity: f64) -> Result<String>;

    /// Get order status
    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<Map<String, Value>>;
}
