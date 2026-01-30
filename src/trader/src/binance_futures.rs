use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use binance::account::OrderSide;
use binance::api::Binance;
use binance::futures::account::{
    CustomOrderRequest, FuturesAccount, OrderType, PositionSide, WorkingType,
};
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use binance::futures::model::Filters;
use chrono::prelude::*;
use rand::Rng;
use serde_json::{Map, Value, json};
use tokio::sync::RwLock;
use tracing::{error, info, instrument, warn};

use super::Trader;

// Constants
const BR_ID: &str = "KzrpZaP9";
const CACHE_DURATION: Duration = Duration::from_secs(15);

// Cache Structures
#[derive(Clone, Debug)]
struct CachedBalance {
    data: Map<String, Value>,
    timestamp: Instant,
}

#[derive(Clone, Debug)]
struct CachedPositions {
    data: Vec<Map<String, Value>>,
    timestamp: Instant,
}

// Main Trader Struct
pub struct FuturesTrader {
    account_client: FuturesAccount,
    general_client: FuturesGeneral,
    market_client: FuturesMarket,

    // Caches protected by RwLock for thread safety
    balance_cache: Arc<RwLock<Option<CachedBalance>>>,
    positions_cache: Arc<RwLock<Option<CachedPositions>>>,
}

impl FuturesTrader {
    pub fn new(api_key: &String, secret_key: &String) -> Self {
        let mut account_client =
            FuturesAccount::new(Some(api_key.clone()), Some(secret_key.clone()));
        let mut general_client =
            FuturesGeneral::new(Some(api_key.clone()), Some(secret_key.clone()));
        let mut market_client = FuturesMarket::new(Some(api_key.clone()), Some(secret_key.clone()));

        let testnet_url = "https://testnet.binancefuture.com".to_string();
        account_client.client.set_host(testnet_url.clone());
        general_client.client.set_host(testnet_url.clone());
        market_client.client.set_host(testnet_url.clone());

        // Note: Server time sync is usually handled automatically by the binance crate
        // during requests if configured, but we can do a manual check if strictly needed.
        let trader = Self {
            account_client,
            general_client,
            market_client,
            balance_cache: Arc::new(RwLock::new(None)),
            positions_cache: Arc::new(RwLock::new(None)),
        };

        trader
    }

    /// Async initialization to set hedge mode, mimicking the constructor logic
    #[instrument(skip_all)]
    pub async fn init(&self) -> Result<()> {
        self.sync_server_time().await;

        // Attempt to enable hedge mode up to 3 times with small delays
        let mut last_error: Option<String> = None;
        for attempt in 1..=3 {
            match self.set_dual_side_position().await {
                Ok(_) => {
                    info!(
                        "✓ Successfully initialized hedge mode on attempt {}",
                        attempt
                    );
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(format!("{:?}", e));
                    warn!(
                        "⚠️ Attempt {} to set dual-side position mode failed: {:?}",
                        attempt, e
                    );
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }

        // If all retries failed, log but don't fail - account might already be in hedge mode
        if let Some(err) = last_error {
            warn!(
                "Could not explicitly enable hedge mode after 3 attempts: {}",
                err
            );
        }
        Ok(())
    }

    fn get_br_order_id() -> String {
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let timestamp = now % 10_000_000_000_000; // 13 digits

        let mut rng = rand::thread_rng();
        let random_bytes: [u8; 4] = rng.r#gen();
        let random_hex = hex::encode(random_bytes);

        // Format: x-{BR_ID}{TIMESTAMP}{RANDOM}
        let mut order_id = format!("x-{}{}{}", BR_ID, timestamp, random_hex);

        // Truncate to 32 chars
        if order_id.len() > 32 {
            order_id.truncate(32);
        }
        order_id
    }

    #[instrument(skip_all)]
    async fn sync_server_time(&self) {
        match self.general_client.get_server_time() {
            Ok(time) => {
                let now = Utc::now().timestamp_millis() as u64;
                // Note: binance-rs handles offset internally usually,
                let offset = if now > time.server_time {
                    now - time.server_time
                } else {
                    time.server_time - now
                };
                info!("⏱ Binance server time synced, offset {}ms", offset);
            }
            Err(e) => warn!("⚠️ Failed to sync Binance server time: {:?}", e),
        }
    }

    #[instrument(skip_all)]
    pub async fn set_dual_side_position(&self) -> Result<()> {
        match self.account_client.change_position_mode(true) {
            Ok(_) => {
                info!("  ✓ Account successfully switched to dual-side position mode (Hedge Mode)");
                info!(
                    "  ℹ️  Dual-side position mode allows holding both long and short positions simultaneously"
                );
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("{:?}", e);

                // Check for various error patterns
                if err_msg.contains("No need to change")
                    || err_msg.contains("already")
                    || err_msg.contains("already in")
                    || err_msg.contains("same")
                {
                    info!("  ✓ Account is already in dual-side position mode (Hedge Mode)");
                    Ok(())
                } else if err_msg.contains("4046") || err_msg.contains("restricted") {
                    // Error 4046: Account restricted from position mode change
                    Err(anyhow!(
                        "Account is restricted from changing position mode. Please check Binance account status. Error: {}",
                        e
                    ))
                } else {
                    Err(anyhow!(
                        "Failed to enable hedge mode. Make sure your Binance account supports dual-side position mode. Error: {}",
                        e
                    ))
                }
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn get_positions(&self) -> Result<Vec<Map<String, Value>>> {
        let cache = self.positions_cache.read().await;
        if let Some(c) = &*cache {
            if c.timestamp.elapsed() < CACHE_DURATION {
                info!(
                    "✓ Using cached position information (cache age: {:.1}s)",
                    c.timestamp.elapsed().as_secs_f64()
                );
                return Ok(c.data.clone());
            }
        }
        drop(cache);

        info!("🔄 Cache expired, calling Binance API to get position information...");
        // Use position_risk to get current positions
        match self.account_client.position_information("") {
            Ok(positions) => {
                let mut result = Vec::new();
                for pos in positions {
                    if pos.position_amount == 0.0 {
                        continue;
                    }

                    let side = if pos.position_amount > 0.0 {
                        "long"
                    } else {
                        "short"
                    };

                    let mut pos_map = Map::new();
                    pos_map.insert("symbol".into(), pos.symbol.into());
                    pos_map.insert("positionAmt".into(), pos.position_amount.into());
                    pos_map.insert("entryPrice".into(), pos.entry_price.into());
                    pos_map.insert("markPrice".into(), pos.mark_price.into());
                    pos_map.insert("unRealizedProfit".into(), pos.unrealized_profit.into());
                    pos_map.insert("leverage".into(), pos.leverage.into());
                    pos_map.insert("liquidationPrice".into(), pos.liquidation_price.into());
                    pos_map.insert("side".into(), side.into());

                    result.push(pos_map);
                }

                let mut write_cache = self.positions_cache.write().await;
                *write_cache = Some(CachedPositions {
                    data: result.clone(),
                    timestamp: Instant::now(),
                });

                Ok(result)
            }
            Err(e) => Err(anyhow!("failed to get positions: {:?}", e)),
        }
    }

    pub fn calculate_position_size(
        &self,
        balance: f64,
        risk_percent: f64,
        price: f64,
        leverage: u8,
    ) -> f64 {
        let risk_amount = balance * (risk_percent / 100.0);
        let position_value = risk_amount * leverage as f64;
        position_value / price
    }

    pub fn get_min_notional(&self) -> f64 {
        10.0
    }

    pub async fn check_min_notional(&self, symbol: &str, quantity: f64) -> Result<()> {
        let price = self.get_market_price(symbol).await?;
        let notional_value = quantity * price;
        let min_notional = self.get_min_notional();

        if notional_value < min_notional {
            return Err(anyhow!(
                "order amount {:.2} USDT is below minimum requirement {:.2} USDT (quantity: {:.4}, price: {:.4})",
                notional_value,
                min_notional,
                quantity,
                price
            ));
        }
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn get_symbol_precision(&self, symbol: &str) -> Result<usize> {
        let exchange_info = self
            .general_client
            .exchange_info()
            .map_err(|e| anyhow!("{:?}", e))?;

        for s in exchange_info.symbols {
            if s.symbol == symbol {
                for filter in s.filters {
                    if let Filters::LotSize { step_size, .. } = filter {
                        let step_str = step_size.to_string(); // binance-rs usually uses f64, convert to check
                        let precision = Self::calculate_precision(&step_str);
                        info!(
                            "  {} quantity precision: {} (stepSize: {})",
                            symbol, precision, step_str
                        );
                        return Ok(precision);
                    }
                }
            }
        }

        info!(
            "  ⚠ {} precision information not found, using default precision 3",
            symbol
        );
        Ok(3)
    }

    fn calculate_precision(step_size: &str) -> usize {
        // Simple string parsing to mimic the Go logic
        let trimmed = step_size.trim_end_matches('0');
        if let Some(idx) = trimmed.find('.') {
            trimmed.len() - idx - 1
        } else {
            0
        }
    }
}

#[async_trait]
impl Trader for FuturesTrader {
    #[instrument(skip_all)]
    async fn get_balance(&self) -> Result<Map<String, Value>> {
        let cache = self.balance_cache.read().await;
        if let Some(c) = &*cache {
            if c.timestamp.elapsed() < CACHE_DURATION {
                info!(
                    "✓ Using cached account balance (cache age: {:.1}s)",
                    c.timestamp.elapsed().as_secs_f64()
                );
                return Ok(c.data.clone());
            }
        }
        drop(cache); // Release read lock

        info!("🔄 Cache expired, calling Binance API to get account balance...");
        match self.account_client.account_information() {
            Ok(account) => {
                let total_wallet_balance: f64 = account.total_wallet_balance;
                let available_balance: f64 = account.available_balance;
                let total_unrealized_profit: f64 = account.total_unrealized_profit;

                info!(
                    "✓ Binance API returned: total balance={}, available={}, unrealized PnL={}",
                    total_wallet_balance, available_balance, total_unrealized_profit
                );

                let mut res = Map::new();
                res.insert("totalWalletBalance".into(), total_wallet_balance.into());
                res.insert("availableBalance".into(), available_balance.into());
                res.insert(
                    "totalUnrealizedProfit".into(),
                    total_unrealized_profit.into(),
                );

                let mut write_cache = self.balance_cache.write().await;
                *write_cache = Some(CachedBalance {
                    data: res.clone(),
                    timestamp: Instant::now(),
                });

                Ok(res)
            }
            Err(e) => {
                error!("❌ Binance API call failed: {:?}", e);
                Err(anyhow!("failed to get account info: {:?}", e))
            }
        }
    }

    #[instrument(skip_all)]
    async fn get_positions(&self) -> Result<Vec<Map<String, Value>>> {
        let cache = self.positions_cache.read().await;
        if let Some(c) = &*cache {
            if c.timestamp.elapsed() < CACHE_DURATION {
                info!(
                    "✓ Using cached position information (cache age: {:.1}s)",
                    c.timestamp.elapsed().as_secs_f64()
                );
                return Ok(c.data.clone());
            }
        }
        drop(cache);

        info!("🔄 Cache expired, calling Binance API to get position information...");
        // Use position_risk to get current positions
        match self.account_client.position_information("") {
            Ok(positions) => {
                let mut result = Vec::new();
                for pos in positions {
                    if pos.position_amount == 0.0 {
                        continue;
                    }

                    let side = if pos.position_amount > 0.0 {
                        "long"
                    } else {
                        "short"
                    };

                    let mut pos_map = Map::new();
                    pos_map.insert("symbol".into(), pos.symbol.into());
                    pos_map.insert("positionAmt".into(), pos.position_amount.into());
                    pos_map.insert("entryPrice".into(), pos.entry_price.into());
                    pos_map.insert("markPrice".into(), pos.mark_price.into());
                    pos_map.insert("unRealizedProfit".into(), pos.unrealized_profit.into());
                    pos_map.insert("leverage".into(), pos.leverage.into());
                    pos_map.insert("liquidationPrice".into(), pos.liquidation_price.into());
                    pos_map.insert("side".into(), side.into());

                    result.push(pos_map);
                }

                let mut write_cache = self.positions_cache.write().await;
                *write_cache = Some(CachedPositions {
                    data: result.clone(),
                    timestamp: Instant::now(),
                });

                Ok(result)
            }
            Err(e) => Err(anyhow!("failed to get positions: {:?}", e)),
        }
    }

    #[instrument(skip_all)]
    async fn open_long(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value> {
        let _ = self.cancel_all_orders(symbol).await;
        self.set_leverage(symbol, leverage).await?;

        let quantity_str = self.format_quantity(symbol, quantity).await?;
        let qty_float: f64 = quantity_str.parse()?;

        if qty_float <= 0.0 {
            return Err(anyhow!("position size too small (rounded to 0)"));
        }

        self.check_min_notional(symbol, qty_float).await?;

        let order_id = Self::get_br_order_id();

        // Use CustomOrderRequest to explicitly specify LONG position side in hedge mode
        let c_request = CustomOrderRequest {
            symbol: symbol.to_string(),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Market,
            time_in_force: None,
            qty: Some(qty_float),
            reduce_only: None,
            price: None,
            stop_price: None,
            close_position: None,
            activation_price: None,
            callback_rate: None,
            working_type: None,
            price_protect: None,
            new_client_order_id: Some(order_id),
        };

        match self.account_client.custom_order(c_request) {
            Ok(transaction) => {
                info!(
                    "✓ Opened long position successfully: {} quantity: {}",
                    symbol, quantity_str
                );
                info!("  Order ID: {}", transaction.order_id);
                Ok(json!({
                    "orderId": transaction.order_id,
                    "symbol": transaction.symbol,
                    "status": transaction.status
                }))
            }
            Err(e) => {
                let err_msg = format!("{:?}", e);

                // Provide helpful diagnostics for common errors
                if err_msg.contains("-4061") || err_msg.contains("position side does not match") {
                    Err(anyhow!(
                        "Position side error: Account may not be in hedge mode. \
                        Please ensure dual-side position mode is enabled in Binance. \
                        Original error: {}",
                        e
                    ))
                } else if err_msg.contains("-4164") || err_msg.contains("notional") {
                    Err(anyhow!(
                        "Order notional value too small. Minimum 100 USDT required. \
                        Current: qty={}, symbol={}. \
                        Original error: {}",
                        qty_float,
                        symbol,
                        e
                    ))
                } else {
                    Err(anyhow!("failed to open long position: {:?}", e))
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn open_short(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value> {
        let _ = self.cancel_all_orders(symbol).await;
        self.set_leverage(symbol, leverage).await?;

        let quantity_str = self.format_quantity(symbol, quantity).await?;
        let qty_float: f64 = quantity_str.parse()?;

        if qty_float <= 0.0 {
            return Err(anyhow!("position size too small"));
        }

        self.check_min_notional(symbol, qty_float).await?;

        let order_id = Self::get_br_order_id();

        // Use CustomOrderRequest to explicitly specify SHORT position side in hedge mode
        let c_request = CustomOrderRequest {
            symbol: symbol.to_string(),
            side: OrderSide::Sell,
            position_side: Some(PositionSide::Short),
            order_type: OrderType::Market,
            time_in_force: None,
            qty: Some(qty_float),
            reduce_only: None,
            price: None,
            stop_price: None,
            close_position: None,
            activation_price: None,
            callback_rate: None,
            working_type: None,
            price_protect: None,
            new_client_order_id: Some(order_id),
        };

        match self.account_client.custom_order(c_request) {
            Ok(transaction) => {
                info!(
                    "✓ Opened short position successfully: {} quantity: {}",
                    symbol, quantity_str
                );
                info!("  Order ID: {}", transaction.order_id);
                Ok(json!({
                    "orderId": transaction.order_id,
                    "symbol": transaction.symbol,
                    "status": transaction.status
                }))
            }
            Err(e) => {
                let err_msg = format!("{:?}", e);

                // Provide helpful diagnostics for common errors
                if err_msg.contains("-4061") || err_msg.contains("position side does not match") {
                    Err(anyhow!(
                        "Position side error: Account may not be in hedge mode. \
                        Please ensure dual-side position mode is enabled in Binance. \
                        Original error: {}",
                        e
                    ))
                } else if err_msg.contains("-4164") || err_msg.contains("notional") {
                    Err(anyhow!(
                        "Order notional value too small. Minimum 100 USDT required. \
                        Current: qty={}, symbol={}. \
                        Original error: {}",
                        qty_float,
                        symbol,
                        e
                    ))
                } else {
                    Err(anyhow!("failed to open short position: {:?}", e))
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn close_long(&self, symbol: &str, mut quantity: f64) -> Result<Value> {
        if quantity == 0.0 {
            let positions = self.get_positions().await?;
            for pos in positions {
                if pos["symbol"] == symbol && pos["side"] == "long" {
                    quantity = pos["positionAmt"].as_f64().unwrap_or(0.0);
                    break;
                }
            }
            if quantity == 0.0 {
                return Err(anyhow!("no long position found for {}", symbol));
            }
        }

        let quantity_str = self.format_quantity(symbol, quantity).await?;
        let qty_float: f64 = quantity_str.parse()?;

        let order_id = Self::get_br_order_id();

        // Use CustomOrderRequest to explicitly specify LONG position side for closing
        let c_request = CustomOrderRequest {
            symbol: symbol.to_string(),
            side: OrderSide::Sell,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Market,
            time_in_force: None,
            qty: Some(qty_float),
            reduce_only: None,
            price: None,
            stop_price: None,
            close_position: None,
            activation_price: None,
            callback_rate: None,
            working_type: None,
            price_protect: None,
            new_client_order_id: Some(order_id),
        };

        match self.account_client.custom_order(c_request) {
            Ok(transaction) => {
                info!(
                    "✓ Closed long position successfully: {} quantity: {}",
                    symbol, quantity_str
                );
                let _ = self.cancel_all_orders(symbol).await;
                Ok(json!({
                    "orderId": transaction.order_id,
                    "symbol": transaction.symbol,
                    "status": transaction.status
                }))
            }
            Err(e) => Err(anyhow!("failed to close long position: {:?}", e)),
        }
    }

    #[instrument(skip_all)]
    async fn close_short(&self, symbol: &str, mut quantity: f64) -> Result<Value> {
        if quantity == 0.0 {
            // Force refresh cache to ensure we have latest position info
            {
                let mut cache = self.positions_cache.write().await;
                *cache = None;
            }
            let positions = self.get_positions().await?;
            for pos in positions {
                if pos["symbol"] == symbol && pos["side"] == "short" {
                    // Short position amount is usually negative, take abs
                    quantity = pos["positionAmt"].as_f64().unwrap_or(0.0).abs();
                    break;
                }
            }
            if quantity == 0.0 {
                return Err(anyhow!("no short position found for {}", symbol));
            }
        }

        let quantity_str = self.format_quantity(symbol, quantity).await?;
        let qty_float: f64 = quantity_str.parse()?;

        let order_id = Self::get_br_order_id();

        // Use CustomOrderRequest to explicitly specify SHORT position side for closing
        let c_request = CustomOrderRequest {
            symbol: symbol.to_string(),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Short),
            order_type: OrderType::Market,
            time_in_force: None,
            qty: Some(qty_float),
            reduce_only: None,
            price: None,
            stop_price: None,
            close_position: None,
            activation_price: None,
            callback_rate: None,
            working_type: None,
            price_protect: None,
            new_client_order_id: Some(order_id),
        };

        match self.account_client.custom_order(c_request) {
            Ok(transaction) => {
                info!(
                    "✓ Closed short position successfully: {} quantity: {}",
                    symbol, quantity_str
                );
                let _ = self.cancel_all_orders(symbol).await;
                Ok(json!({
                    "orderId": transaction.order_id,
                    "symbol": transaction.symbol,
                    "status": transaction.status
                }))
            }
            Err(e) => Err(anyhow!("failed to close short position: {:?}", e)),
        }
    }

    #[instrument(skip_all)]
    async fn set_leverage(&self, symbol: &str, leverage: i32) -> Result<()> {
        // Check current leverage (optimistic check)
        if let Ok(positions) = self.get_positions().await {
            for pos in positions {
                if pos["symbol"] == symbol {
                    if let Some(lev) = pos["leverage"].as_f64() {
                        if lev as i32 == leverage {
                            info!("  ✓ {} leverage is already {}x", symbol, leverage);
                            return Ok(());
                        }
                    }
                }
            }
        }

        match self
            .account_client
            .change_initial_leverage(symbol, leverage as u8)
        {
            Ok(_) => {
                info!("  ✓ {} leverage changed to {}x", symbol, leverage);
                info!("  ⏱ Waiting 5 seconds for cooldown period...");
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(())
            }
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("No need to change") {
                    info!("  ✓ {} leverage is already {}x", symbol, leverage);
                    Ok(())
                } else {
                    Err(anyhow!("failed to set leverage: {:?}", e))
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn set_margin_mode(&self, symbol: &str, is_cross_margin: bool) -> Result<()> {
        let mode_str = if is_cross_margin {
            "Cross Margin"
        } else {
            "Isolated Margin"
        };

        match self
            .account_client
            .change_margin_type(symbol, is_cross_margin)
        {
            Ok(_) => {
                info!("  ✓ {} margin mode set to {}", symbol, mode_str);
                Ok(())
            }
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("No need to change") {
                    info!("  ✓ {} margin mode is already {}", symbol, mode_str);
                    return Ok(());
                }
                if err_str.contains("exists position") {
                    warn!(
                        "  ⚠️ {} has open positions, cannot change margin mode",
                        symbol
                    );
                    return Ok(());
                }
                if err_str.contains("Multi-Assets mode") || err_str.contains("-4168") {
                    warn!(
                        "  ⚠️ {} detected Multi-Assets mode, forcing Cross Margin mode",
                        symbol
                    );
                    return Ok(());
                }
                if err_str.to_lowercase().contains("unified")
                    || err_str.to_lowercase().contains("portfolio")
                {
                    error!("  ❌ {} detected Unified Account API", symbol);
                    return Err(anyhow!("Unified Account API detected"));
                }

                warn!("  ⚠️ Failed to set margin mode: {}", err_str);
                Ok(())
            }
        }
    }

    async fn get_market_price(&self, symbol: &str) -> Result<f64> {
        match self.market_client.get_price(symbol) {
            Ok(price_symbol) => Ok(price_symbol.price),
            Err(e) => Err(anyhow!("failed to get price: {:?}", e)),
        }
    }

    async fn set_stop_loss(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        stop_price: f64,
    ) -> Result<()> {
        let (side, pos_side) = if position_side == "LONG" {
            (OrderSide::Sell, PositionSide::Long)
        } else {
            (OrderSide::Buy, PositionSide::Short)
        };

        let quantity_str = self.format_quantity(symbol, quantity).await?;
        let qty_float: f64 = quantity_str.parse()?;
        let order_id = Self::get_br_order_id();

        let c_request = CustomOrderRequest {
            symbol: symbol.to_string(),
            side: side,
            position_side: Some(pos_side),
            order_type: OrderType::StopMarket,
            time_in_force: None,
            qty: Some(qty_float),
            reduce_only: None,
            price: None,
            stop_price: Some(stop_price),
            close_position: Some(true),
            activation_price: None,
            callback_rate: None,
            working_type: Some(WorkingType::ContractPrice),
            price_protect: None,
            new_client_order_id: Some(order_id),
        };

        match self.account_client.custom_order(c_request) {
            Ok(_) => {
                info!("  Stop-loss price set: {:.4}", stop_price);
                Ok(())
            }
            Err(e) => Err(anyhow!("failed to set stop-loss: {:?}", e)),
        }
    }

    async fn set_take_profit(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        take_profit_price: f64,
    ) -> Result<()> {
        let (side, pos_side) = if position_side == "LONG" {
            (OrderSide::Sell, PositionSide::Long)
        } else {
            (OrderSide::Buy, PositionSide::Short)
        };

        let quantity_str = self.format_quantity(symbol, quantity).await?;
        let qty_float: f64 = quantity_str.parse()?;
        let order_id = Self::get_br_order_id();

        let c_request = CustomOrderRequest {
            symbol: symbol.to_string(),
            side: side,
            position_side: Some(pos_side),
            order_type: OrderType::TakeProfit,
            time_in_force: None,
            qty: Some(qty_float),
            reduce_only: None,
            price: None,
            stop_price: Some(take_profit_price),
            close_position: Some(true),
            activation_price: None,
            callback_rate: None,
            working_type: Some(WorkingType::ContractPrice),
            price_protect: None,
            new_client_order_id: Some(order_id),
        };

        match self.account_client.custom_order(c_request) {
            Ok(_) => {
                info!("  Take-profit price set: {:.4}", take_profit_price);
                Ok(())
            }
            Err(e) => Err(anyhow!("failed to set take-profit: {:?}", e)),
        }
    }

    // This implements the logic to selectively cancel StopLoss/TakeProfit orders
    #[instrument(skip_all)]
    async fn cancel_stop_loss_orders(&self, symbol: &str) -> Result<()> {
        let open_orders = self
            .account_client
            .get_all_open_orders(symbol)
            .map_err(|e| anyhow!("{:?}", e))?;

        let mut canceled_count = 0;
        let mut errors = Vec::new();

        for order in open_orders {
            if order.order_type == OrderType::StopMarket.to_string()
                || order.order_type == OrderType::Stop.to_string()
            {
                match self.account_client.cancel_order(symbol, order.order_id) {
                    Ok(_) => {
                        canceled_count += 1;
                        info!(
                            "  ✓ Canceled stop-loss order (Order ID: {}, Type: {:?}, Side: {:?})",
                            order.order_id, order.order_type, order.position_side
                        );
                    }
                    Err(e) => {
                        let msg = format!("Order ID {}: {:?}", order.order_id, e);
                        info!("  ⚠ Failed to cancel stop-loss order: {}", msg);
                        errors.push(msg);
                    }
                }
            }
        }

        if canceled_count == 0 && errors.is_empty() {
            info!("  ℹ {} has no stop-loss orders to cancel", symbol);
        } else if canceled_count > 0 {
            info!(
                "  ✓ Canceled {} stop-loss order(s) for {}",
                canceled_count, symbol
            );
        }

        if !errors.is_empty() && canceled_count == 0 {
            Err(anyhow!("failed to cancel stop-loss orders: {:?}", errors))
        } else {
            Ok(())
        }
    }

    #[instrument(skip_all)]
    async fn cancel_take_profit_orders(&self, symbol: &str) -> Result<()> {
        let open_orders = self
            .account_client
            .get_all_open_orders(symbol)
            .map_err(|e| anyhow!("{:?}", e))?;

        let mut canceled_count = 0;
        let mut errors = Vec::new();

        for order in open_orders {
            if order.order_type == OrderType::TakeProfitMarket.to_string()
                || order.order_type == OrderType::TakeProfit.to_string()
            {
                match self.account_client.cancel_order(symbol, order.order_id) {
                    Ok(_) => {
                        canceled_count += 1;
                        info!(
                            "  ✓ Canceled take-profit order (Order ID: {}, Type: {:?}, Side: {:?})",
                            order.order_id, &order.order_type, order.position_side
                        );
                    }
                    Err(e) => {
                        let msg = format!("Order ID {}: {:?}", order.order_id, e);
                        info!("  ⚠ Failed to cancel take-profit order: {}", msg);
                        errors.push(msg);
                    }
                }
            }
        }

        if canceled_count == 0 && errors.is_empty() {
            info!("  ℹ {} has no take-profit orders to cancel", symbol);
        } else if canceled_count > 0 {
            info!(
                "  ✓ Canceled {} take-profit order(s) for {}",
                canceled_count, symbol
            );
        }

        if !errors.is_empty() && canceled_count == 0 {
            Err(anyhow!("failed to cancel take-profit orders: {:?}", errors))
        } else {
            Ok(())
        }
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<()> {
        match self.account_client.cancel_all_open_orders(symbol) {
            Ok(_) => {
                info!("  ✓ Canceled all pending orders for {}", symbol);
                Ok(())
            }
            Err(e) => Err(anyhow!("failed to cancel pending orders: {:?}", e)),
        }
    }

    async fn cancel_stop_orders(&self, _symbol: &str) -> Result<()> {
        Ok(())
    }

    async fn format_quantity(&self, symbol: &str, quantity: f64) -> Result<String> {
        let precision = self.get_symbol_precision(symbol).await?;
        Ok(format!("{:.1$}", quantity, precision))
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<Map<String, Value>> {
        let oid: u64 = order_id.parse().map_err(|_| anyhow!("invalid order ID"))?;

        let orders = self
            .account_client
            .get_all_orders(symbol, Some(oid), None, None, None)
            .map_err(|e| anyhow!("Binance error: {:?}", e))?;

        // Safely access the first element
        if let Some(order) = orders.first() {
            let mut status_map = Map::new();
            status_map.insert("orderId".into(), order.order_id.into());
            status_map.insert("symbol".into(), order.symbol.clone().into());
            status_map.insert("status".into(), order.status.clone().into());
            status_map.insert("avgPrice".into(), order.avg_price.into());
            status_map.insert("executedQty".into(), order.executed_qty.into());
            status_map.insert("side".into(), order.side.clone().into());
            status_map.insert("type".into(), order.order_type.clone().into());
            status_map.insert("time".into(), order.time_in_force.clone().into());
            status_map.insert("updateTime".into(), order.update_time.into());
            status_map.insert("commission".into(), 0.0.into());

            Ok(status_map)
        } else {
            Err(anyhow!("Order not found"))
        }
    }
}
