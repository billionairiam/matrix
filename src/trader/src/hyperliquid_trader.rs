use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use hyperliquid_rust_sdk::{
    BaseUrl, ClientCancelRequest, ClientLimit, ClientOrder, ClientOrderRequest, ClientTrigger,
    ExchangeClient, InfoClient, Meta,
};
use serde_json::{Map, Value, json};
use tokio::sync::RwLock;
use tracing::{info, instrument, warn};

use super::Trader;

pub struct HyperliquidTrader {
    exchange: ExchangeClient,
    info: InfoClient,
    wallet_addr: Address,
    // RwLock for concurrent access, Option because it might be empty initially
    meta: RwLock<Option<Meta>>,
    is_cross_margin: AtomicBool,
}

impl HyperliquidTrader {
    /// Creates a new Hyperliquid trader instance
    #[instrument(skip_all)]
    pub async fn new(private_key_hex: &str, wallet_addr_str: &str, testnet: bool) -> Result<Self> {
        // Remove 0x prefix if present
        let private_key_clean = private_key_hex.trim_start_matches("0x");

        // Parse private key
        let wallet: PrivateKeySigner = private_key_clean
            .parse()
            .context("Failed to parse private key")?;

        let api_url = if testnet {
            BaseUrl::Testnet
        } else {
            BaseUrl::Mainnet
        };

        // Security enhancement: Implement Agent Wallet best practices
        let agent_addr = wallet.address();
        let main_wallet_addr =
            Address::from_str(wallet_addr_str).context("Failed to parse wallet address")?;

        if wallet_addr_str.is_empty() {
            return Err(anyhow!(
                "❌ Configuration error: Main wallet address not provided. \
                Please provide the address that holds the funds."
            ));
        }

        // Check if user accidentally uses main wallet private key
        if main_wallet_addr == agent_addr {
            warn!(
                "⚠️⚠️⚠️ WARNING: Main wallet address ({:?}) matches Agent wallet address!",
                main_wallet_addr
            );
            warn!("   This indicates you may be using your main wallet private key!");
            warn!("   Recommendation: Create a separate Agent Wallet on Hyperliquid.");
        } else {
            info!("✓ Using Agent Wallet mode (secure)");
            info!("  └─ Agent wallet address: {:?} (for signing)", agent_addr);
            info!(
                "  └─ Main wallet address: {:?} (holds funds)",
                main_wallet_addr
            );
        }

        // Initialize Clients
        let exchange = ExchangeClient::new(None, wallet.clone(), Some(api_url), None, None)
            .await
            .map_err(|e| anyhow!("Failed to create exchange client: {:?}", e))?;

        let info_client = InfoClient::new(None, Some(api_url))
            .await
            .map_err(|e| anyhow!("Failed to create info client: {:?}", e))?;

        info!(
            "✓ Hyperliquid trader initialized successfully (testnet={}, wallet={:?})",
            testnet, main_wallet_addr
        );

        // Get meta information
        let meta = info_client
            .meta()
            .await
            .map_err(|e| anyhow!("Failed to get meta information: {:?}", e))?;

        // 🔍 Security check: Validate Agent wallet balance
        if main_wallet_addr != agent_addr {
            match info_client.user_state(agent_addr).await {
                Ok(agent_state) => {
                    let agent_balance: f64 = agent_state
                        .cross_margin_summary
                        .account_value
                        .parse()
                        .unwrap_or(0.0);

                    if agent_balance > 100.0 {
                        info!("🚨🚨🚨 CRITICAL SECURITY WARNING 🚨🚨🚨");
                        info!("   Agent wallet balance: {:.2} USDC", agent_balance);
                        return Err(anyhow!(
                            "Security check failed: Agent wallet balance too high"
                        ));
                    } else if agent_balance > 10.0 {
                        info!(
                            "⚠️  Notice: Agent wallet address has some balance: {:.2} USDC",
                            agent_balance
                        );
                    } else {
                        info!("✓ Agent wallet balance is safe: {:.2} USDC", agent_balance);
                    }
                }
                Err(e) => {
                    info!("⚠️  Could not verify Agent wallet balance: {:?}", e);
                }
            }
        }

        Ok(Self {
            exchange,
            info: info_client,
            wallet_addr: main_wallet_addr,
            meta: RwLock::new(Some(meta)),
            is_cross_margin: AtomicBool::new(true),
        })
    }

    fn convert_symbol_to_hyperliquid(&self, symbol: &str) -> String {
        symbol.strip_suffix("USDT").unwrap_or(symbol).to_string()
    }

    #[instrument(skip_all)]
    async fn get_sz_decimals(&self, coin: &str) -> u32 {
        let meta_guard = self.meta.read().await;
        if let Some(meta) = &*meta_guard {
            for asset in &meta.universe {
                if asset.name == coin {
                    return asset.sz_decimals;
                }
            }
        }
        warn!("⚠️ Precision not found for {}, using default 4", coin);
        4
    }

    async fn round_to_sz_decimals(&self, coin: &str, quantity: f64) -> f64 {
        let decimals = self.get_sz_decimals(coin).await;
        let multiplier = 10f64.powi(decimals as i32);
        (quantity * multiplier + 0.5).floor() / multiplier
    }

    fn round_price_to_sigfigs(&self, price: f64) -> f64 {
        if price == 0.0 {
            return 0.0;
        }
        let sigfigs = 5;
        let mut magnitude = price.abs();
        let mut multiplier = 1.0;

        while magnitude >= 10.0 {
            magnitude /= 10.0;
            multiplier /= 10.0;
        }
        while magnitude < 1.0 {
            magnitude *= 10.0;
            multiplier *= 10.0;
        }

        for _ in 0..(sigfigs - 1) {
            multiplier *= 10.0;
        }

        (price * multiplier + 0.5).floor() / multiplier
    }
}

#[async_trait]
impl Trader for HyperliquidTrader {
    /// Gets account balance
    #[instrument(skip_all)]
    async fn get_balance(&self) -> Result<Map<String, Value>> {
        info!("🔄 Calling Hyperliquid API to get account balance...");

        // Get Spot Balance
        let mut spot_usdc_balance = 0.0;
        match self.info.user_token_balances(self.wallet_addr).await {
            Ok(spot_state) => {
                for balance in spot_state.balances {
                    if balance.coin == "USDC" {
                        spot_usdc_balance = balance.total.parse().unwrap_or(0.0);
                        info!("✓ Found Spot balance: {:.2} USDC", spot_usdc_balance);
                        break;
                    }
                }
            }
            Err(e) => warn!("⚠️ Failed to query Spot balance: {:?}", e),
        }

        // Get Perp user state
        let account_state = self
            .info
            .user_state(self.wallet_addr)
            .await
            .map_err(|e| anyhow!("Failed to get account information: {:?}", e))?;

        // Select summary based on margin mode
        let is_cross = self.is_cross_margin.load(Ordering::Relaxed);
        let (account_value, total_margin_used): (f64, f64) = if is_cross {
            (
                account_state
                    .cross_margin_summary
                    .account_value
                    .parse()
                    .unwrap_or(0.0),
                account_state
                    .cross_margin_summary
                    .total_margin_used
                    .parse()
                    .unwrap_or(0.0),
            )
        } else {
            (
                account_state
                    .margin_summary
                    .account_value
                    .parse()
                    .unwrap_or(0.0),
                account_state
                    .margin_summary
                    .total_margin_used
                    .parse()
                    .unwrap_or(0.0),
            )
        };

        // Accumulate unrealized PnL
        let mut total_unrealized_pnl = 0.0;
        for asset_pos in &account_state.asset_positions {
            let upnl: f64 = asset_pos.position.unrealized_pnl.parse().unwrap_or(0.0);
            total_unrealized_pnl += upnl;
        }

        let wallet_balance_without_unrealized = account_value - total_unrealized_pnl;

        // Use Withdrawable field
        let mut available_balance = account_state.withdrawable.parse().unwrap_or(0.0);

        // Fallback
        if available_balance == 0.0 && account_state.withdrawable == "0.0" {
            available_balance = account_value - total_margin_used;
            if available_balance < 0.0 {
                available_balance = 0.0;
            }
        }

        let total_wallet_balance = wallet_balance_without_unrealized + spot_usdc_balance;

        info!("✓ Hyperliquid complete account:");
        info!("  • Spot balance: {:.2} USDC", spot_usdc_balance);
        info!("  • Perpetuals equity: {:.2} USDC", account_value);
        info!("  • Perpetuals available: {:.2} USDC", available_balance);
        info!("  • Total assets: {:.2} USDC", total_wallet_balance);

        let mut result = Map::new();
        result.insert(
            "totalWalletBalance".to_string(),
            json!(total_wallet_balance),
        );
        result.insert("availableBalance".to_string(), json!(available_balance));
        result.insert(
            "totalUnrealizedProfit".to_string(),
            json!(total_unrealized_pnl),
        );
        result.insert("spotBalance".to_string(), json!(spot_usdc_balance));

        Ok(result)
    }

    /// Gets all positions
    async fn get_positions(&self) -> Result<Vec<Map<String, Value>>> {
        let account_state = self
            .info
            .user_state(self.wallet_addr)
            .await
            .map_err(|e| anyhow!("Failed to get positions: {:?}", e))?;

        let mut result = Vec::new();

        for asset_pos in account_state.asset_positions {
            let position = asset_pos.position;
            let pos_amt: f64 = position.szi.parse().unwrap_or(0.0);

            if pos_amt == 0.0 {
                continue;
            }

            let mut pos_map = Map::new();
            let symbol = format!("{}USDT", position.coin);

            pos_map.insert("symbol".to_string(), json!(symbol));

            if pos_amt > 0.0 {
                pos_map.insert("side".to_string(), json!("long"));
                pos_map.insert("positionAmt".to_string(), json!(pos_amt));
            } else {
                pos_map.insert("side".to_string(), json!("short"));
                pos_map.insert("positionAmt".to_string(), json!(-pos_amt));
            }

            let entry_price: f64 = position.entry_px.unwrap_or_default().parse().unwrap_or(0.0);
            let liquidation_px: f64 = position
                .liquidation_px
                .unwrap_or_default()
                .parse()
                .unwrap_or(0.0);
            let position_value: f64 = position.position_value.parse().unwrap_or(0.0);
            let unrealized_pnl: f64 = position.unrealized_pnl.parse().unwrap_or(0.0);

            let mark_price = if pos_amt != 0.0 {
                position_value / pos_amt.abs()
            } else {
                0.0
            };

            pos_map.insert("entryPrice".to_string(), json!(entry_price));
            pos_map.insert("markPrice".to_string(), json!(mark_price));
            pos_map.insert("unRealizedProfit".to_string(), json!(unrealized_pnl));
            pos_map.insert("leverage".to_string(), json!(position.leverage.value));
            pos_map.insert("liquidationPrice".to_string(), json!(liquidation_px));

            result.push(pos_map);
        }

        Ok(result)
    }

    /// Opens a long position
    #[instrument(skip_all)]
    async fn open_long(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value> {
        self.cancel_all_orders(symbol)
            .await
            .unwrap_or_else(|e| info!("  ⚠ Failed to cancel old pending orders: {:?}", e));
        self.set_leverage(symbol, leverage).await?;

        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let price = self.get_market_price(symbol).await?;

        let rounded_qty = self.round_to_sz_decimals(&coin, quantity).await;
        let aggressive_price = self.round_price_to_sigfigs(price * 1.01);

        info!("  📏 Quantity: {:.8} -> {:.8}", quantity, rounded_qty);
        info!("  💰 Price: {:.8} -> {:.8}", price * 1.01, aggressive_price);

        let order = ClientOrderRequest {
            asset: coin.clone(),
            is_buy: true,
            sz: rounded_qty,
            limit_px: aggressive_price,
            order_type: ClientOrder::Limit(ClientLimit {
                tif: "Ioc".to_string(),
            }),
            reduce_only: false,
            cloid: None,
        };

        self.exchange
            .order(order, None)
            .await
            .map_err(|e| anyhow!("Failed to open long: {:?}", e))?;
        info!(
            "✓ Long position opened: {} quantity: {:.4}",
            symbol, rounded_qty
        );

        let val = json!({
            "orderId": 0,
            "symbol": symbol,
            "status": "FILLED"
        });
        Ok(val)
    }

    /// Opens a short position
    async fn open_short(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value> {
        self.cancel_all_orders(symbol)
            .await
            .unwrap_or_else(|e| info!("  ⚠ Failed to cancel old pending orders: {:?}", e));
        self.set_leverage(symbol, leverage).await?;

        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let price = self.get_market_price(symbol).await?;

        let rounded_qty = self.round_to_sz_decimals(&coin, quantity).await;
        let aggressive_price = self.round_price_to_sigfigs(price * 0.99);

        let order = ClientOrderRequest {
            asset: coin.clone(),
            is_buy: false,
            sz: rounded_qty,
            limit_px: aggressive_price,
            order_type: ClientOrder::Limit(ClientLimit {
                tif: "Ioc".to_string(),
            }),
            reduce_only: false,
            cloid: None,
        };

        self.exchange
            .order(order, None)
            .await
            .map_err(|e| anyhow!("Failed to open short: {:?}", e))?;
        info!(
            "✓ Short position opened: {} quantity: {:.4}",
            symbol, rounded_qty
        );

        let val = json!({
            "orderId": 0,
            "symbol": symbol,
            "status": "FILLED"
        });
        Ok(val)
    }

    /// Closes a long position
    async fn close_long(&self, symbol: &str, mut quantity: f64) -> Result<Value> {
        if quantity == 0.0 {
            let positions = self.get_positions().await?;
            for pos in positions {
                if pos.get("symbol").and_then(|v| v.as_str()) == Some(symbol)
                    && pos.get("side").and_then(|v| v.as_str()) == Some("long")
                {
                    quantity = pos
                        .get("positionAmt")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    break;
                }
            }
            if quantity == 0.0 {
                return Err(anyhow!("No long position found for {}", symbol));
            }
        }

        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let price = self.get_market_price(symbol).await?;
        let rounded_qty = self.round_to_sz_decimals(&coin, quantity).await;
        let aggressive_price = self.round_price_to_sigfigs(price * 0.99);

        let order = ClientOrderRequest {
            asset: coin.clone(),
            is_buy: false,
            sz: rounded_qty,
            limit_px: aggressive_price,
            order_type: ClientOrder::Limit(ClientLimit {
                tif: "Ioc".to_string(),
            }),
            reduce_only: true,
            cloid: None,
        };

        self.exchange
            .order(order, None)
            .await
            .map_err(|e| anyhow!("Failed to close long: {:?}", e))?;
        info!(
            "✓ Long position closed: {} quantity: {:.4}",
            symbol, rounded_qty
        );

        self.cancel_all_orders(symbol).await.unwrap_or_default();

        let val = json!({
            "orderId": 0,
            "symbol": symbol,
            "status": "FILLED"
        });
        Ok(val)
    }

    /// Closes a short position
    #[instrument(skip_all)]
    async fn close_short(&self, symbol: &str, mut quantity: f64) -> Result<Value> {
        if quantity == 0.0 {
            let positions = self.get_positions().await?;
            for pos in positions {
                if pos.get("symbol").and_then(|v| v.as_str()) == Some(symbol)
                    && pos.get("side").and_then(|v| v.as_str()) == Some("short")
                {
                    quantity = pos
                        .get("positionAmt")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    break;
                }
            }
            if quantity == 0.0 {
                return Err(anyhow!("No short position found for {}", symbol));
            }
        }

        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let price = self.get_market_price(symbol).await?;
        let rounded_qty = self.round_to_sz_decimals(&coin, quantity).await;
        let aggressive_price = self.round_price_to_sigfigs(price * 1.01);

        let order = ClientOrderRequest {
            asset: coin.clone(),
            is_buy: true,
            sz: rounded_qty,
            limit_px: aggressive_price,
            order_type: ClientOrder::Limit(ClientLimit {
                tif: "Ioc".to_string(),
            }),
            reduce_only: true,
            cloid: None,
        };

        self.exchange
            .order(order, None)
            .await
            .map_err(|e| anyhow!("Failed to close short: {:?}", e))?;
        info!(
            "✓ Short position closed: {} quantity: {:.4}",
            symbol, rounded_qty
        );

        self.cancel_all_orders(symbol).await.unwrap_or_default();

        let val = json!({
            "orderId": 0,
            "symbol": symbol,
            "status": "FILLED"
        });
        Ok(val)
    }

    /// Sets leverage
    #[instrument(skip_all)]
    async fn set_leverage(&self, symbol: &str, leverage: i32) -> Result<()> {
        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let is_cross = self.is_cross_margin.load(Ordering::Relaxed);

        // API call to update leverage
        self.exchange
            .update_leverage(leverage as u32, &coin, is_cross, None)
            .await
            .map_err(|e| anyhow!("Failed to set leverage: {:?}", e))?;

        info!("  ✓ {} leverage switched to {}x", symbol, leverage);
        Ok(())
    }

    /// Sets margin mode
    async fn set_margin_mode(&self, symbol: &str, is_cross_margin: bool) -> Result<()> {
        self.is_cross_margin
            .store(is_cross_margin, Ordering::Relaxed);
        let mode_str = if is_cross_margin {
            "cross margin"
        } else {
            "isolated margin"
        };
        info!("  ✓ {} will use {} mode", symbol, mode_str);
        Ok(())
    }

    /// Get Market Price
    async fn get_market_price(&self, symbol: &str) -> Result<f64> {
        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let all_mids = self
            .info
            .all_mids()
            .await
            .map_err(|e| anyhow!("Failed to get mids: {:?}", e))?;

        if let Some(price_str) = all_mids.get(&coin) {
            let price = price_str.parse::<f64>().context("Price format error")?;
            Ok(price)
        } else {
            Err(anyhow!("Price not found for {}", symbol))
        }
    }

    /// Set Stop Loss
    async fn set_stop_loss(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        stop_price: f64,
    ) -> Result<()> {
        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let is_buy = position_side == "SHORT"; // Closing short = buying

        let rounded_qty = self.round_to_sz_decimals(&coin, quantity).await;
        let rounded_price = self.round_price_to_sigfigs(stop_price);

        let order = ClientOrderRequest {
            asset: coin.clone(),
            is_buy: is_buy,
            sz: rounded_qty,
            limit_px: rounded_price,
            order_type: ClientOrder::Trigger(ClientTrigger {
                is_market: true,
                trigger_px: rounded_price,
                tpsl: "sl".to_string(),
            }),
            reduce_only: true,
            cloid: None,
        };

        self.exchange
            .order(order, None)
            .await
            .map_err(|e| anyhow!("Failed to set SL: {:?}", e))?;
        info!("  Stop loss price set: {:.4}", rounded_price);
        Ok(())
    }

    /// Set Take Profit
    async fn set_take_profit(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        tp_price: f64,
    ) -> Result<()> {
        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let is_buy = position_side == "SHORT";

        let rounded_qty = self.round_to_sz_decimals(&coin, quantity).await;
        let rounded_price = self.round_price_to_sigfigs(tp_price);

        let order = ClientOrderRequest {
            asset: coin.clone(),
            is_buy: is_buy,
            sz: rounded_qty,
            limit_px: rounded_price,
            order_type: ClientOrder::Trigger(ClientTrigger {
                is_market: true,
                trigger_px: rounded_price,
                tpsl: "tp".to_string(),
            }),
            reduce_only: true,
            cloid: None,
        };

        self.exchange
            .order(order, None)
            .await
            .map_err(|e| anyhow!("Failed to set TP: {:?}", e))?;
        info!("  Take profit price set: {:.4}", rounded_price);
        Ok(())
    }

    // Since API can't distinguish TP/SL easily, we reuse logic
    #[instrument(skip_all)]
    async fn cancel_stop_loss_orders(&self, symbol: &str) -> Result<()> {
        warn!("  ⚠️ Hyperliquid cannot distinguish stop/limit, cancelling all for symbol");
        self.cancel_all_orders(symbol).await
    }

    #[instrument(skip_all)]
    async fn cancel_take_profit_orders(&self, symbol: &str) -> Result<()> {
        warn!("  ⚠️ Hyperliquid cannot distinguish stop/limit, cancelling all for symbol");
        self.cancel_all_orders(symbol).await
    }

    /// Cancel orders
    #[instrument(skip_all)]
    async fn cancel_all_orders(&self, symbol: &str) -> Result<()> {
        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let open_orders = self
            .info
            .open_orders(self.wallet_addr)
            .await
            .map_err(|e| anyhow!("{:?}", e))?;

        for order in open_orders {
            if order.coin == coin {
                // Ignore cancel errors
                let c_req = ClientCancelRequest {
                    asset: coin.clone(),
                    oid: order.oid,
                };
                let _ = self.exchange.cancel(c_req, None).await;
            }
        }
        info!("  ✓ Cancelled all pending orders for {}", symbol);
        Ok(())
    }

    async fn cancel_stop_orders(&self, _symbol: &str) -> Result<()> {
        Ok(())
    }

    async fn format_quantity(&self, _symbol: &str, _quantity: f64) -> Result<String> {
        Ok(String::new())
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<Map<String, Value>> {
        let coin = self.convert_symbol_to_hyperliquid(symbol);
        let mut status_map = Map::new();

        if let Ok(open_orders) = self.info.open_orders(self.wallet_addr).await {
            for order in open_orders {
                if order.coin == coin && order.oid == order_id.parse::<u64>().unwrap_or_default() {
                    status_map.insert("orderId".to_string(), order_id.into());
                    status_map.insert("status".to_string(), "NEW".into());
                    status_map.insert("avgPrice".to_string(), 0.0.into());
                    status_map.insert("executedQty".to_string(), 0.0.into());
                    status_map.insert("commission".to_string(), 0.0.into());
                    return Ok(status_map);
                }
            }
        } else {
            status_map.insert("orderId".to_string(), order_id.into());
            status_map.insert("status".to_string(), "FILLED".into());
            status_map.insert("avgPrice".to_string(), 0.0.into());
            status_map.insert("executedQty".to_string(), 0.0.into());
            status_map.insert("commission".to_string(), 0.0.into());
        }

        Ok(status_map)
    }
}
