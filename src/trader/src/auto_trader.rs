use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::Trader;
use super::aster_trader::AsterTrader;
use super::binance_futures::FuturesTrader;
use super::hyperliquid_trader::HyperliquidTrader;
use anyhow::{Result, anyhow};
use chrono::SecondsFormat;
use chrono::{DateTime, Utc};
use decision::engine::Context;
use decision::engine::Decision;
use decision::engine::PositionInfo;
use decision::engine::TradingStats;
use decision::engine::get_full_decision_with_strategy;
use decision::engine::{AccountInfo, RecentOrder};
use decision::strategy_engine::StrategyEngine;
use market::data::get;
use mcp::client::Client;
use mcp::custom_client::CustomProvider;
use mcp::deepseek_client::DeepseekProvider;
use mcp::qwen_client::QwenProvider;
use pool::coin_pool::CoinPoolClient;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use store::decision::DecisionAction;
use store::decision::DecisionRecord;
use store::equity::EquitySnapshot;
use store::order::TraderOrder;
use store::store::Store;
use store::strategy::StrategyConfig;
use tokio::sync::{Notify, RwLock};
use tokio::time::interval;
use tracing::{error, info, warn};

#[derive(Debug, Serialize)]
pub struct PositionResponse {
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub mark_price: f64,
    pub quantity: f64,
    pub leverage: i32,
    pub unrealized_pnl: f64,
    pub unrealized_pnl_pct: f64,
    pub liquidation_price: f64,
    pub margin_used: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccountBalance {
    pub total_wallet_balance: f64,
    pub total_unrealized_profit: f64,
    pub available_balance: f64,
    // Add raw access if specific exchanges have unique fields
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub side: String, // "LONG" or "SHORT" or "BOTH"
    pub amount: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub leverage: f64,
    pub liquidation_price: f64,
    #[serde(flatten)]
    pub raw: HashMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderResult {
    pub order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTraderConfig {
    pub id: String,
    pub name: String,
    pub ai_model: String, // "qwen", "deepseek", or "custom"

    // Supported Exchanges
    pub exchange: String, // "binance", "hyperliquid", "aster"

    // Binance
    pub binance_api_key: String,
    pub binance_secret_key: String,

    // Hyperliquid
    pub hyperliquid_private_key: String,
    pub hyperliquid_wallet_addr: String,
    pub hyperliquid_testnet: bool,

    // Aster
    pub aster_user: String,
    pub aster_signer: String,
    pub aster_private_key: String,

    // AI Config
    pub use_qwen: bool,
    pub deepseek_key: String,
    pub qwen_key: String,
    pub custom_api_url: String,
    pub custom_api_key: String,
    pub custom_model_name: String,

    // Runtime
    pub scan_interval_sec: u64,
    pub initial_balance: f64,
    pub is_cross_margin: bool,

    // Complex Objects
    #[serde(skip)]
    pub strategy_config: Option<StrategyConfig>,
}

#[derive(Clone)]
pub struct AutoTrader {
    // Identity
    id: String,
    name: String,
    exchange: String,
    ai_model: String,
    user_id: String,

    // Components
    config: AutoTraderConfig,
    trader: Arc<Box<dyn Trader>>, // Polymorphic trader
    mcp_client: Arc<Client>,
    store: Arc<Option<Store>>,
    strategy_engine: Arc<Option<StrategyEngine>>,

    // State
    is_running: Arc<RwLock<bool>>,
    stop_notify: Arc<Notify>, // To signal background tasks to stop

    // Metrics & Tracking
    start_time: DateTime<Utc>,
    cycle_number: Arc<RwLock<i32>>,
    call_count: Arc<RwLock<i32>>,
    initial_balance: Arc<RwLock<f64>>,
    daily_pnl: Arc<RwLock<f64>>,
    last_reset_time: Arc<RwLock<DateTime<Utc>>>,
    stop_until: Arc<RwLock<Option<DateTime<Utc>>>>,

    // Caches
    position_first_seen: Arc<RwLock<HashMap<String, i64>>>,
    peak_pnl_cache: Arc<RwLock<HashMap<String, f64>>>,

    // Prompt overrides
    custom_prompt: Arc<RwLock<Option<String>>>,
    override_base_prompt: Arc<RwLock<bool>>,

    // coin pool
    coin_pool: Arc<CoinPoolClient>,
}

impl AutoTrader {
    pub async fn new(
        mut config: AutoTraderConfig,
        st: Arc<Option<Store>>,
        user_id: String,
    ) -> Result<Self> {
        // Defaults
        if config.id.is_empty() {
            config.id = "default_trader".to_string();
        }
        if config.name.is_empty() {
            config.name = "Default Trader".to_string();
        }
        if config.ai_model.is_empty() {
            config.ai_model = if config.use_qwen {
                "qwen".to_string()
            } else {
                "deepseek".to_string()
            };
        }

        info!("🤖 [{}] Using AI Model: {}", &config.name, &config.ai_model);

        let mcp_client = match config.ai_model.as_str() {
            "custom" => Client::builder(Arc::new(CustomProvider)).build(),
            "qwen" => Client::builder(Arc::new(QwenProvider)).build(),
            _ => Client::builder(Arc::new(DeepseekProvider)).build(),
        };

        // Initialize Trader
        let trader: Box<dyn Trader> = match config.exchange.as_str() {
            "binance" => {
                info!("🏦 [{}] Using Binance Futures", config.name);
                Box::new(FuturesTrader::new(
                    &config.binance_api_key,
                    &config.binance_secret_key,
                ))
            }
            "hyperliquid" => {
                info!("🏦 [{}] Using Hyperliquid", config.name);
                Box::new(
                    HyperliquidTrader::new(
                        &config.hyperliquid_private_key,
                        &config.hyperliquid_wallet_addr,
                        config.hyperliquid_testnet,
                    )
                    .await
                    .unwrap(),
                )
            }
            "aster" => {
                info!("🏦 [{}] Using Aster", config.name);
                Box::new(
                    AsterTrader::new(
                        &config.aster_user,
                        &config.aster_signer,
                        &config.aster_private_key,
                    )
                    .unwrap(),
                )
            }
            _ => return Err(anyhow!("Unsupported exchange: {}", config.exchange)),
        };

        // Auto-fetch balance if not set
        if config.initial_balance <= 0.0 {
            info!("📊 [{}] Fetching initial balance...", config.name);
            let bal = trader.get_balance().await?;
            let balance_keys = [
                "total_equity",
                "totalWalletBalance",
                "wallet_balance",
                "totalEq",
                "balance",
            ];
            let found_bal: Vec<f64> = get_values(&balance_keys, &bal)
                .iter()
                .filter_map(|v| match v {
                    Value::Number(n) => n.as_f64(),
                    Value::String(s) => s.parse::<f64>().ok(),
                    _ => None,
                })
                .collect();

            if !found_bal.is_empty() && found_bal[0] > 0.0 {
                config.initial_balance = found_bal[0];
                info!(
                    "✓ [{}] Auto-fetched initial balance: {:.2} USDT",
                    config.name, found_bal[0]
                );
            } else {
                return Err(anyhow!("Initial balance must be > 0"));
            }
        }

        // Recover Cycle Number
        let mut cycle_number = 0;
        if let Some(store) = (*st).as_ref() {
            cycle_number = store
                .decision()
                .get_last_cycle_number(&config.id)
                .await
                .unwrap_or(0);
        }

        // Strategy Engine
        let strategy_cfg = config
            .strategy_config
            .clone()
            .ok_or_else(|| anyhow!("Strategy config missing"))?;
        let strategy_engine = StrategyEngine::new(strategy_cfg);

        let coin_pool = CoinPoolClient::new();

        Ok(Self {
            id: config.id.clone(),
            name: config.name.clone(),
            exchange: config.exchange.clone(),
            ai_model: config.ai_model.clone(),
            user_id,
            config: config.clone(),
            trader: trader.into(),
            mcp_client: mcp_client.into(),
            store: st,
            strategy_engine: Some(strategy_engine).into(),
            is_running: Arc::new(RwLock::new(false)),
            stop_notify: Arc::new(Notify::new()),
            start_time: Utc::now(),
            cycle_number: Arc::new(RwLock::new(cycle_number)),
            call_count: Arc::new(RwLock::new(0)),
            initial_balance: Arc::new(RwLock::new(config.initial_balance)),
            daily_pnl: Arc::new(RwLock::new(0.0)),
            last_reset_time: Arc::new(RwLock::new(Utc::now())),
            stop_until: Arc::new(RwLock::new(None)),
            position_first_seen: Arc::new(RwLock::new(HashMap::new())),
            peak_pnl_cache: Arc::new(RwLock::new(HashMap::new())),
            custom_prompt: Arc::new(RwLock::new(None)),
            override_base_prompt: Arc::new(RwLock::new(false)),
            coin_pool: coin_pool.into(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        {
            let mut running = self.is_running.write().await;
            *running = true;
        }

        info!("🚀 AI-driven automatic trading system started (Aster/Hyperliquid/Binance)");
        info!(
            "💰 Initial balance: {:.2} USDT",
            *self.initial_balance.read().await
        );

        let self_clone = self.clone();

        // Spawn Drawdown Monitor
        tokio::spawn(async move {
            self_clone.run_drawdown_monitor().await;
        });

        let interval_dur = std::time::Duration::from_secs(self.config.scan_interval_sec);
        let mut ticker = interval(interval_dur);

        // First Run
        if let Err(e) = self.run_cycle().await {
            error!("❌ Execution failed: {}", e);
        }

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !*self.is_running.read().await { break; }
                    if let Err(e) = self.run_cycle().await {
                        error!("❌ Execution failed: {}", e);
                    }
                }
                _ = self.stop_notify.notified() => {
                    info!("[{}] ⏹ Stop signal received", self.name);
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn stop(&self) {
        let mut running = self.is_running.write().await;
        if *running {
            *running = false;
            self.stop_notify.notify_waiters();
            info!("⏹ Automatic trading system stopping...");
        }
    }

    async fn run_cycle(&self) -> Result<()> {
        {
            let mut cc = self.call_count.write().await;
            *cc += 1;
            info!(
                "\n{}\n⏰ {} - AI decision cycle #{}",
                "=".repeat(70),
                Utc::now().format("%Y-%m-%d %H:%M:%S"),
                *cc
            );
            info!("{}", "=".repeat(70));
        }

        let mut record = DecisionRecord {
            success: true,
            execution_log: vec![],
            ..Default::default()
        };

        // 1. Risk Control Check
        if let Some(stop_until) = *self.stop_until.read().await {
            if Utc::now() < stop_until {
                let remaining = stop_until.signed_duration_since(Utc::now()).num_minutes();
                info!(
                    "⏸ Risk control: Trading paused, remaining {} minutes",
                    remaining
                );
                return Ok(());
            }
        }

        // 2. Daily PnL Reset
        {
            let mut last_reset = self.last_reset_time.write().await;
            if (Utc::now() - *last_reset).num_hours() >= 24 {
                let mut pnl = self.daily_pnl.write().await;
                *pnl = 0.0;
                *last_reset = Utc::now();
                info!("📅 Daily P&L reset");
            }
        }

        // 3. Build Context
        let mut ctx = match self.build_trading_context().await {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("Failed to build trading context: {}", e);
                record.success = false;
                record.error_message = msg.clone();
                self.save_decision(record).await?;
                return Err(anyhow!(msg));
            }
        };

        self.save_equity_snapshot(&ctx).await;

        info!(
            "📊 Equity: {:.2} | Available: {:.2} | Positions: {}",
            ctx.account.total_equity, ctx.account.available_balance, ctx.account.position_count
        );

        // 4. Request AI Decision
        info!("🤖 Requesting AI analysis...");
        let ai_result = get_full_decision_with_strategy(
            &mut ctx,
            &self.mcp_client,
            self.strategy_engine.as_ref(),
            "balanced",
            &self.coin_pool,
        )
        .await;

        match ai_result {
            Ok(ai_decision) => {
                record.ai_request_duration_ms = ai_decision.ai_request_duration_ms;
                record.system_prompt = ai_decision.system_prompt.clone();
                record.input_prompt = ai_decision.user_prompt.clone();
                record.cot_trace = ai_decision.cot_trace.clone();
                record.decision_json =
                    serde_json::to_string_pretty(&ai_decision.decisions).unwrap_or_default();

                // 5. Execution
                let sorted_decisions = self.sort_decisions_by_priority(ai_decision.decisions);
                info!("🔄 Execution Order: Close first -> Open later");

                for d in &sorted_decisions {
                    info!("  Plan: {} {}", d.symbol, d.action);
                }

                for decision in sorted_decisions {
                    let mut action_record = DecisionAction {
                        action: decision.action.clone(),
                        symbol: decision.symbol.clone(),
                        leverage: decision.leverage.unwrap_or(5),
                        timestamp: Utc::now(),
                        success: false,
                        ..Default::default()
                    };

                    match self.execute_decision(&decision, &mut action_record).await {
                        Ok(_) => {
                            action_record.success = true;
                            record.execution_log.push(format!(
                                "✓ {} {} succeeded",
                                decision.symbol, decision.action
                            ));
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                        Err(e) => {
                            action_record.error = e.to_string();
                            record.execution_log.push(format!(
                                "❌ {} {} failed: {}",
                                decision.symbol, decision.action, e
                            ));
                            error!("❌ Failed to execute: {}", e);
                        }
                    }
                    record.decisions.push(action_record);
                }
            }
            Err(e) => {
                record.success = false;
                record.error_message = e.to_string();
                error!("Failed to get AI decision: {}", e);
            }
        }

        self.save_decision(record).await?;
        Ok(())
    }

    async fn build_trading_context(&self) -> Result<Context> {
        // 1. Account Info
        let balances = self.trader.get_balance().await?;
        let mut total_wallet_balance = 0.0;
        let mut total_unrealized_profit = 0.0;
        let available_balance = 0.0;

        if let Some(val) = balances.get("totalWalletBalance") {
            if val.is_f64() {
                total_wallet_balance = val.as_f64().unwrap();
            }
        };

        if let Some(val) = balances.get("total_unrealized_profit") {
            if val.is_f64() {
                total_unrealized_profit = val.as_f64().unwrap();
            }
        };

        let total_equity = total_wallet_balance + total_unrealized_profit;

        // 2. Positions
        let positions = self.trader.get_positions().await?;
        let mut position_infos = Vec::new();
        let mut total_margin_used = 0.0;
        let mut current_pos_keys = HashSet::new();
        for pos in positions {
            let symbol = pos.get("symbol").unwrap().as_str().unwrap();
            let side = pos.get("side").unwrap().as_str().unwrap();
            let entry_price = pos.get("entryPrice").unwrap().as_f64().unwrap();
            let mark_price = pos.get("markPrice").unwrap().as_f64().unwrap();
            let mut position_amt = pos.get("positionAmt").unwrap().as_f64().unwrap();
            if position_amt < 0.0 {
                position_amt = -position_amt; // Short position quantity is negative, convert to positive
            }

            // Skip closed positions (quantity = 0), prevent "ghost positions" from being passed to AI
            if position_amt == 0.0 {
                continue;
            }

            let unrealized_pnl = pos.get("unRealizedProfit").unwrap().as_f64().unwrap();
            let liquidation_price = pos.get("liquidationPrice").unwrap().as_f64().unwrap();

            let mut leverage = 10.0;
            let lev_val = pos.get("leverage").unwrap();
            if lev_val.is_f64() {
                leverage = lev_val.as_f64().unwrap();
            }

            let margin_used = (position_amt * mark_price) / leverage;
            total_margin_used += margin_used;

            // Calculate P&L percentage (based on margin, considering leverage)
            let pnl_pct = calculate_pn_l_percentage(unrealized_pnl, margin_used);
            // First seen time logic
            let pos_key = format!("{}_{}", symbol, side);
            current_pos_keys.insert(pos_key.clone());

            {
                let mut pfs = self.position_first_seen.write().await;
                pfs.entry(pos_key.clone())
                    .or_insert_with(|| Utc::now().timestamp_millis());
            }
            let update_time = *self.position_first_seen.read().await.get(&pos_key).unwrap();

            // Peak PnL
            let peak_pnl = *self
                .peak_pnl_cache
                .read()
                .await
                .get(&pos_key)
                .unwrap_or(&0.0);

            position_infos.push(PositionInfo {
                symbol: symbol.to_string(),
                side: side.to_string(),
                entry_price: entry_price,
                mark_price: mark_price,
                quantity: position_amt,
                leverage: leverage as i32,
                unrealized_pnl: unrealized_pnl,
                unrealized_pnl_pct: pnl_pct,
                peak_pnl_pct: peak_pnl,
                liquidation_price: liquidation_price,
                margin_used,
                update_time,
            });
        }

        // Cleanup closed positions from cache
        {
            let mut pfs = self.position_first_seen.write().await;
            pfs.retain(|k, _| current_pos_keys.contains(k));
        }

        let engine = (*self.strategy_engine).as_ref().unwrap();
        // 3. Candidates
        let candidate_coins = engine.get_candidate_coins().await?;

        // 4. Calculations
        let init_bal = *self.initial_balance.read().await;
        let total_pnl = total_equity - init_bal;
        let total_pnl_pct = if init_bal > 0.0 {
            (total_pnl / init_bal) * 100.0
        } else {
            0.0
        };
        let margin_used_pct = if total_equity > 0.0 {
            (total_margin_used / total_equity) * 100.0
        } else {
            0.0
        };

        // 5. Configs
        let strat_config = engine.get_config();

        let mut ctx = Context {
            current_time: Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            runtime_minutes: (Utc::now() - self.start_time).num_minutes() as i32,
            call_count: *self.call_count.read().await,
            btc_eth_leverage: strat_config.risk_control.btc_eth_max_leverage,
            altcoin_leverage: strat_config.risk_control.altcoin_max_leverage,
            account: AccountInfo {
                total_equity,
                available_balance,
                unrealized_pnl: 0.0,
                total_pnl,
                total_pnl_pct,
                margin_used: total_margin_used,
                margin_used_pct,
                position_count: position_infos.len(),
            },
            positions: position_infos,
            candidate_coins,
            trading_stats: None,
            recent_orders: vec![],
            quant_data_map: None,
            prompt_variant: None,
            market_data_map: HashMap::new(),
            multi_tf_market: HashMap::new(),
            oi_top_data_map: None,
        };

        // Add history/stats if store available
        if let Some(store) = (*self.store).as_ref() {
            // Implementation omitted for brevity (similar to Go: fetch stats/trades)
            let stats = store.position().get_full_stats(&self.id).await?;
            ctx.trading_stats = Some(TradingStats {
                total_trades: stats.total_trades,
                win_rate: stats.win_rate,
                profit_factor: stats.profit_factor,
                sharpe_ratio: stats.sharpe_ratio,
                total_pnl: stats.total_pnl,
                avg_win: stats.avg_win,
                avg_loss: stats.avg_loss,
                max_drawdown_pct: stats.max_drawdown_pct,
            });

            let recent_trades = store.position().get_recent_trades(&self.id, 10).await?;
            for trader in recent_trades {
                ctx.recent_orders.push(RecentOrder {
                    symbol: trader.symbol,
                    side: trader.side,
                    entry_price: trader.entry_price,
                    exit_price: trader.exit_price,
                    realized_pnl: trader.realized_pnl,
                    pnl_pct: trader.pnl_pct,
                    filled_at: trader.exit_time,
                });
            }
        }

        // Fetch Quant Data if enabled
        if strat_config.indicators.enable_quant_data {
            let mut symbols: Vec<String> = ctx
                .candidate_coins
                .iter()
                .map(|c| c.symbol.clone())
                .collect();
            for p in &ctx.positions {
                symbols.push(p.symbol.clone());
            }
            // Deduplicate logic here...
            ctx.quant_data_map = Some(engine.fetch_quant_data_batch(&symbols).await);
        }

        Ok(ctx)
    }

    async fn execute_decision(
        &self,
        decision: &Decision,
        record: &mut DecisionAction,
    ) -> Result<()> {
        match decision.action.as_str() {
            "open_long" => self.execute_open_long(decision, record).await,
            "open_short" => self.execute_open_short(decision, record).await,
            "close_long" => self.execute_close_long(decision, record).await,
            "close_short" => self.execute_close_short(decision, record).await,
            "hold" | "wait" => Ok(()),
            _ => Err(anyhow!("Unknown action: {}", decision.action)),
        }
    }

    async fn execute_open_long(
        &self,
        decision: &Decision,
        record: &mut DecisionAction,
    ) -> Result<()> {
        info!("  📈 Open Long: {}", decision.symbol);

        // Check Existing Position to prevent stacking
        let positions = self.trader.get_positions().await?;
        for pos in positions {
            if pos.get("symbol").unwrap().as_str().unwrap() == decision.symbol
                && pos.get("side").unwrap().as_str().unwrap().to_uppercase() == "LONG"
            {
                return Err(anyhow!("{} already has LONG position", decision.symbol));
            }
        }

        // Market Data
        let market_data = get(&decision.symbol).await?;

        // Quantity Calc
        let quantity = decision.position_size_usd.unwrap() / market_data.current_price;
        record.quantity = quantity;
        record.price = market_data.current_price;

        // Balance Check
        let balance = self.trader.get_balance().await?;
        let required_margin =
            decision.position_size_usd.unwrap() / (decision.leverage.unwrap() as f64);
        let estimated_fee = decision.position_size_usd.unwrap() * 0.0004;

        let mut available_balance = 0.0;
        let available_balance_val = balance.get("availableBalance").unwrap();
        if available_balance_val.is_f64() {
            available_balance = available_balance_val.as_f64().unwrap();
        }

        if (required_margin + estimated_fee) > available_balance {
            return Err(anyhow!(
                "Insufficient margin. Req: {:.2}, Avail: {:.2}",
                required_margin + estimated_fee,
                available_balance
            ));
        }

        // Execute
        let _ = self
            .trader
            .set_margin_mode(&decision.symbol, self.config.is_cross_margin)
            .await;

        let order = self
            .trader
            .open_long(&decision.symbol, quantity, decision.leverage.unwrap())
            .await?;

        let order_id_val = order.get("orderId").unwrap();
        let mut order_id = 0_i64;
        if order_id_val.is_i64() {
            order_id = order_id_val.as_i64().unwrap();
            record.order_id = order_id;
        }

        info!("  ✓ Position opened. ID: {}", record.order_id);

        // Update Cache
        let pos_key = format!("{}_long", decision.symbol);
        self.position_first_seen
            .write()
            .await
            .insert(pos_key, Utc::now().timestamp_millis());

        // SL/TP
        let _ = self
            .trader
            .set_stop_loss(
                &decision.symbol,
                "LONG",
                quantity,
                decision.stop_loss.unwrap(),
            )
            .await;
        let _ = self
            .trader
            .set_take_profit(
                &decision.symbol,
                "LONG",
                quantity,
                decision.take_profit.unwrap(),
            )
            .await;

        // Record to DB
        self.record_and_confirm_order(
            format!("{}", order_id),
            &decision.symbol,
            "open_long",
            quantity,
            market_data.current_price,
            decision.leverage.unwrap(),
            0.0,
        )
        .await;

        Ok(())
    }

    async fn execute_open_short(
        &self,
        decision: &Decision,
        record: &mut DecisionAction,
    ) -> Result<()> {
        info!("  📉 Open Short: {}", decision.symbol);

        // Check Existing Position to prevent stacking
        let positions = self.trader.get_positions().await?;
        for pos in positions {
            if pos.get("symbol").unwrap().as_str().unwrap() == decision.symbol
                && pos.get("side").unwrap().as_str().unwrap().to_uppercase() == "SHORT"
            {
                return Err(anyhow!("{} already has SHORT position", decision.symbol));
            }
        }

        // Market Data
        let market_date = get(&decision.symbol).await?;

        // Quantity Calc
        let quantity = decision.position_size_usd.unwrap() / market_date.current_price;
        record.quantity = quantity;
        record.price = market_date.current_price;

        // Balance Check
        let balance = self.trader.get_balance().await?;
        let required_margin =
            decision.position_size_usd.unwrap() / (decision.leverage.unwrap() as f64);
        let estimated_fee = decision.position_size_usd.unwrap() * 0.0004;

        let mut available_balance = 0.0;
        let available_balance_val = balance.get("availableBalance").unwrap();
        if available_balance_val.is_f64() {
            available_balance = available_balance_val.as_f64().unwrap();
        }

        if (required_margin + estimated_fee) > available_balance {
            return Err(anyhow!(
                "Insufficient margin. Req: {:.2}, Avail: {:.2}",
                required_margin + estimated_fee,
                available_balance
            ));
        }

        if (required_margin + estimated_fee) > available_balance {
            return Err(anyhow!(
                "Insufficient margin. Req: {:.2}, Avail: {:.2}",
                required_margin + estimated_fee,
                available_balance
            ));
        }

        // Execute
        let _ = self
            .trader
            .set_margin_mode(&decision.symbol, self.config.is_cross_margin)
            .await;

        let order = self
            .trader
            .open_short(&decision.symbol, quantity, decision.leverage.unwrap())
            .await?;

        let order_id_val = order.get("orderId").unwrap();
        let mut order_id = 0_i64;
        if order_id_val.is_i64() {
            order_id = order_id_val.as_i64().unwrap();
            record.order_id = order_id;
        }

        info!("  ✓ Position opened. ID: {}", order_id);

        // Update Cache
        let pos_key = format!("{}_short", decision.symbol);
        self.position_first_seen
            .write()
            .await
            .insert(pos_key, Utc::now().timestamp_millis());

        // SL/TP
        let _ = self
            .trader
            .set_stop_loss(
                &decision.symbol,
                "SHORT",
                quantity,
                decision.stop_loss.unwrap(),
            )
            .await;
        let _ = self
            .trader
            .set_take_profit(
                &decision.symbol,
                "SHORT",
                quantity,
                decision.take_profit.unwrap(),
            )
            .await;

        self.record_and_confirm_order(
            format!("{}", order_id),
            &decision.symbol,
            "open_short",
            quantity,
            market_date.current_price,
            decision.leverage.unwrap(),
            0.0,
        )
        .await;
        Ok(())
    }

    async fn execute_close_long(
        &self,
        decision: &Decision,
        record: &mut DecisionAction,
    ) -> Result<()> {
        info!("  🔄 Close Long: {}", decision.symbol);

        let market_date = get(&decision.symbol).await?;
        record.price = market_date.current_price;

        // Get entry price (for P&L calculation)
        let mut entry_price = 0.0;
        let mut quantity = 0.0;
        if let Some(store) = (*self.store).as_ref() {
            let open_order = store
                .order()
                .get_latest_open_order(&self.id, &decision.symbol, "long")
                .await?;
            entry_price = open_order.avg_price;
            quantity = open_order.executed_qty;
        }

        let order = self.trader.close_long(&decision.symbol, 0.0).await?; // 0.0 = close all 
        let order_id = order.get("orderId").unwrap().as_i64().unwrap();
        record.order_id = order_id;

        // Fetch entry price logic from store omitted... default 0.0
        self.record_and_confirm_order(
            format!("{}", order_id),
            &decision.symbol,
            "close_long",
            quantity,
            market_date.current_price,
            0,
            entry_price,
        )
        .await;

        info!("  ✓ Position closed successfully");
        Ok(())
    }

    async fn execute_close_short(
        &self,
        decision: &Decision,
        record: &mut DecisionAction,
    ) -> Result<()> {
        info!("  🔄 Close Short: {}", decision.symbol);

        // Get current price
        let market_data = get(&decision.symbol).await?;
        record.price = market_data.current_price;

        // Get entry price (for P&L calculation)
        let mut entry_price = 0.0;
        let mut quantity = 0.0;
        if let Some(store) = (*self.store).as_ref() {
            let open_order = store
                .order()
                .get_latest_open_order(&self.id, &decision.symbol, "long")
                .await?;
            entry_price = open_order.avg_price;
            quantity = open_order.executed_qty;
        }

        let order = self.trader.close_long(&decision.symbol, 0.0).await?; // 0.0 = close all 
        let order_id = order.get("orderId").unwrap().as_i64().unwrap();
        record.order_id = order_id;

        self.record_and_confirm_order(
            format!("{}", order_id),
            &decision.symbol,
            "close_short",
            quantity,
            market_data.current_price,
            0,
            entry_price,
        )
        .await;

        info!("  ✓ Position closed successfully");
        Ok(())
    }

    pub async fn get_account_info(&self) -> Result<Map<String, Value>> {
        let balance = self
            .trader
            .get_balance()
            .await
            .map_err(|e| anyhow!("failed to get balance: {}", e))?;

        let update = |key: &str, target: &mut f64, values: &Map<String, Value>| {
            if let Some(val) = values.get(key).and_then(|v| v.as_f64()) {
                *target = val;
            }
        };

        let mut total_wallet_balance = 0.0;
        let mut total_unrealized_profit = 0.0;
        let mut available_balance = 0.0;
        let mut total_margin_used = 0.0;
        let mut total_unrealized_pnL_calculated = 0.0;
        let mut mark_price = 0.0;
        let mut quantity = 0.0;
        let mut unrealized_pnl = 0.0;

        update("totalWalletBalance", &mut total_wallet_balance, &balance);
        update("totalWalletBalance", &mut total_wallet_balance, &balance);
        update(
            "totalUnrealizedProfit",
            &mut total_unrealized_profit,
            &balance,
        );
        update("availableBalance", &mut available_balance, &balance);

        let total_equity = total_wallet_balance + total_unrealized_profit;

        let positions = self
            .trader
            .get_positions()
            .await
            .map_err(|e| anyhow!("failed to get positions: {}", e))?;
        for pos in &positions {
            update("markPrice", &mut mark_price, &pos);
            update("positionAmt", &mut quantity, &pos);

            if quantity < 0.0 {
                quantity = -quantity;
            }

            update("unRealizedProfit", &mut unrealized_pnl, &pos);

            total_unrealized_pnL_calculated += unrealized_pnl;

            let mut leverage = 10.0;
            update("leverage", &mut leverage, &pos);

            let margin_used = quantity * mark_price / leverage;
            total_margin_used += margin_used;
        }

        // Verify unrealized P&L consistency (API value vs calculated from positions)
        let diff = total_unrealized_profit - total_unrealized_pnL_calculated;
        if diff.abs() > 0.1 {
            info!(
                "⚠️ Unrealized P&L inconsistency: API={:.4}, Calculated={:.4}, Diff={:.4}",
                total_unrealized_profit,
                total_unrealized_pnL_calculated,
                diff.abs()
            );
        }
        let initial_balance = { self.initial_balance.read().await.clone() };
        let total_pnl = total_equity - initial_balance;
        let mut total_pn_l_pct = 0.0;
        if initial_balance > 0.0 {
            total_pn_l_pct = total_pnl / initial_balance * 100.0;
        } else {
            info!(
                "⚠️ Initial Balance abnormal: {:.2}, cannot calculate P&L percentage",
                initial_balance
            )
        }

        let mut margin_used_pct = 0.0;
        if total_equity > 0.0 {
            margin_used_pct = total_margin_used / total_equity * 100.0;
        }

        let daily_pnl = { self.daily_pnl.read().await.clone() };

        let value: Value = json!({
            // Core fields
            "total_equity":      total_equity,
            "wallet_balance":    total_wallet_balance,
            "unrealized_profit": total_unrealized_profit,
            "available_balance": available_balance,

            // P&L statistics
            "total_pnl":       total_pnl,
            "total_pnl_pct":   total_pn_l_pct,
            "initial_balance": initial_balance,
            "daily_pnl":       daily_pnl,

            // Position information
            "position_count":  positions.len(),
            "margin_used":     total_margin_used,
            "margin_used_pct": margin_used_pct,
        });

        let account_info: Map<String, Value> = value.as_object().unwrap().clone();

        Ok(account_info)
    }

    pub async fn get_status(&self) -> Result<Map<String, Value>> {
        let mut ai_provider = "DeepSeek";
        if self.config.use_qwen {
            ai_provider = "Qwen";
        }

        let is_running = { self.is_running.read().await.clone() };
        let call_count = { self.call_count.read().await.clone() };
        let initial_balance = { self.initial_balance.read().await.clone() };
        let stop_until = { self.stop_until.read().await.clone().unwrap_or_default() };
        let last_reset_time = { self.last_reset_time.read().await.clone() };

        let value: Value = json!({
            "trader_id":       self.id,
            "trader_name":     self.name,
            "ai_model":        self.ai_model,
            "exchange":        self.exchange,
            "is_running":      is_running,
            "start_time":      self.start_time.to_rfc3339_opts(SecondsFormat::Secs, true),
            "runtime_minutes": (Utc::now() - self.start_time).num_minutes(),
            "call_count":      call_count,
            "initial_balance": initial_balance,
            "scan_interval":   self.config.scan_interval_sec,
            "stop_until":      stop_until.to_rfc3339_opts(SecondsFormat::Secs, true),
            "last_reset_time": last_reset_time.to_rfc3339_opts(SecondsFormat::Secs, true),
            "ai_provider":     ai_provider,
        });

        let status: Map<String, Value> = value.as_object().unwrap().clone();

        Ok(status)
    }

    pub fn get_system_prompt_template(&self) -> String {
        if (*self.strategy_engine).is_some() {
            let engine = (*self.strategy_engine).as_ref().unwrap();
            let config = engine.get_config();
            if !config.custom_prompt.is_empty() {
                return "custom".to_string();
            }
        }
        "strategy".to_string()
    }

    async fn run_drawdown_monitor(&self) {
        let mut ticker = interval(std::time::Duration::from_secs(60));
        info!("📊 Started position drawdown monitoring");

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.check_position_drawdown().await;
                }
                _ = self.stop_notify.notified() => {
                    info!("⏹ Stopped drawdown monitoring");
                    break;
                }
            }
        }
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_ai_model(&self) -> String {
        self.ai_model.clone()
    }

    pub fn get_exchange(&self) -> String {
        self.exchange.clone()
    }

    async fn check_position_drawdown(&self) {
        // Get current positions
        let positions = match self.trader.get_positions().await {
            Ok(p) => p,
            Err(e) => {
                warn!("❌ Drawdown Monitor: failed to get positions: {}", e);
                return;
            }
        };

        for pos in positions {
            let symbol = pos
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            let side = pos
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            let entry_price = pos
                .get("entryPrice")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            let leverage = pos.get("leverage").and_then(|v| v.as_f64()).unwrap_or(5.0);

            let mark_price = pos.get("markPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);

            let mut quantity = pos
                .get("positionAmt")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            if quantity < 0.0 {
                quantity = -quantity // Short position quantity is negative, convert to positive
            }

            // PnL Pct Calculation
            let current_pnl_pct = if side.to_uppercase() == "LONG" {
                ((mark_price - entry_price) / entry_price) * leverage * 100.0
            } else {
                ((entry_price - mark_price) / entry_price) * leverage * 100.0
            };

            let pos_key = format!("{}_{}", symbol, side);

            // Update Peak
            let mut peak_map = self.peak_pnl_cache.write().await;
            let peak_pnl = peak_map.entry(pos_key.clone()).or_insert(current_pnl_pct);

            if current_pnl_pct > *peak_pnl {
                *peak_pnl = current_pnl_pct;
            }
            let cached_peak = *peak_pnl;

            // Calc Drawdown
            let mut drawdown_pct = 0.0;
            if cached_peak > 0.0 && current_pnl_pct < cached_peak {
                drawdown_pct = ((cached_peak - current_pnl_pct) / cached_peak) * 100.0;
            }

            // Trigger Logic
            if current_pnl_pct > 5.0 && drawdown_pct >= 40.0 {
                info!(
                    "🚨 Drawdown Trigger: {} {} | PnL: {:.2}% | Peak: {:.2}% | DD: {:.2}%",
                    symbol, side, current_pnl_pct, cached_peak, drawdown_pct
                );

                // Emergency Close
                let res = if side.to_uppercase() == "LONG" {
                    self.trader.close_long(symbol, 0.0).await
                } else {
                    self.trader.close_short(symbol, 0.0).await
                };

                match res {
                    Ok(_) => {
                        info!("✅ Emergency close success: {}", symbol);
                        peak_map.remove(&pos_key);
                    }
                    Err(e) => error!("❌ Emergency close failed: {}", e),
                }
            }
        }
    }

    fn sort_decisions_by_priority(&self, mut decisions: Vec<Decision>) -> Vec<Decision> {
        decisions.sort_by_key(|d| match d.action.as_str() {
            "close_long" | "close_short" => 1,
            "open_long" | "open_short" => 2,
            _ => 3,
        });
        decisions
    }

    async fn save_equity_snapshot(&self, ctx: &Context) {
        if let Some(store) = (*self.store).as_ref() {
            let mut snapshot = EquitySnapshot {
                id: 0,
                trader_id: self.id.clone(),
                timestamp: Utc::now(),
                total_equity: ctx.account.total_equity,
                balance: ctx.account.total_equity - ctx.account.unrealized_pnl,
                unrealized_pnl: ctx.account.unrealized_pnl,
                position_count: ctx.account.position_count as i32,
                margin_used_pct: ctx.account.margin_used_pct,
            };
            if let Err(e) = store.equity().save(&mut snapshot).await {
                warn!("⚠️ Failed to save equity snapshot: {}", e);
            }
        }
    }

    async fn save_decision(&self, mut record: DecisionRecord) -> Result<()> {
        if let Some(store) = (*self.store).as_ref() {
            {
                let mut cn = self.cycle_number.write().await;
                *cn += 1;
                record.cycle_number = *cn;
            }
            record.trader_id = self.id.clone();
            record.timestamp = Utc::now();

            store.decision().log_decision(&mut record).await?;
            info!("📝 Decision record saved: cycle={}", record.cycle_number);
        }

        Ok(())
    }

    async fn record_and_confirm_order(
        &self,
        order_id: String,
        symbol: &str,
        action: &str,
        qty: f64,
        price: f64,
        leverage: i32,
        entry_price: f64,
    ) {
        if let Some(store) = (*self.store).as_ref() {
            let (side, pos_side) = match action {
                "open_long" => ("BUY", "LONG"),
                "close_long" => ("SELL", "LONG"),
                "open_short" => ("SELL", "SHORT"),
                "close_short" => ("BUY", "SHORT"),
                _ => ("UNKNOWN", "UNKNOWN"),
            };

            let mut order = TraderOrder {
                trader_id: self.id.clone(),
                order_id: order_id.to_string(),
                symbol: symbol.to_string(),
                side: side.to_string(),
                position_side: pos_side.to_string(),
                action: action.to_string(),
                quantity: qty,
                price,
                leverage,
                status: "NEW".to_string(),
                entry_price,
                ..Default::default()
            };

            if let Err(e) = store.order().create(&mut order).await {
                warn!("⚠️ Failed to record order: {}", e);
            }

            info!(
                "  📝 Order recorded (ID: {}, action: {})",
                order_id,
                action.to_string()
            );
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub async fn set_custom_prompt(&self, prompt: &str) {
        *self.custom_prompt.write().await = Some(prompt.to_string());
    }

    pub async fn set_override_base_prompt(&self, ov: bool) {
        *self.override_base_prompt.write().await = ov;
    }

    pub async fn get_positions(&self) -> Result<Vec<PositionResponse>> {
        // 获取 positions，类型推断为 Vec<Map<String, Value>>
        let positions: Vec<Map<String, Value>> = self
            .trader
            .get_positions()
            .await
            .map_err(|e| anyhow!("failed to get positions: {:?}", e))?;

        let mut result = Vec::new();

        for pos in &positions {
            let symbol = pos
                .get("symbol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let side = pos
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let entry_price = pos
                .get("entryPrice")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            let mark_price = pos.get("markPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);

            // Go逻辑: quantity = abs(positionAmt)
            let raw_qty = pos
                .get("positionAmt")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let quantity = raw_qty.abs();

            let unrealized_pnl = pos
                .get("unRealizedProfit")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            let liquidation_price = pos
                .get("liquidationPrice")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            // Go逻辑: leverage 默认为 10
            let leverage = pos
                .get("leverage")
                .and_then(|v| v.as_f64())
                .map(|v| v as i32)
                .unwrap_or(10);

            // 计算 Margin Used
            // Go: (quantity * markPrice) / float64(leverage)
            let margin_used = if leverage != 0 {
                (quantity * mark_price) / leverage as f64
            } else {
                0.0
            };

            // 计算 P&L Percentage
            let pnl_pct = calculate_pn_l_percentage(unrealized_pnl, margin_used);

            result.push(PositionResponse {
                symbol,
                side,
                entry_price,
                mark_price,
                quantity,
                leverage,
                unrealized_pnl,
                unrealized_pnl_pct: pnl_pct,
                liquidation_price,
                margin_used,
            });
        }

        Ok(result)
    }
}

fn get_values(keys: &[&str], vals: &Map<String, Value>) -> Vec<Value> {
    let mut found_vals = Vec::new();
    for key in keys {
        if let Some(val) = vals.get(*key) {
            found_vals.push(val.clone());
        }
    }

    found_vals
}

fn calculate_pn_l_percentage(unrealized_pnl: f64, margin_used: f64) -> f64 {
    if margin_used > 0.0 {
        return (unrealized_pnl / margin_used) * 100.0;
    }
    return 0.0;
}
