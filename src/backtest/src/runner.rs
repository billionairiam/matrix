use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::account::BacktestAccount;
use crate::account::Position;
use crate::account::calculate_unrealized_pnl;
use crate::ai_client::configure_mcp_client;
use crate::aicache::AICache;
use crate::config::BacktestConfig;
use crate::config::FillPolicy;
use crate::datafeed::DataFeed;
use crate::lock::RunLockInfo;
use crate::lock::acquire_run_lock;
use crate::lock::update_run_lock_heartbeat;
use crate::lock::{LOCK_HEARTBEAT_INTERVAL, delete_run_lock};
use crate::metrics::calculate_metrics;
use crate::registry::update_run_index;
use crate::storage::{decision_log_dir, load_checkpoint, persist_metrics};
use crate::storage::{ensure_run_dir, save_checkpoint};
use crate::storage::{persist_decision_record, save_run_metadata};
use crate::types::BacktestState;
use crate::types::Checkpoint;
use crate::types::RunMetadata;
use crate::types::RunState;
use crate::types::RunSummary;
use crate::types::StatusPayload;
use crate::types::TradeEvent;
use anyhow::{Result, anyhow};
use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use decision::engine::AccountInfo;
use decision::engine::CandidateCoin;
use decision::engine::Context;
use decision::engine::Decision;
use decision::engine::FullDecision;
use decision::engine::PositionInfo;
use decision::engine::get_full_decision_with_custom_prompt;
use market::types::Data;
use mcp::Provider;
use mcp::client::Client;
use pool::coin_pool::CoinPoolClient;
use store::decision::AccountSnapshot;
use store::decision::DecisionAction;
use store::decision::DecisionRecord;
use store::decision::PositionSnapshot;
use thiserror::Error;
use tokio::sync::{Mutex, Notify, RwLock, oneshot};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::info;

const METRICS_WRITE_INTERVAL: TimeDelta = TimeDelta::try_seconds(5).unwrap();
const AI_DECISION_MAX_RETRIES: usize = 3;

#[derive(Debug, Error)]
pub enum BacktestError {
    #[error("Backtest completed successfully")]
    Completed,

    #[error("Account liquidated")]
    Liquidated,

    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

#[derive(Default)]
pub struct ErrorState {
    pub last_error: Option<String>,
}

#[derive(Default)]
pub struct TimeMetrics {
    pub last_checkpoint: chrono::DateTime<Utc>,
    pub created_at: chrono::DateTime<Utc>,
    pub last_metrics_write: chrono::DateTime<Utc>,
}

#[derive(Default)]
pub struct LockControl {
    pub info: Option<RunLockInfo>,
    pub stop_rx: Option<oneshot::Receiver<()>>,
    pub stop_tx: Option<oneshot::Sender<()>>,
}

pub struct Runner {
    pub cfg: BacktestConfig,
    pub feed: Arc<DataFeed>,
    pub decision_log_dir: PathBuf,
    pub cache_path: PathBuf,
    pub mcp_client: Arc<Client>,
    pub oi_client: Arc<CoinPoolClient>,
    pub ai_cache: Option<Arc<AICache>>,
    pub account: Arc<RwLock<BacktestAccount>>,
    pub status: RwLock<RunState>,
    pub state: RwLock<BacktestState>,

    pub pause_notify: Arc<Notify>,
    pub resume_notify: Arc<Notify>,
    pub stop_notify: Arc<Notify>,
    pub done_notify: Arc<Notify>,

    pub error_state: RwLock<ErrorState>,
    pub metrics: RwLock<TimeMetrics>,
    pub lock_control: Mutex<LockControl>,
}

impl Runner {
    pub async fn new(cfg: BacktestConfig, mcp_client: Arc<dyn Provider>) -> Result<Arc<Self>> {
        ensure_run_dir(&cfg.run_id).await?;

        let coin_pool = CoinPoolClient::new();
        let client = configure_mcp_client(&cfg, mcp_client)?;

        let feed = DataFeed::new(cfg.clone()).await?;

        let d_log_dir = decision_log_dir(&cfg.run_id);
        std::fs::create_dir_all(&d_log_dir)?;

        let account = BacktestAccount::new(cfg.initial_balance, cfg.fee_bps, cfg.slippage_bps);

        let created_at = Utc::now();
        let state = BacktestState {
            positions: HashMap::new(),
            cash: account.cash(),
            equity: cfg.initial_balance,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            max_equity: cfg.initial_balance,
            min_equity: cfg.initial_balance,
            max_drawdown_pct: 0.0,
            last_update: created_at,
            bar_index: 0,
            decision_cycle: 0,
            bar_timestamp: 0,
            liquidated: false,
            liquidation_note: String::new(),
        };

        let mut ai_cache = AICache::default();
        let mut cache_path = String::new();

        if cfg.cache_ai || cfg.replay_only || !cfg.shared_ai_cache_path.is_empty() {
            cache_path = cfg.shared_ai_cache_path.clone();
            let cache = AICache::load(&cache_path)?;
            ai_cache = cache;
        }

        let runner = Runner {
            cfg: cfg,
            feed: feed.into(),
            decision_log_dir: d_log_dir,
            cache_path: cache_path.into(),
            mcp_client: client.into(),
            ai_cache: Some(Arc::new(ai_cache)),
            account: RwLock::new(account).into(),
            status: RwLock::new(RunState::Created),
            state: RwLock::new(state),
            pause_notify: Notify::new().into(),
            resume_notify: Notify::new().into(),
            stop_notify: Notify::new().into(),
            done_notify: Notify::new().into(),
            error_state: RwLock::new(ErrorState::default()),
            metrics: RwLock::new(TimeMetrics::default()),
            lock_control: Mutex::new(LockControl::default()),
            oi_client: coin_pool.into(),
        };

        Ok(Arc::new(runner))
    }

    pub async fn current_metadata(&self) -> Result<RunMetadata> {
        let state = self.snapshot_state().await;
        let mut run_state = { self.status.read().await.clone() };
        let mut meta = self.build_metadata(&state, &mut run_state).await?;

        meta.created_at = self.metrics.read().await.created_at.clone();
        meta.updated_at = state.last_update;

        Ok(meta)
    }

    pub async fn restore_from_checkpoint(&self) -> Result<()> {
        let ckpt = load_checkpoint(&self.cfg.run_id).await?;
        self.apply_checkpoint(&ckpt).await
    }

    async fn apply_checkpoint(&self, ckpt: &Checkpoint) -> Result<()> {
        {
            self.account.write().await.restore_from_snapshots(
                ckpt.cash,
                ckpt.realized_pnl,
                ckpt.positions.as_ref(),
            );
        }

        {
            let mut state = self.state.write().await;
            state.bar_index = ckpt.bar_index;
            state.bar_timestamp = ckpt.bar_timestamp;
            state.cash = ckpt.cash;
            state.equity = ckpt.cash;
            state.unrealized_pnl = ckpt.unrealized_pnl;
            state.realized_pnl = ckpt.realized_pnl;
            state.decision_cycle = ckpt.decision_cycle;
            state.liquidated = ckpt.liquidated;
            state.liquidation_note = ckpt.liquidation_note.clone().unwrap();
            state.max_equity = ckpt.max_equity;
            state.min_equity = ckpt.min_equity;
            state.max_drawdown_pct = ckpt.max_drawdown_pct;
            state.positions = self.snapshots_to_map(ckpt.positions.as_ref());
            state.last_update = Utc::now();
        }

        self.metrics.write().await.last_checkpoint = Utc::now();

        Ok(())
    }

    fn snapshots_to_map(
        &self,
        snaps: &Vec<crate::types::PositionSnapshot>,
    ) -> HashMap<String, crate::types::PositionSnapshot> {
        let mut positions = HashMap::new();
        for snap in snaps {
            let key = format!("{}:{}", snap.symbol, snap.side);
            positions.insert(key, snap.clone());
        }

        positions
    }

    async fn init_lock(self: Arc<Self>) -> Result<()> {
        if self.cfg.run_id.is_empty() {
            return Err(anyhow!("run_id required for lock"));
        }

        let info = acquire_run_lock(self.cfg.run_id.as_str()).await?;
        let (tx, rx) = oneshot::channel();

        {
            let mut lock = self.lock_control.lock().await;
            lock.info = Some(info);
            lock.stop_rx = Some(rx);
            lock.stop_tx = Some(tx);
        }

        let self_clone = self.clone();
        tokio::spawn(async move {
            let _ = self_clone.lock_heartbeat_loop().await;
        });

        Ok(())
    }

    async fn lock_heartbeat_loop(self: Arc<Self>) -> Result<()> {
        let mut ticker = interval(LOCK_HEARTBEAT_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut stop_rx = {
            let mut control = self.lock_control.lock().await;
            control
                .stop_rx
                .take()
                .ok_or_else(|| anyhow!("stop_rx was not initialized"))?
        };

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let mut lock_control_guard = self.lock_control.lock().await;
                    if let Some(info) = lock_control_guard.info.as_mut() {

                        if let Err(err) = update_run_lock_heartbeat(info).await {
                            info!("failed to update lock heartbeat for {}: {:?}", self.cfg.run_id, err);
                        }
                    } else {
                        info!("Lock info is None, skipping heartbeat update for run_id: {}", self.cfg.run_id);
                    }
                }

                _ = &mut stop_rx => {
                    info!("Lock heartbeat loop stopped");
                    ()
                }
            }
        }
    }

    async fn release_lock(self: Arc<Self>) -> Result<()> {
        {
            let mut lock_control_guard = self.lock_control.lock().await;
            let stop_tx = lock_control_guard
                .stop_tx
                .take()
                .ok_or_else(|| anyhow!("stop_rx was not initialized"))?;
            let _ = stop_tx.send(());
        }

        delete_run_lock(&self.cfg.run_id)?;
        Ok(())
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        {
            let mut status = self.status.write().await;
            if *status != RunState::Created && *status != RunState::Paused {
                return Err(anyhow!("cannot start runner in state {:?}", *status));
            }
            *status = RunState::Running;
        }

        tokio::spawn(async move {
            let _ = self.run_loop().await;
        });

        Ok(())
    }

    async fn run_loop(self: Arc<Self>) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.stop_notify.notified() => {
                    self.handle_stop(None).await?;
                    return Ok(());
                }
                _ = self.pause_notify.notified() => {
                    self.handle_pause().await?;
                    let _ = self.resume_notify.notify_waiters();
                    self.resume_from_pause().await;
                }
                else => {}
            }

            match self.step_once().await {
                Ok(_) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => match e {
                    BacktestError::Completed => {
                        self.handle_completion().await?;
                        return Ok(());
                    }
                    BacktestError::Liquidated => {
                        self.handle_liquidation().await?;
                        return Ok(());
                    }
                    BacktestError::Unexpected(err) => {
                        self.handle_failure(err).await?;
                        return Ok(());
                    }
                },
            }
        }
    }

    async fn step_once(&self) -> Result<(), BacktestError> {
        let state_snap = self.snapshot_state().await;

        if state_snap.bar_index >= self.feed.decision_bar_count() {
            return Err(BacktestError::Completed);
        }

        let ts = self.feed.decision_timestamp(state_snap.bar_index);
        let (market_data, multi_tf) = self.feed.build_market_data(ts)?;

        let mut price_map = HashMap::new();
        for (sym, data) in &market_data {
            price_map.insert(sym.clone(), data.current_price);
        }

        let call_count = state_snap.decision_cycle + 1;
        let should_decide = self.should_trigger_decision(state_snap.bar_index);

        let mut decision_record = None;
        let mut trade_events = Vec::new();
        let mut exec_log = Vec::new();
        let mut had_error = false;
        let mut decision_attempted = should_decide;

        if should_decide {
            let (mut ctx, mut rec) = self
                .build_decision_context(ts, &market_data, multi_tf, &price_map, call_count)
                .await?;

            let mut full_decision = None;
            let mut cache_key = String::new();
            let mut from_cache = false;

            if let Some(cache) = &self.ai_cache {
                cache_key = format!("dec:{}:{}", self.cfg.prompt_variant, ts); // simplified key gen
                if let Some(cached) = cache.get(&cache_key) {
                    full_decision = Some(cached);
                    from_cache = true;
                } else if self.cfg.replay_only {
                    let err = anyhow!("cached decision not found for ts={}", ts);
                    rec.success = false;
                    rec.error_message = err.to_string();
                    self.log_decision(&rec).await?;
                    return Err(err.into());
                }
            }

            if !from_cache {
                match self.invoke_ai_with_retry(&mut ctx).await {
                    Ok(fd) => {
                        full_decision = Some(fd.clone());
                        if self.cfg.cache_ai {
                            if let Some(cache) = &self.ai_cache {
                                let _ = cache.put(
                                    cache_key.as_str(),
                                    self.cfg.prompt_variant.as_str(),
                                    ts,
                                    &fd,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        decision_attempted = true;
                        had_error = true;
                        rec.success = false;
                        rec.error_message = format!("AI decision failed: {}", e);
                        exec_log.push(format!("⚠️ AI decision failed: {}", e));
                        self.set_last_error(Some(e.to_string())).await;
                    }
                }
            }

            if let Some(fd) = full_decision {
                rec.input_prompt = fd.user_prompt.clone();
                rec.cot_trace = fd.cot_trace.clone();
                if let Ok(json) = serde_json::to_string_pretty(&fd.decisions) {
                    rec.decision_json = json;
                }

                // Sort decisions... (omitted, assuming functionality)
                for dec in fd.decisions {
                    let (mut action_rec, trades, log_entry, err) = self
                        .execute_decision(dec.clone(), &price_map, ts, call_count)
                        .await;
                    if let Some(e) = err {
                        action_rec.success = false;
                        action_rec.error = e.to_string();
                        had_error = true;
                        exec_log.push(format!("❌ {} {}: {}", dec.symbol, dec.action, e));
                    } else {
                        action_rec.success = true;
                        exec_log.push(format!("✓ {} {}", dec.symbol, dec.action));
                    }
                    trade_events.extend(trades);
                    if !log_entry.is_empty() {
                        exec_log.push(log_entry);
                    }
                    rec.decisions.push(action_rec);
                }
            }
            decision_record = Some(rec);
        }

        let cycle_for_log = if decision_attempted {
            call_count
        } else {
            state_snap.decision_cycle
        };

        let (liq_events, liq_note) = self
            .check_liquidation(ts, &price_map, cycle_for_log)
            .await?;
        if !liq_events.is_empty() {
            had_error = true;
            trade_events.extend(liq_events);
            if let Some(_) = &mut decision_record {
                exec_log.push(format!("⚠️ Forced liquidation: {}", liq_note));
            }
        }

        if let Some(mut rec) = decision_record {
            rec.execution_log = exec_log;
            rec.success = !had_error && liq_note.is_empty();
            if !liq_note.is_empty() {
                rec.error_message = liq_note.clone();
            }
            self.log_decision(&rec).await?;
        }

        {
            let (equity, unrealized, _) = self.account.read().await.total_equity(&price_map);
            self.update_state(ts, equity, unrealized, decision_attempted)
                .await;
        }

        let snapshot = self.snapshot_state().await;

        self.maybe_checkpoint().await?;
        self.persist_metadata().await;
        self.persist_metrics(false).await;

        if !had_error && liq_note.is_empty() {
            self.set_last_error(None).await;
        }

        if snapshot.liquidated {
            return Err(BacktestError::Liquidated);
        }

        Ok(())
    }

    async fn build_decision_context(
        &self,
        ts: i64,
        market_data: &HashMap<String, Data>,
        multi_tf: HashMap<String, HashMap<String, Data>>,
        price_map: &HashMap<String, f64>,
        call_count: usize,
    ) -> Result<(Context, DecisionRecord)> {
        let (account, equity, unrealized, margin_used, margin_pct) = {
            let account = self.account.read().await;
            let (equity, unrealized, _) = account.total_equity(price_map);
            let margin_used = self.total_margin_used().await;
            let margin_pct = if equity > 0.0 {
                (margin_used / equity) * 100.0
            } else {
                0.0
            };

            (account, equity, unrealized, margin_used, margin_pct)
        };

        let account_info = AccountInfo {
            total_equity: equity,
            available_balance: account.cash(),
            total_pnl: equity - account.initial_balance(),
            total_pnl_pct: ((equity - account.initial_balance()) / account.initial_balance())
                * 100.0,
            margin_used,
            margin_used_pct: margin_pct,
            position_count: account.positions().len(),
            unrealized_pnl: unrealized,
        };

        // Convert positions
        let mut pos_infos = Vec::new();
        let mut pos_snaps = Vec::new();
        for p in account.positions() {
            let price = *price_map.get(&p.symbol).unwrap_or(&0.0);
            pos_infos.push(PositionInfo {
                symbol: p.symbol.clone(),
                side: p.side.clone().as_str().to_string(),
                entry_price: p.entry_price,
                mark_price: price,
                quantity: p.quantity,
                leverage: p.leverage,
                unrealized_pnl: calculate_unrealized_pnl(p, price),
                unrealized_pnl_pct: 0.0,
                liquidation_price: p.liquidation_price,
                margin_used: p.margin,
                update_time: Utc::now().timestamp_millis(),
                peak_pnl_pct: 0_f64,
            });
            pos_snaps.push(PositionSnapshot {
                symbol: p.symbol.clone(),
                side: p.side.clone().as_str().to_string(),
                position_amt: p.quantity,
                entry_price: p.entry_price,
                mark_price: price,
                unrealized_profit: calculate_unrealized_pnl(p, price),
                leverage: p.leverage as f64,
                liquidation_price: p.liquidation_price,
            });
        }

        let candidates: Vec<CandidateCoin> = self
            .cfg
            .symbols
            .iter()
            .map(|s| CandidateCoin {
                symbol: s.clone(),
                sources: Vec::new(),
            })
            .collect();

        let ctx = Context {
            current_time: Utc.timestamp_millis_opt(ts).unwrap().to_rfc3339(),
            runtime_minutes: (ts - self.cfg.start_ts * 1000) as i32 / 60000,
            call_count: call_count as i32,
            account: account_info.clone(),
            positions: pos_infos,
            candidate_coins: candidates.clone(),
            prompt_variant: Some(self.cfg.prompt_variant.clone()),
            market_data_map: market_data.clone(),
            multi_tf_market: multi_tf,
            btc_eth_leverage: self.cfg.leverage.btc_eth_leverage,
            altcoin_leverage: self.cfg.leverage.altcoin_leverage,
            trading_stats: None,
            recent_orders: Vec::new(),
            oi_top_data_map: None,
            quant_data_map: None,
        };

        let record = DecisionRecord {
            account_state: AccountSnapshot {
                total_balance: account_info.total_equity,
                available_balance: account_info.available_balance,
                total_unrealized_profit: unrealized,
                position_count: account_info.position_count as i32,
                margin_used_pct: account_info.margin_used_pct,
                initial_balance: 0_f64,
            }
            .into(),
            candidate_coins: candidates.iter().map(|c| c.symbol.clone()).collect(),
            positions: pos_snaps,
            timestamp: Utc.timestamp_millis_opt(ts).unwrap(),
            success: false,
            error_message: String::new(),
            execution_log: Vec::new(),
            decisions: Vec::new(),
            input_prompt: "".into(),
            cot_trace: "".into(),
            decision_json: "".into(),
            id: 0_i64,
            trader_id: String::new(),
            cycle_number: 0,
            system_prompt: String::new(),
            ai_request_duration_ms: 0_i64,
        };

        Ok((ctx, record))
    }

    async fn execute_decision(
        &self,
        dec: Decision,
        price_map: &HashMap<String, f64>,
        ts: i64,
        cycle: usize,
    ) -> (
        DecisionAction,
        Vec<TradeEvent>,
        String,
        Option<anyhow::Error>,
    ) {
        let symbol = dec.symbol.clone();
        let leverage = self.resolve_leverage(dec.leverage.unwrap(), &symbol);

        let mut action_record = DecisionAction {
            action: dec.action.clone(),
            symbol: symbol.clone(),
            leverage,
            timestamp: Utc.timestamp_millis_opt(ts).unwrap(),
            quantity: 0.0,
            price: 0.0,
            success: false,
            error: String::new(),
            order_id: 0_i64,
        };

        let base_price = match price_map.get(&symbol) {
            Some(&p) => p,
            None => {
                return (
                    action_record,
                    vec![],
                    "".to_string(),
                    Some(anyhow!("price unavailable")),
                );
            }
        };

        let fill_price = self.execution_price(&symbol, base_price, ts).await;
        let mut acct = self.account.write().await;

        match dec.action.as_str() {
            "open_long" | "open_short" => {
                let qty = self.determine_quantity(&dec, base_price).await;
                if qty <= 0.0 {
                    return (
                        action_record,
                        vec![],
                        "".to_string(),
                        Some(anyhow!("invalid qty")),
                    );
                }
                let side = if dec.action == "open_long" {
                    "long"
                } else {
                    "short"
                };

                match acct.open(symbol.as_str(), side, qty, leverage, fill_price, ts) {
                    Ok((pos, fee, exec_price)) => {
                        action_record.quantity = qty;
                        action_record.price = exec_price;
                        action_record.leverage = pos.leverage;
                        let trade = TradeEvent {
                            timestamp: ts,
                            symbol: symbol.clone(),
                            action: dec.action.clone(),
                            side: side.to_string().into(),
                            quantity: qty,
                            price: exec_price,
                            fee,
                            slippage: (exec_price - base_price).abs(),
                            order_value: exec_price * qty,
                            realized_pnl: 0.0,
                            leverage: pos.leverage.into(),
                            cycle: cycle as i32,
                            position_after: pos.quantity,
                            liquidation_flag: false,
                            note: None,
                        };
                        (action_record, vec![trade], "".into(), None)
                    }
                    Err(e) => (action_record, vec![], "".into(), Some(e)),
                }
            }
            "close_long" | "close_short" => {
                let side = if dec.action == "close_long" {
                    "long"
                } else {
                    "short"
                };
                let qty = self.determine_close_quantity(&acct, &symbol, side, &dec);
                if qty <= 0.0 {
                    return (
                        action_record,
                        vec![],
                        "".into(),
                        Some(anyhow!("invalid close qty")),
                    );
                }
                let pos_lev = acct.position_leverage(&symbol, side);
                match acct.close(symbol.as_str(), side, qty, fill_price) {
                    Ok((realized, fee, exec_price)) => {
                        action_record.quantity = qty;
                        action_record.price = exec_price;
                        action_record.leverage = pos_lev;
                        let trade = TradeEvent {
                            timestamp: ts,
                            symbol: symbol.clone(),
                            action: dec.action.clone(),
                            side: side.to_string().into(),
                            quantity: qty,
                            price: exec_price,
                            fee,
                            slippage: (exec_price - base_price).abs(),
                            order_value: exec_price * qty,
                            realized_pnl: realized - fee,
                            leverage: pos_lev.into(),
                            cycle: cycle as i32,
                            position_after: 0.0,
                            liquidation_flag: false,
                            note: None,
                        };
                        (action_record, vec![trade], "".into(), None)
                    }
                    Err(e) => (action_record, vec![], "".into(), Some(e)),
                }
            }
            "hold" | "wait" => (action_record, vec![], format!("hold: {}", dec.action), None),
            _ => (
                action_record,
                vec![],
                "".into(),
                Some(anyhow!("unsupported action")),
            ),
        }
    }

    async fn check_liquidation(
        &self,
        ts: i64,
        price_map: &HashMap<String, f64>,
        cycle: usize,
    ) -> Result<(Vec<TradeEvent>, String)> {
        let positions: Vec<Position> = {
            let acct = self.account.read().await;
            acct.positions().into_iter().cloned().collect()
        };

        let mut events = Vec::new();
        let mut notes = Vec::new();

        for pos in positions {
            let price = *price_map.get(&pos.symbol).unwrap_or(&0.0);
            let liq_price = pos.liquidation_price;
            let mut trigger = false;
            let mut exec_price = price;

            if (pos.side.as_str() == "long" && price <= liq_price)
                || (pos.side.as_str() == "short" && price >= liq_price)
            {
                if liq_price > 0.0 {
                    trigger = true;
                    exec_price = liq_price;
                }
            }

            if trigger {
                let (realized, fee, final_price) = {
                    let mut acct = self.account.write().await;
                    acct.close(
                        pos.symbol.as_str(),
                        pos.side.as_str(),
                        pos.quantity,
                        exec_price,
                    )?
                };

                notes.push(format!(
                    "{} {} @ {:.4}",
                    pos.symbol,
                    pos.side.as_str(),
                    final_price
                ));
                events.push(TradeEvent {
                    timestamp: ts,
                    symbol: pos.symbol.clone(),
                    action: "liquidated".into(),
                    side: pos.side.as_str().to_string().into(),
                    quantity: pos.quantity,
                    price: final_price,
                    fee,
                    slippage: 0.0,
                    order_value: final_price * pos.quantity,
                    realized_pnl: realized - fee,
                    leverage: pos.leverage.into(),
                    cycle: cycle as i32,
                    position_after: 0.0,
                    liquidation_flag: true,
                    note: Some(format!("forced liquidation at {:.4}", final_price)),
                });
            }
        }

        let note = notes.join("; ");
        if !note.is_empty() {
            let mut state = self.state.write().await;
            state.liquidated = true;
            state.liquidation_note = note.clone();
        }
        Ok((events, note))
    }

    async fn invoke_ai_with_retry(&self, ctx: &mut Context) -> Result<FullDecision> {
        let mut last_err = anyhow!("unknown");
        for i in 0..AI_DECISION_MAX_RETRIES {
            match get_full_decision_with_custom_prompt(
                ctx,
                &self.mcp_client,
                &self.cfg.custom_prompt,
                self.cfg.override_base_prompt,
                &self.cfg.prompt_template,
                &self.oi_client,
            )
            .await
            {
                // Assuming trait has this method
                Ok(fd) => return Ok(fd),
                Err(e) => {
                    last_err = e.into();
                    sleep(Duration::from_millis(((i + 1) * 500) as u64)).await;
                }
            }
        }
        Err(last_err)
    }

    async fn snapshot_state(&self) -> BacktestState {
        self.state.read().await.clone()
    }

    async fn update_state(&self, ts: i64, equity: f64, unrealized: f64, advanced: bool) {
        let mut state = self.state.write().await;
        let acct = self.account.read().await;

        if state.max_equity == 0.0 || equity > state.max_equity {
            state.max_equity = equity;
        }
        if state.min_equity == 0.0 || equity < state.min_equity {
            state.min_equity = equity;
        }
        if state.max_equity > 0.0 {
            let dd = ((state.max_equity - equity) / state.max_equity) * 100.0;
            if dd > state.max_drawdown_pct {
                state.max_drawdown_pct = dd;
            }
        }

        let mut positions = HashMap::new();
        for pos in acct.positions() {
            positions.insert(
                format!("{}:{}", pos.symbol, pos.side.as_str()),
                crate::types::PositionSnapshot {
                    symbol: pos.symbol.clone(),
                    side: pos.side.as_str().to_string(),
                    quantity: pos.quantity,
                    avg_price: pos.entry_price,
                    leverage: pos.leverage,
                    liquidation_price: pos.liquidation_price,
                    margin_used: pos.margin,
                    open_time: pos.open_time,
                },
            );
        }

        state.bar_timestamp = ts;
        state.bar_index += 1;
        if advanced {
            state.decision_cycle += 1;
        }
        state.cash = acct.cash();
        state.equity = equity;
        state.unrealized_pnl = unrealized;
        state.realized_pnl = acct.realized_pnl();
        state.positions = positions;
        state.last_update = Utc::now();
    }

    fn resolve_leverage(&self, req: i32, symbol: &str) -> i32 {
        if req > 0 {
            return req;
        }
        let sym = symbol.to_uppercase();
        if sym == "BTCUSDT" || sym == "ETHUSDT" {
            if self.cfg.leverage.btc_eth_leverage > 0 {
                return self.cfg.leverage.btc_eth_leverage;
            }
        } else if self.cfg.leverage.altcoin_leverage > 0 {
            return self.cfg.leverage.altcoin_leverage;
        }
        5
    }

    async fn execution_price(&self, symbol: &str, mark: f64, ts: i64) -> f64 {
        let (curr, next) = self.feed.decision_bar_snapshot(symbol, ts);
        match self.cfg.fill_policy {
            FillPolicy::NextOpen => {
                if let Some(k) = next {
                    if k.open > 0.0 {
                        return k.open;
                    }
                }
            }
            FillPolicy::MidPrice => {
                if let Some(k) = curr {
                    if k.high > 0.0 && k.low > 0.0 {
                        return (k.high + k.low) / 2.0;
                    }
                }
            }
            FillPolicy::BarVWAP => { /* calculate vwap */ }
        }
        mark
    }

    async fn determine_quantity(&self, dec: &Decision, price: f64) -> f64 {
        let state = self.state.read().await;
        let equity = if state.equity <= 0.0 {
            self.cfg.initial_balance
        } else {
            state.equity
        };
        let usd = if dec.position_size_usd.unwrap_or_default() <= 0.0 {
            0.05 * equity
        } else {
            dec.position_size_usd.unwrap_or_default()
        };
        if price <= 0.0 { 0.0 } else { usd / price }
    }

    fn determine_close_quantity(
        &self,
        acct: &BacktestAccount,
        symbol: &str,
        side: &str,
        _dec: &Decision,
    ) -> f64 {
        acct.positions()
            .iter()
            .find(|p| p.symbol == symbol && p.side.as_str() == side)
            .map(|p| p.quantity)
            .unwrap_or(0.0)
    }

    async fn total_margin_used(&self) -> f64 {
        self.account
            .read()
            .await
            .positions()
            .iter()
            .map(|p| p.margin)
            .sum()
    }

    fn should_trigger_decision(&self, idx: usize) -> bool {
        self.cfg.decision_cadence_nbars <= 1
            || (idx % self.cfg.decision_cadence_nbars as usize == 0)
    }

    async fn handle_stop(self: Arc<Self>, err: Option<anyhow::Error>) -> Result<()> {
        self.force_checkpoint().await?;

        self.set_last_error(err.map(|e| e.to_string())).await;
        *self.status.write().await = RunState::Stopped;
        self.persist_metadata().await;
        self.persist_metrics(true).await?;
        self.release_lock().await?;

        Ok(())
    }

    async fn handle_completion(self: Arc<Self>) -> Result<()> {
        self.set_last_error(None).await;
        *self.status.write().await = RunState::Completed;
        self.persist_metadata().await;
        self.persist_metrics(true).await?;
        self.release_lock().await?;
        Ok(())
    }

    async fn handle_liquidation(self: Arc<Self>) -> Result<()> {
        self.force_checkpoint().await?;
        self.set_last_error(Some("account liquidated".into())).await;
        *self.status.write().await = RunState::Liquidated;
        self.persist_metadata().await;
        self.persist_metrics(true).await?;
        self.release_lock().await?;

        Ok(())
    }

    async fn handle_failure(self: Arc<Self>, err: anyhow::Error) -> Result<()> {
        self.force_checkpoint().await?;
        self.set_last_error(Some(err.to_string())).await;
        *self.status.write().await = RunState::Failed;
        self.persist_metadata().await;
        self.persist_metrics(true).await?;
        self.release_lock().await?;

        Ok(())
    }

    async fn handle_pause(&self) -> Result<()> {
        self.force_checkpoint().await?;
        *self.status.write().await = RunState::Paused;
        self.persist_metadata().await;
        self.persist_metrics(true).await?;

        Ok(())
    }

    async fn resume_from_pause(&self) {
        *self.status.write().await = RunState::Running;
        self.persist_metadata().await;
    }

    pub async fn pause(&self) {
        self.pause_notify.notify_waiters();
    }
    pub async fn resume(&self) {
        self.resume_notify.notify_waiters();
    }
    pub async fn stop(&self) {
        self.stop_notify.notify_waiters();
    }

    // Status Payload Builder
    pub async fn status_payload(self: Arc<Self>) -> StatusPayload {
        let snap = self.snapshot_state().await;
        let progress = self.progress_percent(&snap);
        StatusPayload {
            run_id: self.cfg.run_id.clone(),
            state: self.status.read().await.clone(),
            progress_pct: progress,
            processed_bars: snap.bar_index as i32,
            current_time: snap.bar_timestamp,
            decision_cycle: snap.decision_cycle as i32,
            equity: snap.equity,
            unrealized_pnl: snap.unrealized_pnl,
            realized_pnl: snap.realized_pnl,
            note: snap.liquidation_note.clone().into(),
            last_error: self.error_state.read().await.last_error.clone(),
            last_updated_iso: snap.last_update.to_rfc3339(),
        }
    }

    fn progress_percent(&self, state: &BacktestState) -> f64 {
        let duration = self.cfg.duration().as_seconds_f64();
        if duration <= 0_f64 || state.bar_timestamp == 0 {
            return 0.0;
        }
        let elapsed = state.bar_timestamp - self.cfg.start_ts;
        let pct = (elapsed as f64 / duration) * 100.0;
        pct.clamp(0.0, 100.0)
    }

    // Persistence Stubs
    async fn set_last_error(&self, err: Option<String>) {
        self.error_state.write().await.last_error = err;
    }

    pub async fn persist_metadata(&self) {
        let state = { self.state.read().await.clone() };
        let status = { self.status.read().await.clone() };
        let summary = RunSummary {
            symbol_count: self.cfg.symbols.len(),
            decision_tf: self.cfg.decision_timeframe.clone(),
            processed_bars: state.bar_index,
            progress_pct: self.progress_percent(&state),
            equity_last: state.equity,
            max_drawdown_pct: state.max_drawdown_pct,
            liquidated: state.liquidated,
            liquidation_note: state.liquidation_note.into(),
        };
        let mut meta = RunMetadata {
            run_id: self.cfg.run_id.clone(),
            user_id: self.cfg.user_id.clone().into(),
            state: status.clone(),
            last_error: self.error_state.read().await.last_error.clone(),
            created_at: Utc::now(),
            updated_at: state.last_update,
            summary,
            version: 0,
            label: None,
        };

        match save_run_metadata(&mut meta).await {
            Ok(_) => match update_run_index(&meta, Some(&self.cfg)).await {
                Ok(_) => {}
                Err(e) => {
                    info!(
                        "failed to save run metadata for {}: {}",
                        &self.cfg.run_id, e
                    )
                }
            },
            Err(e) => {
                info!(
                    "failed to save run metadata for {}: {}",
                    &self.cfg.run_id, e
                )
            }
        }

        info!("Metadata saved for {}", meta.run_id);
    }

    async fn persist_metrics(&self, force: bool) -> Result<()> {
        if self.cfg.run_id.is_empty() {
            return Ok(());
        }

        let last_metric_write: DateTime<Utc> = { self.metrics.read().await.last_metrics_write };
        if !force && last_metric_write != DateTime::<Utc>::default() {
            if Utc::now() - last_metric_write < METRICS_WRITE_INTERVAL {
                return Ok(());
            }
        }

        let state = { self.state.read().await.clone() };
        let metrics = calculate_metrics(&self.cfg.run_id, Some(&self.cfg), Some(&state)).await?;
        persist_metrics(&self.cfg.run_id, &metrics).await?;

        self.metrics.write().await.last_metrics_write = Utc::now();
        Ok(())
    }

    async fn maybe_checkpoint(&self) -> Result<()> {
        let should;
        {
            let state = { self.state.read().await.clone() };
            let last = { self.metrics.read().await.last_checkpoint.clone() };
            let interval = if self.cfg.checkpoint_interval_seconds > 0 {
                self.cfg.checkpoint_interval_seconds
            } else {
                2
            };
            let time_check = (Utc::now() - last).num_seconds() as i32 >= interval;
            let bar_check = self.cfg.checkpoint_interval_bars > 0
                && state.bar_index > 0
                && state.bar_index % self.cfg.checkpoint_interval_bars as usize == 0;
            should = time_check || bar_check;
        }
        if should {
            self.force_checkpoint().await;
        }
        Ok(())
    }

    async fn force_checkpoint(&self) -> Result<()> {
        let state = { self.state.read().await.clone() };

        {
            self.metrics.write().await.last_checkpoint = Utc::now()
        };

        self.save_checkpoint(&state).await?;
        Ok(())
    }

    async fn log_decision(&self, record: &DecisionRecord) -> Result<()> {
        persist_decision_record(&self.cfg.run_id, Some(record)).await?;
        Ok(())
    }

    async fn save_checkpoint(&self, state: &BacktestState) -> Result<()> {
        let ckpt = self.build_checkpoint_from_state(state).unwrap();

        save_checkpoint(&self.cfg.run_id, &ckpt).await?;

        self.metrics.write().await.last_checkpoint = Utc::now();
        Ok(())
    }

    fn build_checkpoint_from_state(&self, state: &BacktestState) -> Result<Checkpoint> {
        let ckpt = Checkpoint {
            bar_index: state.bar_index,
            bar_timestamp: state.bar_timestamp,
            cash: state.cash,
            equity: state.equity,
            unrealized_pnl: state.unrealized_pnl,
            realized_pnl: state.realized_pnl,
            positions: self.snapshot_for_checkpoint(state).unwrap(),
            decision_cycle: state.decision_cycle,
            liquidated: state.liquidated,
            liquidation_note: Some(state.liquidation_note.clone()),
            max_equity: state.max_equity,
            min_equity: state.min_equity,
            max_drawdown_pct: state.max_drawdown_pct,
            ai_cache_ref: Some(self.cache_path.to_string_lossy().into_owned()),
            indicators_state: None,
            rng_seed: None,
        };

        Ok(ckpt)
    }

    fn snapshot_for_checkpoint(
        &self,
        state: &BacktestState,
    ) -> Result<Vec<crate::types::PositionSnapshot>> {
        let mut poses = Vec::new();
        for (_, pos) in &state.positions {
            poses.push(pos.clone());
        }

        poses.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.side.cmp(&b.side)));

        Ok(poses)
    }

    async fn build_metadata(
        &self,
        state: &BacktestState,
        run_state: &mut RunState,
    ) -> Result<RunMetadata> {
        if state.liquidated && *run_state != RunState::Liquidated {
            *run_state = RunState::Liquidated;
        }

        let progress = self.progress_percent(state);

        let summary = RunSummary {
            symbol_count: self.cfg.symbols.len(),
            decision_tf: self.cfg.decision_timeframe.clone(),
            processed_bars: state.bar_index,
            progress_pct: progress,
            equity_last: state.equity,
            max_drawdown_pct: state.max_drawdown_pct,
            liquidated: state.liquidated,
            liquidation_note: state.liquidation_note.clone().into(),
        };

        let meta = RunMetadata {
            run_id: self.cfg.run_id.clone(),
            user_id: self.cfg.user_id.clone().into(),
            state: run_state.clone(),
            last_error: self.error_state.read().await.last_error.clone(),
            summary: summary,
            created_at: Utc::now(),
            label: None,
            version: 0,
            updated_at: Utc::now(),
        };

        Ok(meta)
    }

    pub async fn wait(&self) -> Result<Option<String>> {
        self.done_notify.notified().await;

        Ok(self.error_state.read().await.last_error.clone())
    }
}
