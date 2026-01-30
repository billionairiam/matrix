use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use crate::engine::{CandidateCoin, Context, PositionInfo};
use anyhow::{Result, anyhow};
use chrono::Utc;
use market::{
    data::{get, normalize},
    types::{Data, TimeframeSeriesData},
};
use pool::coin_pool::CoinPoolClient;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use store::strategy::{ExternalDataSource, IndicatorConfig, RiskControlConfig, StrategyConfig};
use tracing::{info, instrument, warn};

// Data structures for Quantitative Data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantData {
    pub symbol: String,
    pub price: f64,
    pub netflow: Option<NetflowData>,
    pub oi: Option<HashMap<String, OIData>>,
    pub price_change: Option<HashMap<String, f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetflowData {
    pub institution: Option<FlowTypeData>,
    pub personal: Option<FlowTypeData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTypeData {
    pub future: Option<HashMap<String, f64>>,
    pub spot: Option<HashMap<String, f64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OIData {
    pub current_oi: f64,
    pub net_long: f64,
    pub net_short: f64,
    pub delta: Option<HashMap<String, OIDeltaData>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OIDeltaData {
    pub oi_delta: f64,
    pub oi_delta_value: f64,
    pub oi_delta_percent: f64,
}

#[derive(Debug, Deserialize)]
struct QuantApiResponse {
    code: i32,
    data: Option<QuantData>,
}

pub struct OrderInfo {
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub realized_pnl: f64,
    pub pnl_pct: f64,
    pub filled_at: String,
}

#[derive(Clone)]
pub struct StrategyEngine {
    config: StrategyConfig,
    http_client: Arc<Client>,
    coin_client: Arc<CoinPoolClient>,
}

impl StrategyEngine {
    /// Creates strategy execution engine
    #[instrument]
    pub fn new(config: StrategyConfig) -> Self {
        Self {
            config,
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default()
                .into(),
            coin_client: CoinPoolClient::new().into(),
        }
    }

    /// Gets candidate coins based on strategy configuration
    pub async fn get_candidate_coins(&self) -> Result<Vec<CandidateCoin>> {
        let coin_source = &self.config.coin_source;
        let mut candidates = Vec::new();
        let mut symbol_sources: HashMap<String, Vec<String>> = HashMap::new();

        // Set custom API URL (if configured)
        if !coin_source.coin_pool_api_url.is_empty() {
            self.coin_client
                .set_coin_pool_api(coin_source.coin_pool_api_url.clone())
                .await;
            info!(
                "✓ Using strategy-configured AI500 API URL: {}",
                coin_source.coin_pool_api_url
            );
        }
        if !coin_source.oi_top_api_url.is_empty() {
            self.coin_client
                .set_oi_top_api(coin_source.oi_top_api_url.clone())
                .await;
            info!(
                "✓ Using strategy-configured OI Top API URL: {}",
                coin_source.oi_top_api_url
            );
        }

        match coin_source.source_type.as_str() {
            "static" => {
                for symbol in &coin_source.static_coins {
                    let norm_symbol = normalize(symbol);
                    candidates.push(CandidateCoin {
                        symbol: norm_symbol,
                        sources: vec!["static".to_string()],
                    });
                }
                Ok(candidates)
            }
            "coinpool" => self.get_coin_pool_coins(coin_source.coin_pool_limit).await,
            "oi_top" => self.get_oi_top_coins(coin_source.oi_top_limit).await,
            "mixed" => {
                // AI500
                if coin_source.use_coin_pool {
                    match self.get_coin_pool_coins(coin_source.coin_pool_limit).await {
                        Ok(pool_coins) => {
                            for coin in pool_coins {
                                symbol_sources
                                    .entry(coin.symbol)
                                    .or_default()
                                    .push("ai500".to_string());
                            }
                        }
                        Err(e) => warn!("⚠️  Failed to get AI500 coin pool: {:?}", e),
                    }
                }

                // OI Top
                if coin_source.use_oi_top {
                    match self.get_oi_top_coins(coin_source.oi_top_limit).await {
                        Ok(oi_coins) => {
                            for coin in oi_coins {
                                symbol_sources
                                    .entry(coin.symbol)
                                    .or_default()
                                    .push("oi_top".to_string());
                            }
                        }
                        Err(e) => warn!("⚠️  Failed to get OI Top: {:?}", e),
                    }
                }

                // Static
                for symbol in &coin_source.static_coins {
                    let norm_symbol = normalize(symbol);
                    let entry = symbol_sources.entry(norm_symbol).or_default();
                    entry.push("static".to_string());
                }

                // Convert to list
                for (symbol, sources) in symbol_sources {
                    candidates.push(CandidateCoin { symbol, sources });
                }
                Ok(candidates)
            }
            _ => Err(anyhow!(
                "unknown coin source type: {}",
                coin_source.source_type
            )),
        }
    }

    async fn get_coin_pool_coins(&self, limit: i32) -> Result<Vec<CandidateCoin>> {
        let limit = if limit <= 0 { 30 } else { limit };
        // Assuming pool::get_top_rated_coins returns Result<Vec<String>>
        let symbols = self.coin_client.get_top_rated_coins(limit as usize).await?;

        let candidates = symbols
            .into_iter()
            .map(|s| CandidateCoin {
                symbol: s,
                sources: vec!["ai500".to_string()],
            })
            .collect();

        Ok(candidates)
    }

    async fn get_oi_top_coins(&self, limit: i32) -> Result<Vec<CandidateCoin>> {
        let limit = if limit <= 0 { 20 } else { limit } as usize;
        // Assuming pool::get_oi_top_positions returns Result<Vec<PositionStruct>>
        let positions = self.coin_client.get_oi_top_positions().await?;

        let mut candidates = Vec::new();
        for (i, pos) in positions.iter().enumerate() {
            if i >= limit {
                break;
            }
            // Assuming pos has a field 'symbol'
            let symbol = normalize(&pos.symbol);
            candidates.push(CandidateCoin {
                symbol,
                sources: vec!["oi_top".to_string()],
            });
        }
        Ok(candidates)
    }

    /// Fetches market data based on strategy configuration
    pub async fn fetch_market_data(&self, symbol: &str) -> Result<Data> {
        get(symbol).await
    }

    /// Fetches external data sources
    #[instrument(skip_all)]
    pub async fn fetch_external_data(&self) -> Result<HashMap<String, Value>> {
        let mut external_data = HashMap::new();

        for source in &self.config.indicators.external_data_sources {
            match self.fetch_single_external_source(source).await {
                Ok(data) => {
                    external_data.insert(source.name.clone(), data);
                }
                Err(e) => {
                    warn!(
                        "⚠️  Failed to fetch external data source [{}]: {:?}",
                        source.name, e
                    );
                }
            }
        }
        Ok(external_data)
    }

    /// Fetches quantitative data for a single coin
    pub async fn fetch_quant_data(&self, symbol: &str) -> Result<Option<QuantData>> {
        if !self.config.indicators.enable_quant_data
            || self.config.indicators.quant_data_api_url.is_empty()
        {
            return Ok(None);
        }

        let url = self
            .config
            .indicators
            .quant_data_api_url
            .replace("{symbol}", symbol);

        let resp = self.http_client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(anyhow!("HTTP status code: {}", resp.status()));
        }

        let api_resp: QuantApiResponse = resp.json().await?;

        if api_resp.code != 0 {
            return Err(anyhow!("API returned error code: {}", api_resp.code));
        }

        Ok(api_resp.data)
    }

    /// Batch fetches quantitative data
    #[instrument(skip_all)]
    pub async fn fetch_quant_data_batch(
        &self,
        symbols: &Vec<String>,
    ) -> HashMap<String, QuantData> {
        let mut result = HashMap::new();

        if !self.config.indicators.enable_quant_data
            || self.config.indicators.quant_data_api_url.is_empty()
        {
            return result;
        }

        // Ideally, this should use `futures::stream::iter` with `buffer_unordered` for concurrency,
        // but for a direct translation we loop sequentially or simple join.
        // Here is a sequential loop to match the Go logic structure closely.
        for symbol in symbols {
            match self.fetch_quant_data(symbol).await {
                Ok(Some(data)) => {
                    result.insert(symbol.clone(), data);
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        "⚠️  Failed to fetch quantitative data for {}: {:?}",
                        symbol, e
                    );
                }
            }
        }

        result
    }

    fn format_quant_data(&self, data: &QuantData) -> String {
        let mut sb = String::new();
        writeln!(sb, "📊 Quantitative Data:").unwrap();

        // Price changes
        if let Some(price_change) = &data.price_change {
            if !price_change.is_empty() {
                write!(sb, "Price Change: ").unwrap();
                let timeframes = ["5m", "15m", "1h", "4h", "24h"];
                let mut parts = Vec::new();
                for tf in timeframes {
                    if let Some(v) = price_change.get(tf) {
                        parts.push(format!("{}: {:+.2}%", tf, v));
                    }
                }
                writeln!(sb, "{}", parts.join(" | ")).unwrap();
            }
        }

        // Fund flow
        if let Some(netflow) = &data.netflow {
            writeln!(sb, "Fund Flow (USDT):").unwrap();
            let tfs = ["1h", "4h", "24h"];

            // Institution
            if let Some(inst) = &netflow.institution {
                if let Some(future) = &inst.future {
                    write!(sb, "  Institutional Futures: ").unwrap();
                    let mut parts = Vec::new();
                    for tf in &tfs {
                        if let Some(v) = future.get(*tf) {
                            parts.push(format!("{}: {:+.0}", tf, v));
                        }
                    }
                    writeln!(sb, "{}", parts.join(" | ")).unwrap();
                }
                if let Some(spot) = &inst.spot {
                    write!(sb, "  Institutional Spot: ").unwrap();
                    let mut parts = Vec::new();
                    for tf in &tfs {
                        if let Some(v) = spot.get(*tf) {
                            parts.push(format!("{}: {:+.0}", tf, v));
                        }
                    }
                    writeln!(sb, "{}", parts.join(" | ")).unwrap();
                }
            }

            // Retail
            if let Some(personal) = &netflow.personal {
                if let Some(future) = &personal.future {
                    write!(sb, "  Retail Futures: ").unwrap();
                    let mut parts = Vec::new();
                    for tf in &tfs {
                        if let Some(v) = future.get(*tf) {
                            parts.push(format!("{}: {:+.0}", tf, v));
                        }
                    }
                    writeln!(sb, "{}", parts.join(" | ")).unwrap();
                }
            }
        }

        // Position data
        if let Some(oi_map) = &data.oi {
            for (exchange, oi_data) in oi_map {
                writeln!(
                    sb,
                    "Open Interest ({}): Current {:.2} | Long {:.2} Short {:.2}",
                    exchange, oi_data.current_oi, oi_data.net_long, oi_data.net_short
                )
                .unwrap();

                if let Some(delta) = &oi_data.delta {
                    write!(sb, "  OI Change: ").unwrap();
                    let mut parts = Vec::new();
                    for tf in &["1h", "4h", "24h"] {
                        if let Some(d) = delta.get(*tf) {
                            parts.push(format!("{}: {:+.2}%", tf, d.oi_delta_percent));
                        }
                    }
                    writeln!(sb, "{}", parts.join(" | ")).unwrap();
                }
            }
        }

        sb
    }

    async fn fetch_single_external_source(&self, source: &ExternalDataSource) -> Result<Value> {
        let timeout = if source.refresh_secs > 0 {
            Duration::from_secs(source.refresh_secs as u64)
        } else {
            Duration::from_secs(30)
        };

        // Create a specific client for this request to respect specific timeout
        let client = Client::builder().timeout(timeout).build()?;

        let mut req_builder = match source.method.to_uppercase().as_str() {
            "POST" => client.post(&source.url),
            _ => client.get(&source.url),
        };

        for (k, v) in &source.headers {
            req_builder = req_builder.header(k, v);
        }

        let resp = req_builder.send().await?;
        let json_val: Value = resp.json().await?;

        if !source.data_path.is_empty() {
            Self::extract_json_path(&json_val, &source.data_path)
                .ok_or_else(|| anyhow!("Path not found in JSON"))
        } else {
            Ok(json_val)
        }
    }

    fn extract_json_path(data: &Value, path: &str) -> Option<Value> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = data;
        for part in parts {
            current = current.get(part)?;
        }
        Some(current.clone())
    }

    /// Builds User Prompt based on strategy configuration
    pub fn build_user_prompt(&self, ctx: &Context) -> String {
        let mut sb = String::new();

        // System status
        writeln!(
            sb,
            "Time: {} | Period: #{} | Runtime: {} minutes\n",
            ctx.current_time, ctx.call_count, ctx.runtime_minutes
        )
        .unwrap();

        // BTC market
        if let Some(btc_data) = ctx.market_data_map.get("BTCUSDT") {
            writeln!(
                sb,
                "BTC: {:.2} (1h: {:+.2}%, 4h: {:+.2}%) | MACD: {:.4} | RSI: {:.2}\n",
                btc_data.current_price,
                btc_data.price_change_1h,
                btc_data.price_change_4h,
                btc_data.current_macd,
                btc_data.current_rsi7
            )
            .unwrap();
        }

        // Account information
        let balance_pct = if ctx.account.total_equity != 0.0 {
            (ctx.account.available_balance / ctx.account.total_equity) * 100.0
        } else {
            0.0
        };

        writeln!(sb, "Account: Equity {:.2} | Balance {:.2} ({:.1}%) | PnL {:+.2}% | Margin {:.1}% | Positions {}\n",
            ctx.account.total_equity,
            ctx.account.available_balance,
            balance_pct,
            ctx.account.total_pnl_pct,
            ctx.account.margin_used_pct,
            ctx.account.position_count).unwrap();

        // Position information
        if !ctx.positions.is_empty() {
            writeln!(sb, "## Current Positions").unwrap();
            for (i, pos) in ctx.positions.iter().enumerate() {
                sb.push_str(&self.format_position_info(i + 1, pos, ctx));
            }
        } else {
            writeln!(sb, "Current Positions: None\n").unwrap();
        }

        // Trading statistics
        if let Some(stats) = &ctx.trading_stats {
            if stats.total_trades > 0 {
                writeln!(sb, "## Historical Trading Statistics").unwrap();
                writeln!(sb, "Total Trades: {} | Win Rate: {:.1}% | Profit Factor: {:.2} | Sharpe Ratio: {:.2}",
                    stats.total_trades, stats.win_rate, stats.profit_factor, stats.sharpe_ratio).unwrap();
                writeln!(sb, "Total P&L: {:.2} USDT | Avg Win: {:.2} | Avg Loss: {:.2} | Max Drawdown: {:.1}%\n",
                    stats.total_pnl, stats.avg_win, stats.avg_loss, stats.max_drawdown_pct).unwrap();
            }
        }

        // Recent orders
        if !ctx.recent_orders.is_empty() {
            writeln!(sb, "## Recent Completed Trades").unwrap();
            for (i, order) in ctx.recent_orders.iter().enumerate() {
                let result_str = if order.realized_pnl < 0.0 {
                    "Loss"
                } else {
                    "Profit"
                };
                writeln!(
                    sb,
                    "{}. {} {} | Entry {:.4} Exit {:.4} | {}: {:+.2} USDT ({:+.2}%) | {}",
                    i + 1,
                    order.symbol,
                    order.side,
                    order.entry_price,
                    order.exit_price,
                    result_str,
                    order.realized_pnl,
                    order.pnl_pct,
                    order.filled_at
                )
                .unwrap();
            }
            writeln!(sb).unwrap();
        }

        // Candidate coins
        writeln!(
            sb,
            "## Candidate Coins ({} coins)\n",
            ctx.market_data_map.len()
        )
        .unwrap();
        let mut displayed_count = 0;

        for coin in &ctx.candidate_coins {
            if let Some(market_data) = ctx.market_data_map.get(&coin.symbol) {
                displayed_count += 1;
                let source_tags = self.format_coin_source_tag(&coin.sources);
                writeln!(
                    sb,
                    "### {}. {}{}\n",
                    displayed_count, coin.symbol, source_tags
                )
                .unwrap();
                sb.push_str(&self.format_market_data(market_data));

                // Add quant data
                if let Some(q_map) = &ctx.quant_data_map {
                    if let Some(quant_data) = q_map.get(&coin.symbol) {
                        sb.push_str(&self.format_quant_data(quant_data));
                    }
                }
                writeln!(sb).unwrap();
            }
        }
        writeln!(sb).unwrap();

        writeln!(sb, "---\n").unwrap();
        writeln!(
            sb,
            "Now please analyze and output your decision (Chain of Thought + JSON)"
        )
        .unwrap();

        sb
    }

    fn format_position_info(&self, index: usize, pos: &PositionInfo, ctx: &Context) -> String {
        let mut sb = String::new();

        let holding_duration = if pos.update_time > 0 {
            let duration_ms = Utc::now().timestamp_millis() - pos.update_time;
            let duration_min = duration_ms / (1000 * 60);
            if duration_min < 60 {
                format!(" | Holding Duration {} min", duration_min)
            } else {
                let h = duration_min / 60;
                let m = duration_min % 60;
                format!(" | Holding Duration {}h {}m", h, m)
            }
        } else {
            String::new()
        };

        let position_value = (pos.quantity * pos.mark_price).abs();

        writeln!(sb, "{}. {} {} | Entry {:.4} Current {:.4} | Qty {:.4} | Position Value {:.2} USDT | PnL {:+.2}% | PnL Amount {:+.2} USDT | Peak PnL {:.2}% | Leverage {}x | Margin {:.0} | Liq Price {:.4}{}\n",
            index, pos.symbol, pos.side.to_uppercase(),
            pos.entry_price, pos.mark_price, pos.quantity, position_value,
            pos.unrealized_pnl_pct, pos.unrealized_pnl, pos.peak_pnl_pct,
            pos.leverage, pos.margin_used, pos.liquidation_price, holding_duration
        ).unwrap();

        if let Some(market_data) = ctx.market_data_map.get(&pos.symbol) {
            sb.push_str(&self.format_market_data(market_data));

            if let Some(q_map) = &ctx.quant_data_map {
                if let Some(quant_data) = q_map.get(&pos.symbol) {
                    sb.push_str(&self.format_quant_data(quant_data));
                }
            }
            writeln!(sb).unwrap();
        }

        sb
    }

    fn format_coin_source_tag(&self, sources: &[String]) -> &str {
        if sources.len() > 1 {
            return " (AI500+OI_Top dual signal)";
        } else if sources.len() == 1 {
            match sources[0].as_str() {
                "ai500" => return " (AI500)",
                "oi_top" => return " (OI_Top position growth)",
                "static" => return " (Manual selection)",
                _ => {}
            }
        }
        ""
    }

    fn format_market_data(&self, data: &Data) -> String {
        let mut sb = String::new();
        let indicators = &self.config.indicators;

        write!(sb, "current_price = {:.4}", data.current_price).unwrap();

        if indicators.enable_ema {
            write!(sb, ", current_ema20 = {:.3}", data.current_ema20).unwrap();
        }
        if indicators.enable_macd {
            write!(sb, ", current_macd = {:.3}", data.current_macd).unwrap();
        }
        if indicators.enable_rsi {
            write!(sb, ", current_rsi7 = {:.3}", data.current_rsi7).unwrap();
        }
        writeln!(sb, "\n").unwrap();

        if indicators.enable_oi || indicators.enable_funding_rate {
            writeln!(sb, "Additional data for {}:\n", data.symbol).unwrap();

            if indicators.enable_oi {
                if let Some(oi) = &data.open_interest {
                    writeln!(
                        sb,
                        "Open Interest: Latest: {:.2} Average: {:.2}\n",
                        oi.latest, oi.average
                    )
                    .unwrap();
                }
            }
            if indicators.enable_funding_rate {
                writeln!(sb, "Funding Rate: {:.2e}\n", data.funding_rate).unwrap();
            }
        }

        if let Some(timeframe_data) = &data.timeframe_data {
            let timeframe_order = [
                "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d",
                "1w",
            ];
            for tf in timeframe_order {
                if let Some(tf_data) = timeframe_data.get(tf) {
                    writeln!(
                        sb,
                        "=== {} Timeframe (oldest → latest) ===\n",
                        tf.to_uppercase()
                    )
                    .unwrap();
                    self.format_timeframe_series_data(&mut sb, tf_data, indicators);
                }
            }
        } else {
            // Intraday
            if let Some(intraday) = &data.intraday_series {
                let kline_config = &indicators.klines;
                writeln!(
                    sb,
                    "Intraday series ({} intervals, oldest → latest):\n",
                    kline_config.primary_timeframe
                )
                .unwrap();

                if !intraday.mid_prices.is_empty() {
                    writeln!(
                        sb,
                        "Mid prices: {}\n",
                        Self::format_float_slice(&intraday.mid_prices)
                    )
                    .unwrap();
                }
                if indicators.enable_ema && !intraday.ema20_values.is_empty() {
                    writeln!(
                        sb,
                        "EMA indicators (20-period): {}\n",
                        Self::format_float_slice(&intraday.ema20_values)
                    )
                    .unwrap();
                }
                if indicators.enable_macd && !intraday.macd_values.is_empty() {
                    writeln!(
                        sb,
                        "MACD indicators: {}\n",
                        Self::format_float_slice(&intraday.macd_values)
                    )
                    .unwrap();
                }
                if indicators.enable_rsi {
                    if !intraday.rsi7_values.is_empty() {
                        writeln!(
                            sb,
                            "RSI indicators (7-Period): {}\n",
                            Self::format_float_slice(&intraday.rsi7_values)
                        )
                        .unwrap();
                    }
                    if !intraday.rsi14_values.is_empty() {
                        writeln!(
                            sb,
                            "RSI indicators (14-Period): {}\n",
                            Self::format_float_slice(&intraday.rsi14_values)
                        )
                        .unwrap();
                    }
                }
                if indicators.enable_volume && !intraday.volume.is_empty() {
                    writeln!(
                        sb,
                        "Volume: {}\n",
                        Self::format_float_slice(&intraday.volume)
                    )
                    .unwrap();
                }
                if indicators.enable_atr {
                    writeln!(sb, "3m ATR (14-period): {:.3}\n", intraday.atr14).unwrap();
                }
            }

            // Longer term
            if let Some(lt_context) = &data.longer_term_context {
                if indicators.klines.enable_multi_timeframe {
                    writeln!(
                        sb,
                        "Longer-term context ({} timeframe):\n",
                        indicators.klines.longer_timeframe
                    )
                    .unwrap();

                    if indicators.enable_ema {
                        writeln!(
                            sb,
                            "20-Period EMA: {:.3} vs. 50-Period EMA: {:.3}\n",
                            lt_context.ema20, lt_context.ema50
                        )
                        .unwrap();
                    }
                    if indicators.enable_atr {
                        writeln!(
                            sb,
                            "3-Period ATR: {:.3} vs. 14-Period ATR: {:.3}\n",
                            lt_context.atr3, lt_context.atr14
                        )
                        .unwrap();
                    }
                    if indicators.enable_volume {
                        writeln!(
                            sb,
                            "Current Volume: {:.3} vs. Average Volume: {:.3}\n",
                            lt_context.current_volume, lt_context.average_volume
                        )
                        .unwrap();
                    }
                    if indicators.enable_macd && !lt_context.macd_values.is_empty() {
                        writeln!(
                            sb,
                            "MACD indicators: {}\n",
                            Self::format_float_slice(&lt_context.macd_values)
                        )
                        .unwrap();
                    }
                    if indicators.enable_rsi && !lt_context.rsi14_values.is_empty() {
                        writeln!(
                            sb,
                            "RSI indicators (14-Period): {}\n",
                            Self::format_float_slice(&lt_context.rsi14_values)
                        )
                        .unwrap();
                    }
                }
            }
        }

        sb
    }

    fn format_timeframe_series_data(
        &self,
        sb: &mut String,
        data: &TimeframeSeriesData,
        indicators: &IndicatorConfig,
    ) {
        if !data.mid_prices.is_empty() {
            writeln!(
                sb,
                "Mid prices: {}\n",
                Self::format_float_slice(&data.mid_prices)
            )
            .unwrap();
        }

        if indicators.enable_ema {
            if !data.ema20_values.is_empty() {
                writeln!(
                    sb,
                    "EMA indicators (20-period): {}\n",
                    Self::format_float_slice(&data.ema20_values)
                )
                .unwrap();
            }
            if !data.ema50_values.is_empty() {
                writeln!(
                    sb,
                    "EMA indicators (50-period): {}\n",
                    Self::format_float_slice(&data.ema50_values)
                )
                .unwrap();
            }
        }

        if indicators.enable_macd && !data.macd_values.is_empty() {
            writeln!(
                sb,
                "MACD indicators: {}\n",
                Self::format_float_slice(&data.macd_values)
            )
            .unwrap();
        }

        if indicators.enable_rsi {
            if !data.rsi7_values.is_empty() {
                writeln!(
                    sb,
                    "RSI indicators (7-Period): {}\n",
                    Self::format_float_slice(&data.rsi7_values)
                )
                .unwrap();
            }
            if !data.rsi14_values.is_empty() {
                writeln!(
                    sb,
                    "RSI indicators (14-Period): {}\n",
                    Self::format_float_slice(&data.rsi14_values)
                )
                .unwrap();
            }
        }

        if indicators.enable_volume && !data.volume.is_empty() {
            writeln!(sb, "Volume: {}\n", Self::format_float_slice(&data.volume)).unwrap();
        }

        if indicators.enable_atr {
            writeln!(sb, "ATR (14-period): {:.3}\n", data.atr14).unwrap();
        }
    }

    fn format_float_slice(values: &[f64]) -> String {
        let strs: Vec<String> = values.iter().map(|v| format!("{:.4}", v)).collect();
        format!("[{}]", strs.join(", "))
    }

    pub fn build_system_prompt(&self, account_equity: f64, variant: &str) -> String {
        let mut sb = String::new();
        let risk_control = &self.config.risk_control;
        let prompt_sections = &self.config.prompt_sections;

        // 1. Role definition
        if !prompt_sections.role_definition.is_empty() {
            writeln!(sb, "{}\n", prompt_sections.role_definition).unwrap();
        } else {
            writeln!(sb, "# You are a professional cryptocurrency trading AI\n").unwrap();
            writeln!(
                sb,
                "Your task is to make trading decisions based on provided market data.\n"
            )
            .unwrap();
        }

        // 2. Trading mode
        match variant.trim().to_lowercase().as_str() {
            "aggressive" => {
                writeln!(sb, "## Mode: Aggressive\n- Prioritize capturing trend breakouts, can build positions in batches when confidence ≥ 70\n- Allow higher positions, but must strictly set stop-loss and explain risk-reward ratio\n").unwrap();
            }
            "conservative" => {
                writeln!(sb, "## Mode: Conservative\n- Only open positions when multiple signals resonate\n- Prioritize cash preservation, must pause for multiple periods after consecutive losses\n").unwrap();
            }
            "scalping" => {
                writeln!(sb, "## Mode: Scalping\n- Focus on short-term momentum, smaller profit targets but require quick action\n- If price doesn't move as expected within two bars, immediately reduce position or stop-loss\n").unwrap();
            }
            _ => {}
        }

        // 3. Hard constraints
        writeln!(sb, "# Hard Constraints (Risk Control)\n").unwrap();
        writeln!(
            sb,
            "1. Risk-Reward Ratio: Must be ≥ 1:{:.1}",
            risk_control.min_risk_reward_ratio
        )
        .unwrap();
        writeln!(
            sb,
            "2. Max Positions: {} coins (quality > quantity)",
            risk_control.max_positions
        )
        .unwrap();
        writeln!(
            sb,
            "3. Single Coin Position: Altcoins {:.0}-{:.0} U | BTC/ETH {:.0}-{:.0} U",
            account_equity * 0.8,
            account_equity * risk_control.max_position_ratio,
            account_equity * 5.0,
            account_equity * 10.0
        )
        .unwrap();
        writeln!(
            sb,
            "4. Leverage Limits: **Altcoins max {}x leverage** | **BTC/ETH max {}x leverage**",
            risk_control.altcoin_max_leverage, risk_control.btc_eth_max_leverage
        )
        .unwrap();
        writeln!(
            sb,
            "5. Margin Usage ≤ {:.0}%",
            risk_control.max_margin_usage * 100.0
        )
        .unwrap();
        writeln!(
            sb,
            "6. Opening Amount: Recommended ≥{:.0} USDT",
            risk_control.min_position_size
        )
        .unwrap();
        writeln!(
            sb,
            "7. Minimum Confidence: ≥{}\n",
            risk_control.min_confidence
        )
        .unwrap();

        // 4. Trading frequency
        if !prompt_sections.trading_frequency.is_empty() {
            writeln!(sb, "{}\n", prompt_sections.trading_frequency).unwrap();
        } else {
            writeln!(sb, "# ⏱️ Trading Frequency Awareness\n").unwrap();
            writeln!(
                sb,
                "- Excellent traders: 2-4 trades/day ≈ 0.1-0.2 trades/hour"
            )
            .unwrap();
            writeln!(sb, "- >2 trades/hour = Overtrading").unwrap();
            writeln!(sb, "- Single position hold time ≥ 30-60 minutes").unwrap();
            writeln!(sb, "If you find yourself trading every period → standards too low; if closing positions < 30 minutes → too impatient.\n").unwrap();
        }

        // 5. Entry standards
        if !prompt_sections.entry_standards.is_empty() {
            writeln!(
                sb,
                "{}\n\nYou have the following indicator data:",
                prompt_sections.entry_standards
            )
            .unwrap();
            self.write_available_indicators(&mut sb);
            writeln!(
                sb,
                "\n**Confidence ≥ {}** required to open positions.\n",
                risk_control.min_confidence
            )
            .unwrap();
        } else {
            writeln!(sb, "# 🎯 Entry Standards (Strict)\n").unwrap();
            writeln!(
                sb,
                "Only open positions when multiple signals resonate. You have:"
            )
            .unwrap();
            self.write_available_indicators(&mut sb);
            writeln!(sb, "\nFeel free to use any effective analysis method, but **confidence ≥ {}** required to open positions; avoid low-quality behaviors such as single indicators, contradictory signals, sideways consolidation, reopening immediately after closing, etc.\n", risk_control.min_confidence).unwrap();
        }

        // 6. Decision process
        if !prompt_sections.decision_process.is_empty() {
            writeln!(sb, "{}\n", prompt_sections.decision_process).unwrap();
        } else {
            writeln!(sb, "# 📋 Decision Process\n").unwrap();
            writeln!(sb, "1. Check positions → Should we take profit/stop-loss").unwrap();
            writeln!(
                sb,
                "2. Scan candidate coins + multi-timeframe → Are there strong signals"
            )
            .unwrap();
            writeln!(
                sb,
                "3. Write chain of thought first, then output structured JSON\n"
            )
            .unwrap();
        }

        // 7. Output format
        writeln!(sb, "# Output Format (Strictly Follow)\n").unwrap();
        writeln!(sb, "**Must use XML tags <reasoning> and <decision> to separate chain of thought and decision JSON, avoiding parsing errors**\n").unwrap();
        writeln!(sb, "## Format Requirements\n").unwrap();
        writeln!(sb, "<reasoning>\nYour chain of thought analysis...\n- Briefly analyze your thinking process \n</reasoning>\n").unwrap();
        writeln!(sb, "<decision>\nStep 2: JSON decision array\n\n```json\n[").unwrap();
        writeln!(sb, "  {{\"symbol\": \"BTCUSDT\", \"action\": \"open_short\", \"leverage\": {}, \"position_size_usd\": {:.0}, \"stop_loss\": 97000, \"take_profit\": 91000, \"confidence\": 85, \"risk_usd\": 300}},",
            risk_control.btc_eth_max_leverage, account_equity * 5.0).unwrap();
        writeln!(
            sb,
            "  {{\"symbol\": \"ETHUSDT\", \"action\": \"close_long\"}}\n]\n```\n</decision>\n"
        )
        .unwrap();
        writeln!(sb, "## Field Description\n").unwrap();
        writeln!(
            sb,
            "- `action`: open_long | open_short | close_long | close_short | hold | wait"
        )
        .unwrap();
        writeln!(
            sb,
            "- `confidence`: 0-100 (opening recommended ≥ {})",
            risk_control.min_confidence
        )
        .unwrap();
        writeln!(sb, "- Required when opening: leverage, position_size_usd, stop_loss, take_profit, confidence, risk_usd\n").unwrap();

        // 8. Custom Prompt
        if !self.config.custom_prompt.is_empty() {
            writeln!(sb, "# 📌 Personalized Trading Strategy\n").unwrap();
            writeln!(sb, "{}\n", self.config.custom_prompt).unwrap();
            writeln!(sb, "Note: The above personalized strategy is a supplement to the basic rules and cannot violate the basic risk control principles.").unwrap();
        }

        sb
    }

    fn write_available_indicators(&self, sb: &mut String) {
        let indicators = &self.config.indicators;
        let kline = &indicators.klines;

        write!(sb, "- {} price series", kline.primary_timeframe).unwrap();
        if kline.enable_multi_timeframe {
            writeln!(sb, " + {} K-line series", kline.longer_timeframe).unwrap();
        } else {
            writeln!(sb).unwrap();
        }

        if indicators.enable_ema {
            write!(sb, "- EMA indicators").unwrap();
            if !indicators.ema_periods.is_empty() {
                write!(sb, " (periods: {:?})", indicators.ema_periods).unwrap();
            }
            writeln!(sb).unwrap();
        }

        if indicators.enable_macd {
            writeln!(sb, "- MACD indicators").unwrap();
        }

        if indicators.enable_rsi {
            write!(sb, "- RSI indicators").unwrap();
            if !indicators.rsi_periods.is_empty() {
                write!(sb, " (periods: {:?})", indicators.rsi_periods).unwrap();
            }
            writeln!(sb).unwrap();
        }

        if indicators.enable_atr {
            write!(sb, "- ATR indicators").unwrap();
            if !indicators.atr_periods.is_empty() {
                write!(sb, " (periods: {:?})", indicators.atr_periods).unwrap();
            }
            writeln!(sb).unwrap();
        }

        if indicators.enable_volume {
            writeln!(sb, "- Volume data").unwrap();
        }

        if indicators.enable_oi {
            writeln!(sb, "- Open Interest (OI) data").unwrap();
        }

        if indicators.enable_funding_rate {
            writeln!(sb, "- Funding rate").unwrap();
        }

        if !self.config.coin_source.static_coins.is_empty()
            || self.config.coin_source.use_coin_pool
            || self.config.coin_source.use_oi_top
        {
            writeln!(sb, "- AI500 / OI_Top filter tags (if available)").unwrap();
        }

        if indicators.enable_quant_data {
            writeln!(sb, "- Quantitative data (institutional/retail fund flow, position changes, multi-period price changes)").unwrap();
        }
    }

    pub fn get_risk_control_config(&self) -> RiskControlConfig {
        self.config.risk_control.clone()
    }

    pub fn get_config(&self) -> StrategyConfig {
        self.config.clone()
    }
}
