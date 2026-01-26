use std::fmt::Write;
use std::time::{Duration, Instant};

use super::types::{Data, IntradayData, Kline, LongerTermData, OIData, TimeframeSeriesData};
use crate::monitor::WSMonitor;
use anyhow::{Result, anyhow};
use chrono::Duration as ChronoDuration;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use reqwest::Client;
use tracing::{instrument, warn};

struct FundingRateCache {
    rate: f64,
    updated_at: Instant,
}

static FUNDING_RATE_MAP: Lazy<DashMap<String, FundingRateCache>> = Lazy::new(DashMap::new);
const FR_CACHE_TTL: Duration = Duration::from_secs(3600); // 1 Hour

/// Get retrieves market data for the specified token
#[instrument]
pub async fn get(symbol: &str) -> Result<Data> {
    let symbol = normalize(symbol);
    let wsmonitor_cli = WSMonitor::new(5 as usize);

    // Get 3-minute K-line data (latest 10+)
    let klines3m = wsmonitor_cli
        .get_current_klines(&symbol, "3m")
        .await
        .map_err(|e| anyhow!("Failed to get 3-minute K-line: {}", e))?;

    // Data staleness detection
    if is_stale_data(&klines3m, &symbol) {
        warn!(
            "⚠️  WARNING: {} detected stale data (consecutive price freeze), skipping symbol",
            symbol
        );
        return Err(anyhow!("{} data is stale, possible cache failure", symbol));
    }

    // Get 4-hour K-line data
    let klines4h = wsmonitor_cli
        .get_current_klines(&symbol, "4h")
        .await
        .map_err(|e| anyhow!("Failed to get 4-hour K-line: {}", e))?;

    if klines3m.is_empty() {
        return Err(anyhow!("3-minute K-line data is empty"));
    }
    if klines4h.is_empty() {
        return Err(anyhow!("4-hour K-line data is empty"));
    }

    // Calculate current indicators (based on 3-minute latest data)
    let current_price = klines3m.last().unwrap().close;
    let current_ema20 = calculate_ema(&klines3m, 20);
    let current_macd = calculate_macd(&klines3m);
    let current_rsi7 = calculate_rsi(&klines3m, 7);

    // Calculate price change percentage
    // 1-hour price change = price from 20 3-minute K-lines ago
    let mut price_change_1h = 0.0;
    if klines3m.len() >= 21 {
        let price_1h_ago = klines3m[klines3m.len() - 21].close;
        if price_1h_ago > 0.0 {
            price_change_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100.0;
        }
    }

    // 4-hour price change = price from 1 4-hour K-line ago
    let mut price_change_4h = 0.0;
    if klines4h.len() >= 2 {
        let price_4h_ago = klines4h[klines4h.len() - 2].close;
        if price_4h_ago > 0.0 {
            price_change_4h = ((current_price - price_4h_ago) / price_4h_ago) * 100.0;
        }
    }

    // Get OI data
    let open_interest = get_open_interest_data(&symbol).await.ok(); // Convert Err to None

    // Get Funding Rate
    let funding_rate = get_funding_rate(&symbol).await.unwrap_or(0.0);

    // Calculate intraday series data
    let intraday_series = calculate_intraday_series(&klines3m);

    // Calculate longer-term data
    let longer_term_context = calculate_longer_term_data(&klines4h);

    Ok(Data {
        symbol,
        current_price,
        price_change_1h,
        price_change_4h,
        current_ema20,
        current_macd,
        current_rsi7,
        open_interest,
        funding_rate,
        intraday_series: Some(intraday_series),
        longer_term_context: Some(longer_term_context),
        timeframe_data: None,
    })
}

/// get_with_timeframes retrieves market data for specified multiple timeframes
#[instrument]
pub async fn get_with_timeframes(
    symbol: &str,
    timeframes: &mut Vec<String>,
    primary_timeframe: Option<String>,
) -> Result<Data> {
    let symbol = normalize(symbol);

    if timeframes.is_empty() {
        return Err(anyhow!("at least one timeframe is required"));
    }

    // If primary timeframe is not specified, use the first one
    let primary_tf = primary_timeframe.unwrap_or_else(|| timeframes[0].clone());

    // Ensure primary timeframe is in the list
    if !timeframes.contains(&primary_tf) {
        timeframes.insert(0, primary_tf.clone());
    }

    let mut timeframe_data = std::collections::HashMap::new();
    let mut primary_klines: Option<Vec<Kline>> = None;
    let wsmonitor_cli = WSMonitor::new(5 as usize);
    // Get K-line data for each timeframe
    for tf in timeframes {
        match wsmonitor_cli.get_current_klines(&symbol, tf).await {
            Ok(klines) => {
                if klines.is_empty() {
                    warn!("⚠️ {} {} K-line data is empty", symbol, tf);
                    continue;
                }

                // Save primary timeframe K-lines
                if *tf == primary_tf {
                    primary_klines = Some(klines.clone());
                }

                let series_data = calculate_timeframe_series(&klines, tf);
                timeframe_data.insert(tf.clone(), series_data);
            }
            Err(e) => {
                warn!("⚠️ Failed to get {} {} K-line: {}", symbol, tf, e);
                continue;
            }
        }
    }

    let primary_klines = primary_klines
        .ok_or_else(|| anyhow!("Primary timeframe {} K-line data is empty", primary_tf))?;

    // Data staleness detection
    if is_stale_data(&primary_klines, &symbol) {
        warn!(
            "⚠️  WARNING: {} detected stale data (consecutive price freeze), skipping symbol",
            symbol
        );
        return Err(anyhow!("{} data is stale, possible cache failure", symbol));
    }

    // Calculate current indicators
    let current_price = primary_klines.last().unwrap().close;
    let current_ema20 = calculate_ema(&primary_klines, 20);
    let current_macd = calculate_macd(&primary_klines);
    let current_rsi7 = calculate_rsi(&primary_klines, 7);

    // Calculate price changes
    let price_change_1h = calculate_price_change_by_bars(&primary_klines, &primary_tf, 60);
    let price_change_4h = calculate_price_change_by_bars(&primary_klines, &primary_tf, 240);

    let open_interest = get_open_interest_data(&symbol).await.ok();
    let funding_rate = get_funding_rate(&symbol).await.unwrap_or(0.0);

    Ok(Data {
        symbol,
        current_price,
        price_change_1h,
        price_change_4h,
        current_ema20,
        current_macd,
        current_rsi7,
        open_interest,
        funding_rate,
        intraday_series: None,
        longer_term_context: None,
        timeframe_data: Some(timeframe_data),
    })
}

fn calculate_timeframe_series(klines: &[Kline], timeframe: &str) -> TimeframeSeriesData {
    let mut data = TimeframeSeriesData {
        timeframe: timeframe.to_string(),
        mid_prices: Vec::new(),
        ema20_values: Vec::new(),
        ema50_values: Vec::new(),
        macd_values: Vec::new(),
        rsi7_values: Vec::new(),
        rsi14_values: Vec::new(),
        volume: Vec::new(),
        atr14: 0.0,
    };

    let start = if klines.len() > 10 {
        klines.len() - 10
    } else {
        0
    };

    for i in start..klines.len() {
        data.mid_prices.push(klines[i].close);
        data.volume.push(klines[i].volume);

        if i >= 19 {
            data.ema20_values.push(calculate_ema(&klines[..=i], 20));
        }
        if i >= 49 {
            data.ema50_values.push(calculate_ema(&klines[..=i], 50));
        }
        if i >= 25 {
            data.macd_values.push(calculate_macd(&klines[..=i]));
        }
        if i >= 7 {
            data.rsi7_values.push(calculate_rsi(&klines[..=i], 7));
        }
        if i >= 14 {
            data.rsi14_values.push(calculate_rsi(&klines[..=i], 14));
        }
    }

    data.atr14 = calculate_atr(klines, 14);
    data
}

fn calculate_price_change_by_bars(klines: &[Kline], timeframe: &str, target_minutes: i64) -> f64 {
    if klines.len() < 2 {
        return 0.0;
    }

    let tf_minutes = parse_timeframe_to_minutes(timeframe);
    if tf_minutes <= 0 {
        return 0.0;
    }

    let mut bars_back = target_minutes / tf_minutes;
    if bars_back < 1 {
        bars_back = 1;
    }

    let current_price = klines.last().unwrap().close;
    // Safely calculate index
    let idx = if klines.len() > (bars_back as usize + 1) {
        klines.len() - 1 - (bars_back as usize)
    } else {
        0
    };

    let old_price = klines[idx].close;
    if old_price > 0.0 {
        ((current_price - old_price) / old_price) * 100.0
    } else {
        0.0
    }
}

fn parse_timeframe_to_minutes(tf: &str) -> i64 {
    match tf {
        "1m" => 1,
        "3m" => 3,
        "5m" => 5,
        "15m" => 15,
        "30m" => 30,
        "1h" => 60,
        "2h" => 120,
        "4h" => 240,
        "6h" => 360,
        "8h" => 480,
        "12h" => 720,
        "1d" => 1440,
        "3d" => 4320,
        "1w" => 10080,
        _ => 0,
    }
}

fn calculate_ema(klines: &[Kline], period: usize) -> f64 {
    if klines.len() < period {
        return 0.0;
    }

    let sum: f64 = klines.iter().take(period).map(|k| k.close).sum();
    let mut ema = sum / period as f64;

    let multiplier = 2.0 / (period as f64 + 1.0);
    for k in klines.iter().skip(period) {
        ema = (k.close - ema) * multiplier + ema;
    }
    ema
}

fn calculate_macd(klines: &[Kline]) -> f64 {
    if klines.len() < 26 {
        return 0.0;
    }
    let ema12 = calculate_ema(klines, 12);
    let ema26 = calculate_ema(klines, 26);
    ema12 - ema26
}

fn calculate_rsi(klines: &[Kline], period: usize) -> f64 {
    if klines.len() <= period {
        return 0.0;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;

    // Initial average
    for i in 1..=period {
        let change = klines[i].close - klines[i - 1].close;
        if change > 0.0 {
            gains += change;
        } else {
            losses -= change;
        }
    }

    let mut avg_gain = gains / period as f64;
    let mut avg_loss = losses / period as f64;

    // Wilder smoothing
    for i in (period + 1)..klines.len() {
        let change = klines[i].close - klines[i - 1].close;
        if change > 0.0 {
            avg_gain = (avg_gain * (period as f64 - 1.0) + change) / period as f64;
            avg_loss = (avg_loss * (period as f64 - 1.0)) / period as f64;
        } else {
            avg_gain = (avg_gain * (period as f64 - 1.0)) / period as f64;
            avg_loss = (avg_loss * (period as f64 - 1.0) + (-change)) / period as f64;
        }
    }

    if avg_loss == 0.0 {
        return 100.0;
    }

    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

fn calculate_atr(klines: &[Kline], period: usize) -> f64 {
    if klines.len() <= period {
        return 0.0;
    }

    let mut trs = vec![0.0; klines.len()];
    for i in 1..klines.len() {
        let high = klines[i].high;
        let low = klines[i].low;
        let prev_close = klines[i - 1].close;

        let tr1 = high - low;
        let tr2 = (high - prev_close).abs();
        let tr3 = (low - prev_close).abs();

        trs[i] = tr1.max(tr2).max(tr3);
    }

    // Initial ATR
    let sum: f64 = trs.iter().skip(1).take(period).sum();
    let mut atr = sum / period as f64;

    // Wilder smoothing
    for val in trs.iter().skip(period + 1) {
        atr = (atr * (period as f64 - 1.0) + *val) / period as f64;
    }

    atr
}

fn calculate_intraday_series(klines: &[Kline]) -> IntradayData {
    let mut data = IntradayData {
        mid_prices: Vec::new(),
        ema20_values: Vec::new(),
        macd_values: Vec::new(),
        rsi7_values: Vec::new(),
        rsi14_values: Vec::new(),
        volume: Vec::new(),
        atr14: 0.0,
    };

    let start = if klines.len() > 10 {
        klines.len() - 10
    } else {
        0
    };

    for i in start..klines.len() {
        data.mid_prices.push(klines[i].close);
        data.volume.push(klines[i].volume);

        if i >= 19 {
            data.ema20_values.push(calculate_ema(&klines[..=i], 20));
        }
        if i >= 25 {
            data.macd_values.push(calculate_macd(&klines[..=i]));
        }
        if i >= 7 {
            data.rsi7_values.push(calculate_rsi(&klines[..=i], 7));
        }
        if i >= 14 {
            data.rsi14_values.push(calculate_rsi(&klines[..=i], 14));
        }
    }

    data.atr14 = calculate_atr(klines, 14);
    data
}

fn calculate_longer_term_data(klines: &[Kline]) -> LongerTermData {
    let mut data = LongerTermData {
        ema20: calculate_ema(klines, 20),
        ema50: calculate_ema(klines, 50),
        atr3: calculate_atr(klines, 3),
        atr14: calculate_atr(klines, 14),
        current_volume: 0.0,
        average_volume: 0.0,
        macd_values: Vec::new(),
        rsi14_values: Vec::new(),
    };

    if !klines.is_empty() {
        data.current_volume = klines.last().unwrap().volume;
        let sum: f64 = klines.iter().map(|k| k.volume).sum();
        data.average_volume = sum / klines.len() as f64;
    }

    let start = if klines.len() > 10 {
        klines.len() - 10
    } else {
        0
    };

    for i in start..klines.len() {
        if i >= 25 {
            data.macd_values.push(calculate_macd(&klines[..=i]));
        }
        if i >= 14 {
            data.rsi14_values.push(calculate_rsi(&klines[..=i], 14));
        }
    }

    data
}

async fn get_open_interest_data(symbol: &str) -> Result<OIData, String> {
    let url = format!(
        "https://fapi.binance.com/fapi/v1/openInterest?symbol={}",
        symbol
    );

    let client = Client::new();
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;

    #[derive(serde::Deserialize)]
    struct OIResponse {
        #[serde(rename = "openInterest")]
        open_interest: String,
    }

    let result: OIResponse = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
    let oi: f64 = result.open_interest.parse().unwrap_or(0.0);

    Ok(OIData {
        latest: oi,
        average: oi * 0.999, // Approximate average
    })
}

async fn get_funding_rate(symbol: &str) -> Result<f64, String> {
    // Check cache
    if let Some(cached) = FUNDING_RATE_MAP.get(symbol) {
        if cached.updated_at.elapsed() < FR_CACHE_TTL {
            return Ok(cached.rate);
        }
    }

    let url = format!(
        "https://fapi.binance.com/fapi/v1/premiumIndex?symbol={}",
        symbol
    );

    let client = Client::new();
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;

    #[derive(serde::Deserialize)]
    struct FundingResponse {
        #[serde(rename = "lastFundingRate")]
        last_funding_rate: String,
    }

    let result: FundingResponse = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
    let rate: f64 = result.last_funding_rate.parse().unwrap_or(0.0);

    // Update cache
    FUNDING_RATE_MAP.insert(
        symbol.to_string(),
        FundingRateCache {
            rate,
            updated_at: Instant::now(),
        },
    );

    Ok(rate)
}

pub fn format_data(data: &Data) -> String {
    let mut sb = String::new();

    let price_str = format_price_with_dynamic_precision(data.current_price);
    writeln!(
        sb,
        "current_price = {}, current_ema20 = {:.3}, current_macd = {:.3}, current_rsi (7 period) = {:.3}\n",
        price_str, data.current_ema20, data.current_macd, data.current_rsi7
    ).unwrap();

    writeln!(
        sb,
        "In addition, here is the latest {} open interest and funding rate for perps:\n",
        data.symbol
    )
    .unwrap();

    if let Some(oi) = &data.open_interest {
        let oi_latest = format_price_with_dynamic_precision(oi.latest);
        let oi_avg = format_price_with_dynamic_precision(oi.average);
        writeln!(
            sb,
            "Open Interest: Latest: {} Average: {}\n",
            oi_latest, oi_avg
        )
        .unwrap();
    }

    writeln!(sb, "Funding Rate: {:.2e}\n", data.funding_rate).unwrap();

    if let Some(series) = &data.intraday_series {
        sb.push_str("Intraday series (3‑minute intervals, oldest → latest):\n\n");
        if !series.mid_prices.is_empty() {
            writeln!(
                sb,
                "Mid prices: {}\n",
                format_float_slice(&series.mid_prices)
            )
            .unwrap();
        }
        if !series.ema20_values.is_empty() {
            writeln!(
                sb,
                "EMA indicators (20‑period): {}\n",
                format_float_slice(&series.ema20_values)
            )
            .unwrap();
        }
        if !series.macd_values.is_empty() {
            writeln!(
                sb,
                "MACD indicators: {}\n",
                format_float_slice(&series.macd_values)
            )
            .unwrap();
        }
        if !series.rsi7_values.is_empty() {
            writeln!(
                sb,
                "RSI indicators (7‑Period): {}\n",
                format_float_slice(&series.rsi7_values)
            )
            .unwrap();
        }
        if !series.rsi14_values.is_empty() {
            writeln!(
                sb,
                "RSI indicators (14‑Period): {}\n",
                format_float_slice(&series.rsi14_values)
            )
            .unwrap();
        }
        if !series.volume.is_empty() {
            writeln!(sb, "Volume: {}\n", format_float_slice(&series.volume)).unwrap();
        }
        writeln!(sb, "3m ATR (14‑period): {:.3}\n", series.atr14).unwrap();
    }

    if let Some(ctx) = &data.longer_term_context {
        sb.push_str("Longer‑term context (4‑hour timeframe):\n\n");
        writeln!(
            sb,
            "20‑Period EMA: {:.3} vs. 50‑Period EMA: {:.3}\n",
            ctx.ema20, ctx.ema50
        )
        .unwrap();
        writeln!(
            sb,
            "3‑Period ATR: {:.3} vs. 14‑Period ATR: {:.3}\n",
            ctx.atr3, ctx.atr14
        )
        .unwrap();
        writeln!(
            sb,
            "Current Volume: {:.3} vs. Average Volume: {:.3}\n",
            ctx.current_volume, ctx.average_volume
        )
        .unwrap();

        if !ctx.macd_values.is_empty() {
            writeln!(
                sb,
                "MACD indicators: {}\n",
                format_float_slice(&ctx.macd_values)
            )
            .unwrap();
        }
        if !ctx.rsi14_values.is_empty() {
            writeln!(
                sb,
                "RSI indicators (14‑Period): {}\n",
                format_float_slice(&ctx.rsi14_values)
            )
            .unwrap();
        }
    }

    if let Some(tf_data) = &data.timeframe_data {
        let timeframe_order = [
            "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
        ];
        for tf in timeframe_order {
            if let Some(d) = tf_data.get(tf) {
                writeln!(sb, "=== {} Timeframe ===\n", tf.to_uppercase()).unwrap();
                format_timeframe_data(&mut sb, d);
            }
        }
    }

    sb
}

fn format_timeframe_data(sb: &mut String, data: &TimeframeSeriesData) {
    if !data.mid_prices.is_empty() {
        writeln!(sb, "Mid prices: {}\n", format_float_slice(&data.mid_prices)).unwrap();
    }
    if !data.ema20_values.is_empty() {
        writeln!(
            sb,
            "EMA indicators (20‑period): {}\n",
            format_float_slice(&data.ema20_values)
        )
        .unwrap();
    }
    if !data.ema50_values.is_empty() {
        writeln!(
            sb,
            "EMA indicators (50‑period): {}\n",
            format_float_slice(&data.ema50_values)
        )
        .unwrap();
    }
    if !data.macd_values.is_empty() {
        writeln!(
            sb,
            "MACD indicators: {}\n",
            format_float_slice(&data.macd_values)
        )
        .unwrap();
    }
    if !data.rsi7_values.is_empty() {
        writeln!(
            sb,
            "RSI indicators (7‑Period): {}\n",
            format_float_slice(&data.rsi7_values)
        )
        .unwrap();
    }
    if !data.rsi14_values.is_empty() {
        writeln!(
            sb,
            "RSI indicators (14‑Period): {}\n",
            format_float_slice(&data.rsi14_values)
        )
        .unwrap();
    }
    if !data.volume.is_empty() {
        writeln!(sb, "Volume: {}\n", format_float_slice(&data.volume)).unwrap();
    }
    writeln!(sb, "ATR (14‑period): {:.3}\n", data.atr14).unwrap();
}

pub fn format_price_with_dynamic_precision(price: f64) -> String {
    if price < 0.0001 {
        format!("{:.8}", price)
    } else if price < 0.001 {
        format!("{:.6}", price)
    } else if price < 0.01 {
        format!("{:.6}", price)
    } else if price < 1.0 {
        format!("{:.4}", price)
    } else if price < 100.0 {
        format!("{:.4}", price)
    } else {
        format!("{:.2}", price)
    }
}

fn format_float_slice(values: &[f64]) -> String {
    let strs: Vec<String> = values
        .iter()
        .map(|v| format_price_with_dynamic_precision(*v))
        .collect();
    format!("[{}]", strs.join(", "))
}

pub fn normalize(symbol: &str) -> String {
    let mut s = symbol.to_uppercase();
    if !s.ends_with("USDT") {
        s.push_str("USDT");
    }
    s
}

pub fn build_data_from_klines(
    symbol: &str,
    primary: &[Kline],
    longer: Option<&[Kline]>,
) -> Result<Data> {
    if primary.is_empty() {
        return Err(anyhow!("primary series is empty"));
    }

    let symbol = normalize(symbol);
    let current = primary.last().unwrap();
    let current_price = current.close;

    let mut data = Data {
        symbol,
        current_price,
        current_ema20: calculate_ema(primary, 20),
        current_macd: calculate_macd(primary),
        current_rsi7: calculate_rsi(primary, 7),
        price_change_1h: price_change_from_series(primary, ChronoDuration::hours(1)),
        price_change_4h: price_change_from_series(primary, ChronoDuration::hours(4)),
        open_interest: Some(OIData {
            latest: 0.0,
            average: 0.0,
        }),
        funding_rate: 0.0,
        intraday_series: Some(calculate_intraday_series(primary)),
        longer_term_context: None,
        timeframe_data: None,
    };

    if let Some(longer) = longer {
        data.longer_term_context = Some(calculate_longer_term_data(longer));
    }

    Ok(data)
}

fn price_change_from_series(series: &[Kline], duration: ChronoDuration) -> f64 {
    if series.is_empty() {
        return 0.0;
    }
    let last = series.last().unwrap();
    // Assuming CloseTime is in milliseconds (i64)
    let target = last.close_time - duration.num_milliseconds();

    for k in series.iter().rev() {
        if k.close_time <= target {
            if k.close > 0.0 {
                return ((last.close - k.close) / k.close) * 100.0;
            }
            break;
        }
    }
    0.0
}

#[instrument]
fn is_stale_data(klines: &[Kline], symbol: &str) -> bool {
    if klines.len() < 5 {
        return false;
    }

    const STALE_PRICE_THRESHOLD: usize = 5;
    const PRICE_TOLERANCE_PCT: f64 = 0.0001;

    let recent_klines = &klines[klines.len() - STALE_PRICE_THRESHOLD..];
    let first_price = recent_klines[0].close;

    for k in recent_klines.iter().skip(1) {
        let price_diff = (k.close - first_price).abs() / first_price;
        if price_diff > PRICE_TOLERANCE_PCT {
            return false;
        }
    }

    let all_volume_zero = recent_klines.iter().all(|k| k.volume <= 0.0);

    if all_volume_zero {
        warn!(
            "⚠️  {} stale data confirmed: price freeze + zero volume",
            symbol
        );
        true
    } else {
        warn!(
            "⚠️  {} detected extreme price stability (no fluctuation for {} consecutive periods), but volume is normal",
            symbol, STALE_PRICE_THRESHOLD
        );
        false
    }
}
