use std::time::Duration;

use super::data::normalize;
use super::timeframe::normalize_timeframe;
use super::types::Kline;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::Value;

const BINANCE_FUTURES_KLINES_URL: &str = "https://fapi.binance.com/fapi/v1/klines";
const BINANCE_MAX_KLINE_LIMIT: usize = 1500;

/// Fetches K-line series within specified time range (closed interval).
/// Returns data sorted by time in ascending order.
pub async fn get_klines_range(
    symbol: &str,
    timeframe: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Kline>> {
    let symbol = normalize(symbol);
    let norm_tf = normalize_timeframe(timeframe)?;

    if end <= start {
        return Err(anyhow!("end time must be after start time"));
    }

    let start_ms = start.timestamp_millis();
    let end_ms = end.timestamp_millis();

    let mut all_klines: Vec<Kline> = Vec::new();
    let mut cursor = start_ms;

    let client = Client::builder().timeout(Duration::from_secs(15)).build()?;

    while cursor < end_ms {
        let params = [
            ("symbol", symbol.as_str()),
            ("interval", norm_tf.as_str()),
            ("limit", &BINANCE_MAX_KLINE_LIMIT.to_string()),
            ("startTime", &cursor.to_string()),
            ("endTime", &end_ms.to_string()),
        ];

        let response = client
            .get(BINANCE_FUTURES_KLINES_URL)
            .query(&params)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response.text().await?;
            return Err(anyhow!(
                "binance klines api returned status {}: {}",
                status,
                error_text
            ));
        }

        // Binance returns Vec<Vec<Value>> (mixed types: numbers and strings)
        let raw: Vec<Vec<Value>> = response.json().await?;

        if raw.is_empty() {
            break;
        }

        let batch_len = raw.len();
        let mut batch: Vec<Kline> = Vec::with_capacity(batch_len);

        for item in raw {
            // Helper closure to handle extracting float from Value (which could be string or number)
            let parse_float = |v: &Value| -> f64 {
                if let Some(n) = v.as_f64() {
                    n
                } else if let Some(s) = v.as_str() {
                    s.parse::<f64>().unwrap_or(0.0)
                } else {
                    0.0
                }
            };

            // Helper to extract i64 (timestamps)
            let parse_i64 = |v: &Value| -> i64 { if let Some(n) = v.as_i64() { n } else { 0 } };

            // Binance format: [OpenTime, Open, High, Low, Close, Volume, CloseTime, ...]
            // Index 0: Open time
            // Index 1: Open
            // Index 2: High
            // Index 3: Low
            // Index 4: Close
            // Index 5: Volume
            // Index 6: Close time
            if item.len() < 7 {
                continue;
            }

            let kline = Kline {
                open_time: parse_i64(&item[0]),
                open: parse_float(&item[1]),
                high: parse_float(&item[2]),
                low: parse_float(&item[3]),
                close: parse_float(&item[4]),
                volume: parse_float(&item[5]),
                close_time: parse_i64(&item[6]),
                ..Default::default()
            };

            batch.push(kline);
        }

        if let Some(last) = batch.last() {
            cursor = last.close_time + 1;
        }

        all_klines.extend(batch);

        // If returned quantity is less than request limit, reached the end.
        if batch_len < BINANCE_MAX_KLINE_LIMIT {
            break;
        }
    }

    Ok(all_klines)
}
