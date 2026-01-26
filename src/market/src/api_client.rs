use std::time::Duration;

use crate::types::{ExchangeInfo, Kline, PriceTicker};
use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use serde_json::Value; // You need to ensure this struct derives Deserialize

const BASE_URL: &str = "https://fapi.binance.com";

#[derive(Debug, Clone)]
pub struct APIClient {
    client: Client,
}

impl APIClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        // Logic to use your custom hook (commented out to make code compile for checking, uncomment in your real project)
        /*
        match hook_exec::<SetHttpClientResult>(SET_HTTP_CLIENT, args!(client.clone())) {
            Some(hook_res) => {
                info!("Using HTTP client set by Hook");
                if let Ok(c) = hook_res.get_result() {
                    client = c;
                }
            }
            _ => {}
        }
        */

        Self { client }
    }

    pub async fn get_exchange_info(&self) -> Result<ExchangeInfo> {
        let url = format!("{}/fapi/v1/exchangeInfo", BASE_URL);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send request for exchange info")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Exchange info failed {}: {}", status, text));
        }

        // Optimization: Deserialize directly to the struct
        let einfo: ExchangeInfo = resp
            .json()
            .await
            .context("Failed to deserialize ExchangeInfo")?;

        Ok(einfo)
    }

    pub async fn get_klines(&self, symbol: &str, interval: &str, limit: i32) -> Result<Vec<Kline>> {
        let url = format!("{}/fapi/v1/klines", BASE_URL);

        let params = [
            ("symbol", symbol),
            ("interval", interval),
            ("limit", &limit.to_string()),
        ];

        let resp = self
            .client
            .get(&url)
            .query(&params)
            .send()
            .await
            .context("Failed to send request for klines")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Get klines failed {}: {}", status, text));
        }

        // Binance Klines are returned as Vec<Vec<Value>> (Mixed types)
        let raw_klines: Vec<Vec<Value>> = resp
            .json()
            .await
            .context("Failed to deserialize raw kline data")?;

        let mut klines = Vec::with_capacity(raw_klines.len());

        for (i, raw) in raw_klines.into_iter().enumerate() {
            match parse_kline(raw) {
                Ok(k) => klines.push(k),
                Err(e) => {
                    // Log error but continue, similar to the Go "continue" logic
                    eprintln!("Failed to parse K-line at index {}: {}", i, e);
                    continue;
                }
            }
        }

        Ok(klines)
    }

    pub async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let url = format!("{}/fapi/v1/ticker/price", BASE_URL);

        let resp = self
            .client
            .get(&url)
            .query(&[("symbol", symbol)])
            .send()
            .await
            .context("Failed to send request for ticker price")?;

        if !resp.status().is_success() {
            return Err(anyhow!("Get price failed: {}", resp.status()));
        }

        let ticker: PriceTicker = resp
            .json()
            .await
            .context("Failed to deserialize PriceTicker")?;

        let price = ticker
            .price
            .parse::<f64>()
            .context("Failed to parse price string to float")?;

        Ok(price)
    }
}

// Helper function to convert the heterogeneous JSON array to a Kline struct
fn parse_kline(data: Vec<Value>) -> Result<Kline> {
    if data.len() < 11 {
        return Err(anyhow!("Invalid kline data length"));
    }

    // Helper closure to get string and parse to f64
    let get_float_from_str = |idx: usize| -> Result<f64> {
        data.get(idx)
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Field {} is not a string", idx))?
            .parse::<f64>()
            .map_err(|e| anyhow!("Field {} parse error: {}", idx, e))
    };

    // Helper closure to get number (u64/f64) and convert to i64
    let get_int = |idx: usize| -> Result<i64> {
        let val = data
            .get(idx)
            .ok_or_else(|| anyhow!("Missing field {}", idx))?;
        if let Some(i) = val.as_i64() {
            Ok(i)
        } else if let Some(f) = val.as_f64() {
            Ok(f as i64)
        } else {
            Err(anyhow!("Field {} is not a number", idx))
        }
    };

    // Helper closure to get trades count (usually int or float in JSON)
    let get_trades = |idx: usize| -> Result<i64> {
        let val = data
            .get(idx)
            .ok_or_else(|| anyhow!("Missing field {}", idx))?;
        // In Go code: int(kr[8].(float64))
        if let Some(f) = val.as_f64() {
            Ok(f as i64)
        } else if let Some(i) = val.as_i64() {
            Ok(i)
        } else {
            Err(anyhow!("Field {} is not a number", idx))
        }
    };

    Ok(Kline {
        open_time: get_int(0)?,
        open: get_float_from_str(1)?,
        high: get_float_from_str(2)?,
        low: get_float_from_str(3)?,
        close: get_float_from_str(4)?,
        volume: get_float_from_str(5)?,
        close_time: get_int(6)?,
        quote_volume: get_float_from_str(7)?,
        trades: get_trades(8)?,
        taker_buy_base_volume: get_float_from_str(9)?,
        taker_buy_quote_volume: get_float_from_str(10)?,
    })
}
