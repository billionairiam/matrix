use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use super::Trader;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use ethers::abi::{self, Token};
use ethers::prelude::*;
use ethers::utils::keccak256;
use reqwest::{Client, Method};
use serde_json::{Map, Value};
use tokio::sync::RwLock;
use tracing::info;

const BASE_URL: &str = "https://fapi.asterdex.com";

/// Symbol precision information
#[derive(Debug, Clone, Copy, Default)]
pub struct SymbolPrecision {
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub tick_size: f64,
    pub step_size: f64,
}

/// Aster trading platform implementation
pub struct AsterTrader {
    user: Address,
    signer: Address,
    wallet: LocalWallet,
    client: Client,
    base_url: String,
    symbol_precision: Arc<RwLock<HashMap<String, SymbolPrecision>>>,
}

impl AsterTrader {
    /// Create Aster trader
    /// user: Main wallet address
    /// signer: API wallet address
    /// private_key_hex: API wallet private key
    pub fn new(user: &str, signer: &str, private_key_hex: &str) -> Result<Self> {
        let user_addr = user.parse::<Address>().context("Invalid user address")?;
        let signer_addr = signer
            .parse::<Address>()
            .context("Invalid signer address")?;

        let private_key_hex = private_key_hex.trim_start_matches("0x");
        let wallet = private_key_hex
            .parse::<LocalWallet>()
            .context("Invalid private key")?;

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()?;

        // Note: The hook logic from Go is omitted as it implies internal middleware.
        // If needed, configure the Client here.

        Ok(Self {
            user: user_addr,
            signer: signer_addr,
            wallet,
            client,
            base_url: BASE_URL.to_string(),
            symbol_precision: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Generate microsecond timestamp
    fn gen_nonce(&self) -> u64 {
        Utc::now().timestamp_micros() as u64
    }

    /// Get symbol precision information
    async fn get_precision(&self, symbol: &str) -> Result<SymbolPrecision> {
        {
            let cache = self.symbol_precision.read().await;
            if let Some(prec) = cache.get(symbol) {
                return Ok(*prec);
            }
        }

        // Get exchange information
        let url = format!("{}/fapi/v3/exchangeInfo", self.base_url);
        let resp = self.client.get(&url).send().await?.json::<Value>().await?;

        let symbols = resp["symbols"]
            .as_array()
            .ok_or_else(|| anyhow!("Invalid exchange info response"))?;

        let mut precisions = HashMap::new();

        for s in symbols {
            let sym_name = s["symbol"].as_str().unwrap_or_default().to_string();
            let mut prec = SymbolPrecision {
                price_precision: s["pricePrecision"].as_u64().unwrap_or(0) as u32,
                quantity_precision: s["quantityPrecision"].as_u64().unwrap_or(0) as u32,
                tick_size: 0.0,
                step_size: 0.0,
            };

            if let Some(filters) = s["filters"].as_array() {
                for filter in filters {
                    let f_type = filter["filterType"].as_str().unwrap_or("");
                    match f_type {
                        "PRICE_FILTER" => {
                            if let Some(ts) = filter["tickSize"].as_str() {
                                prec.tick_size = ts.parse().unwrap_or(0.0);
                            }
                        }
                        "LOT_SIZE" => {
                            if let Some(ss) = filter["stepSize"].as_str() {
                                prec.step_size = ss.parse().unwrap_or(0.0);
                            }
                        }
                        _ => {}
                    }
                }
            }
            precisions.insert(sym_name, prec);
        }

        let mut cache = self.symbol_precision.write().await;
        cache.extend(precisions);

        cache
            .get(symbol)
            .copied()
            .ok_or_else(|| anyhow!("Precision information not found for symbol {}", symbol))
    }

    /// Round price/quantity to the nearest multiple of tick size/step size
    fn round_to_tick_size(value: f64, tick_size: f64) -> f64 {
        if tick_size <= 0.0 {
            return value;
        }
        let steps = value / tick_size;
        steps.round() * tick_size
    }

    /// Format price to correct precision and tick size
    async fn format_price(&self, symbol: &str, price: f64) -> Result<f64> {
        let prec = self.get_precision(symbol).await?;

        if prec.tick_size > 0.0 {
            Ok(Self::round_to_tick_size(price, prec.tick_size))
        } else {
            let multiplier = 10f64.powi(prec.price_precision as i32);
            Ok((price * multiplier).round() / multiplier)
        }
    }

    /// Format float to string with specified precision (remove trailing zeros)
    fn format_float_with_precision(value: f64, precision: u32) -> String {
        let formatted = format!("{:.1$}", value, precision as usize);
        let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
        trimmed.to_string()
    }

    /// Recursively normalize parameters (sorted by key, all values converted to strings)
    fn normalize_params(&self, v: &Value) -> Value {
        match v {
            Value::Object(map) => {
                // BTreeMap automatically sorts keys
                let mut sorted_map = BTreeMap::new();
                for (k, val) in map {
                    sorted_map.insert(k.clone(), self.normalize_params(val));
                }
                // Convert back to serde_json::Value (will preserve order when stringified if using Map,
                // but strictly we want a sorted JSON string. Serde serializes maps in order if "preserve_order" isn't on,
                // but BTreeMap ensures key sorting)
                let mut out_map = Map::new();
                for (k, v) in sorted_map {
                    out_map.insert(k, v);
                }
                Value::Object(out_map)
            }
            Value::Array(arr) => {
                let mut out_arr = Vec::new();
                for item in arr {
                    out_arr.push(self.normalize_params(item));
                }
                Value::Array(out_arr)
            }
            Value::String(s) => Value::String(s.clone()),
            Value::Number(n) => Value::String(n.to_string()),
            Value::Bool(b) => Value::String(b.to_string()),
            Value::Null => Value::String("null".to_string()),
        }
    }

    /// Sign request parameters
    async fn sign(&self, params: &mut Map<String, Value>, nonce: u64) -> Result<()> {
        // Add timestamp and recvWindow
        params.insert("recvWindow".to_string(), Value::String("50000".to_string()));
        let timestamp = Utc::now().timestamp_millis().to_string();
        params.insert("timestamp".to_string(), Value::String(timestamp));

        // Normalize
        let value_wrapper = Value::Object(params.clone());
        let normalized = self.normalize_params(&value_wrapper);
        let json_str = serde_json::to_string(&normalized)?;

        // ABI encoding: (string, address, address, uint256)
        let tokens = vec![
            Token::String(json_str),
            Token::Address(self.user),
            Token::Address(self.signer),
            Token::Uint(U256::from(nonce)),
        ];

        let packed = abi::encode(&tokens);
        let hash = keccak256(&packed);

        // Sign hash (sign_hash handles the Ethereum Signed Message prefix internally in ethers-rs if using sign_message,
        // but here we manually construct the hash to match Go's crypto.Sign behavior on the pre-hashed message?
        // Go: `crypto.Sign(msgHash.Bytes(), t.privateKey)` where msgHash is Prefixed.
        // Ethers `sign_message` takes the raw message and prefixes it.
        // We have `hash` (Keccak(packed)). We need to sign Keccak(Prefix + hash).
        // `wallet.sign_message(H256::from(hash))` does exactly that.

        let signature = self.wallet.sign_message(H256::from(hash)).await?;

        // Ethers produces 'v' as 27/28 (EIP-155) or 0/1 depending on chainId.
        // But the Go code manually does `sig[64] += 27` implying the raw crypto.Sign gave 0/1.
        // `signature.v` in ethers is usually 27 or 28. Let's convert to 0x string.
        let sig_bytes = signature.to_vec();
        let sig_hex = format!("0x{}", hex::encode(sig_bytes));

        params.insert(
            "user".to_string(),
            Value::String(format!("{:?}", self.user)),
        );
        params.insert(
            "signer".to_string(),
            Value::String(format!("{:?}", self.signer)),
        );
        params.insert("signature".to_string(), Value::String(sig_hex));
        params.insert("nonce".to_string(), Value::String(nonce.to_string()));

        Ok(())
    }

    /// Send HTTP request with retry
    async fn request(
        &self,
        method: Method,
        endpoint: &str,
        params: Map<String, Value>,
    ) -> Result<Value> {
        const MAX_RETRIES: u32 = 3;
        let mut last_err = anyhow!("unknown error");

        for attempt in 1..=MAX_RETRIES {
            let nonce = self.gen_nonce();
            let mut current_params = params.clone();

            if let Err(e) = self.sign(&mut current_params, nonce).await {
                return Err(e);
            }

            match self
                .do_request(method.clone(), endpoint, &current_params)
                .await
            {
                Ok(body) => return Ok(body),
                Err(e) => {
                    last_err = e;
                    let err_str = last_err.to_string();
                    if err_str.contains("timeout")
                        || err_str.contains("connection reset")
                        || err_str.contains("EOF")
                    {
                        if attempt < MAX_RETRIES {
                            tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
                            continue;
                        }
                    }
                    // Don't retry logic errors
                    break;
                }
            }
        }

        Err(anyhow!(
            "request failed (retried {} times): {}",
            MAX_RETRIES,
            last_err
        ))
    }

    /// Execute actual HTTP request
    async fn do_request(
        &self,
        method: Method,
        endpoint: &str,
        params: &Map<String, Value>,
    ) -> Result<Value> {
        let full_url = format!("{}{}", self.base_url, endpoint);

        let resp = match method {
            Method::POST => {
                // Convert Map<String, Value> to Map<String, String> for form url encoded
                let mut form_params = HashMap::new();
                for (k, v) in params {
                    let s = match v {
                        Value::String(s) => s.clone(),
                        _ => v.to_string(),
                    };
                    form_params.insert(k, s);
                }

                self.client
                    .post(&full_url)
                    .form(&form_params)
                    .send()
                    .await?
            }
            Method::GET | Method::DELETE => {
                let mut query = Vec::new();
                for (k, v) in params {
                    let s = match v {
                        Value::String(s) => s.clone(),
                        _ => v.to_string(),
                    };
                    query.push((k, s));
                }

                self.client
                    .request(method, &full_url)
                    .query(&query)
                    .send()
                    .await?
            }
            _ => return Err(anyhow!("unsupported HTTP method")),
        };

        let status = resp.status();
        let body_text = resp.text().await?;

        if !status.is_success() {
            return Err(anyhow!("HTTP {}: {}", status, body_text));
        }

        let json_body: Value = serde_json::from_str(&body_text)
            .with_context(|| format!("Failed to parse response: {}", body_text))?;

        Ok(json_body)
    }

    /// Format quantity to correct precision and step size
    async fn format_quantity_f64(&self, symbol: &str, quantity: f64) -> Result<f64> {
        let prec = self.get_precision(symbol).await?;

        if prec.step_size > 0.0 {
            Ok(Self::round_to_tick_size(quantity, prec.step_size))
        } else {
            let multiplier = 10f64.powi(prec.quantity_precision as i32);
            Ok((quantity * multiplier).round() / multiplier)
        }
    }
}

#[async_trait]
impl Trader for AsterTrader {
    async fn get_balance(&self) -> Result<Map<String, Value>> {
        let params = Map::new();
        let body = self
            .request(Method::GET, "/fapi/v3/balance", params)
            .await?;

        let balances = body
            .as_array()
            .ok_or_else(|| anyhow!("Invalid balance format"))?;

        let mut available_balance = 0.0;
        let mut _cross_un_pnl = 0.0;
        let mut cross_wallet_balance = 0.0;
        let mut found_usdt = false;

        for bal in balances {
            if bal["asset"].as_str() == Some("USDT") {
                found_usdt = true;
                if let Some(s) = bal["availableBalance"].as_str() {
                    available_balance = s.parse().unwrap_or(0.0);
                }
                if let Some(s) = bal["crossUnPnl"].as_str() {
                    _cross_un_pnl = s.parse().unwrap_or(0.0);
                }
                if let Some(s) = bal["crossWalletBalance"].as_str() {
                    cross_wallet_balance = s.parse().unwrap_or(0.0);
                }
                break;
            }
        }

        if !found_usdt {
            info!("⚠️  USDT asset record not found!");
        }

        let positions = match self.get_positions().await {
            Ok(p) => p,
            Err(e) => {
                info!("⚠️  Failed to get position information: {}", e);
                let mut fallback = Map::new();
                fallback.insert("totalWalletBalance".into(), cross_wallet_balance.into());
                fallback.insert("availableBalance".into(), available_balance.into());
                fallback.insert("totalUnrealizedProfit".into(), _cross_un_pnl.into());
                return Ok(fallback);
            }
        };

        let mut total_margin_used = 0.0;
        let mut real_unrealized_pnl = 0.0;

        for pos in positions {
            let mark_price = pos["markPrice"].as_f64().unwrap_or(0.0);
            let mut quantity = pos["positionAmt"].as_f64().unwrap_or(0.0);
            if quantity < 0.0 {
                quantity = -quantity;
            }
            let unrealized_pnl = pos["unRealizedProfit"].as_f64().unwrap_or(0.0);
            real_unrealized_pnl += unrealized_pnl;

            let leverage = pos["leverage"].as_f64().unwrap_or(10.0);
            let margin_used = (quantity * mark_price) / leverage;
            total_margin_used += margin_used;
        }

        let total_equity = available_balance + total_margin_used;
        let total_wallet_balance = total_equity - real_unrealized_pnl;

        let mut res = Map::new();
        res.insert("totalWalletBalance".into(), total_wallet_balance.into());
        res.insert("availableBalance".into(), available_balance.into());
        res.insert("totalUnrealizedProfit".into(), real_unrealized_pnl.into());

        Ok(res)
    }

    async fn get_positions(&self) -> Result<Vec<Map<String, Value>>> {
        let params = Map::new();
        let body = self
            .request(Method::GET, "/fapi/v3/positionRisk", params)
            .await?;

        let positions_arr = body
            .as_array()
            .ok_or_else(|| anyhow!("Invalid position data"))?;
        let mut result = Vec::new();

        for pos in positions_arr {
            let pos_amt_str = match pos["positionAmt"].as_str() {
                Some(s) => s,
                None => continue,
            };

            let mut pos_amt: f64 = pos_amt_str.parse().unwrap_or(0.0);
            if pos_amt == 0.0 {
                continue;
            }

            let entry_price: f64 = pos["entryPrice"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
            let mark_price: f64 = pos["markPrice"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
            let un_pnl: f64 = pos["unRealizedProfit"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
            let leverage: f64 = pos["leverage"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
            let liq_price: f64 = pos["liquidationPrice"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);

            let mut side = "long";
            if pos_amt < 0.0 {
                side = "short";
                pos_amt = -pos_amt;
            }

            let mut mapped = Map::new();
            mapped.insert("symbol".into(), pos["symbol"].clone());
            mapped.insert("side".into(), side.into());
            mapped.insert("positionAmt".into(), pos_amt.into());
            mapped.insert("entryPrice".into(), entry_price.into());
            mapped.insert("markPrice".into(), mark_price.into());
            mapped.insert("unRealizedProfit".into(), un_pnl.into());
            mapped.insert("leverage".into(), leverage.into());
            mapped.insert("liquidationPrice".into(), liq_price.into());

            result.push(mapped);
        }

        Ok(result)
    }

    async fn open_long(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value> {
        if let Err(e) = self.cancel_all_orders(symbol).await {
            info!("  ⚠ Failed to cancel pending orders: {}", e);
        }

        self.set_leverage(symbol, leverage)
            .await
            .context("Failed to set leverage")?;

        let price = self.get_market_price(symbol).await?;
        let limit_price = price * 1.01;

        let formatted_price = self.format_price(symbol, limit_price).await?;
        let formatted_qty = self.format_quantity_f64(symbol, quantity).await?;
        let prec = self.get_precision(symbol).await?;

        let price_str = Self::format_float_with_precision(formatted_price, prec.price_precision);
        let qty_str = Self::format_float_with_precision(formatted_qty, prec.quantity_precision);

        info!(
            "  📏 Precision handling: price {:.8} -> {} (prec={}), quantity {:.8} -> {} (prec={})",
            limit_price,
            price_str,
            prec.price_precision,
            quantity,
            qty_str,
            prec.quantity_precision
        );

        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("positionSide".into(), "BOTH".into());
        params.insert("type".into(), "LIMIT".into());
        params.insert("side".into(), "BUY".into());
        params.insert("timeInForce".into(), "GTC".into());
        params.insert("quantity".into(), qty_str.into());
        params.insert("price".into(), price_str.into());

        self.request(Method::POST, "/fapi/v3/order", params).await
    }

    async fn open_short(&self, symbol: &str, quantity: f64, leverage: i32) -> Result<Value> {
        if let Err(e) = self.cancel_all_orders(symbol).await {
            info!("  ⚠ Failed to cancel pending orders: {}", e);
        }

        self.set_leverage(symbol, leverage)
            .await
            .context("Failed to set leverage")?;

        let price = self.get_market_price(symbol).await?;
        let limit_price = price * 0.99;

        let formatted_price = self.format_price(symbol, limit_price).await?;
        let formatted_qty = self.format_quantity_f64(symbol, quantity).await?;
        let prec = self.get_precision(symbol).await?;

        let price_str = Self::format_float_with_precision(formatted_price, prec.price_precision);
        let qty_str = Self::format_float_with_precision(formatted_qty, prec.quantity_precision);

        info!(
            "  📏 Precision handling: price {:.8} -> {} (prec={}), quantity {:.8} -> {} (prec={})",
            limit_price,
            price_str,
            prec.price_precision,
            quantity,
            qty_str,
            prec.quantity_precision
        );

        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("positionSide".into(), "BOTH".into());
        params.insert("type".into(), "LIMIT".into());
        params.insert("side".into(), "SELL".into());
        params.insert("timeInForce".into(), "GTC".into());
        params.insert("quantity".into(), qty_str.into());
        params.insert("price".into(), price_str.into());

        self.request(Method::POST, "/fapi/v3/order", params).await
    }

    async fn close_long(&self, symbol: &str, mut quantity: f64) -> Result<Value> {
        if quantity == 0.0 {
            let positions = self.get_positions().await?;
            for pos in positions {
                if pos["symbol"].as_str() == Some(symbol) && pos["side"].as_str() == Some("long") {
                    quantity = pos["positionAmt"].as_f64().unwrap_or(0.0);
                    break;
                }
            }
            if quantity == 0.0 {
                return Err(anyhow!("no long position found for {}", symbol));
            }
            info!("  📊 Retrieved long position quantity: {:.8}", quantity);
        }

        let price = self.get_market_price(symbol).await?;
        let limit_price = price * 0.99;

        let formatted_price = self.format_price(symbol, limit_price).await?;
        let formatted_qty = self.format_quantity_f64(symbol, quantity).await?;
        let prec = self.get_precision(symbol).await?;

        let price_str = Self::format_float_with_precision(formatted_price, prec.price_precision);
        let qty_str = Self::format_float_with_precision(formatted_qty, prec.quantity_precision);

        info!(
            "  📏 Precision handling: price {:.8} -> {} (prec={}), quantity {:.8} -> {} (prec={})",
            limit_price,
            price_str,
            prec.price_precision,
            quantity,
            qty_str,
            prec.quantity_precision
        );

        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("positionSide".into(), "BOTH".into());
        params.insert("type".into(), "LIMIT".into());
        params.insert("side".into(), "SELL".into());
        params.insert("timeInForce".into(), "GTC".into());
        params.insert("quantity".into(), qty_str.clone().into());
        params.insert("price".into(), price_str.into());

        let res = self.request(Method::POST, "/fapi/v3/order", params).await?;
        info!(
            "✓ Successfully closed long position: {} quantity: {}",
            symbol, qty_str
        );

        if let Err(e) = self.cancel_all_orders(symbol).await {
            info!("  ⚠ Failed to cancel pending orders: {}", e);
        }

        Ok(res)
    }

    async fn close_short(&self, symbol: &str, mut quantity: f64) -> Result<Value> {
        if quantity == 0.0 {
            let positions = self.get_positions().await?;
            for pos in positions {
                if pos["symbol"].as_str() == Some(symbol) && pos["side"].as_str() == Some("short") {
                    quantity = pos["positionAmt"].as_f64().unwrap_or(0.0);
                    break;
                }
            }
            if quantity == 0.0 {
                return Err(anyhow!("no short position found for {}", symbol));
            }
            info!("  📊 Retrieved short position quantity: {:.8}", quantity);
        }

        let price = self.get_market_price(symbol).await?;
        let limit_price = price * 1.01;

        let formatted_price = self.format_price(symbol, limit_price).await?;
        let formatted_qty = self.format_quantity_f64(symbol, quantity).await?;
        let prec = self.get_precision(symbol).await?;

        let price_str = Self::format_float_with_precision(formatted_price, prec.price_precision);
        let qty_str = Self::format_float_with_precision(formatted_qty, prec.quantity_precision);

        info!(
            "  📏 Precision handling: price {:.8} -> {} (prec={}), quantity {:.8} -> {} (prec={})",
            limit_price,
            price_str,
            prec.price_precision,
            quantity,
            qty_str,
            prec.quantity_precision
        );

        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("positionSide".into(), "BOTH".into());
        params.insert("type".into(), "LIMIT".into());
        params.insert("side".into(), "BUY".into());
        params.insert("timeInForce".into(), "GTC".into());
        params.insert("quantity".into(), qty_str.clone().into());
        params.insert("price".into(), price_str.into());

        let res = self.request(Method::POST, "/fapi/v3/order", params).await?;
        info!(
            "✓ Successfully closed short position: {} quantity: {}",
            symbol, qty_str
        );

        if let Err(e) = self.cancel_all_orders(symbol).await {
            info!("  ⚠ Failed to cancel pending orders: {}", e);
        }

        Ok(res)
    }

    async fn set_leverage(&self, symbol: &str, leverage: i32) -> Result<()> {
        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("leverage".into(), leverage.into());
        self.request(Method::POST, "/fapi/v3/leverage", params)
            .await
            .map(|_| ())
    }

    async fn set_margin_mode(&self, symbol: &str, is_cross_margin: bool) -> Result<()> {
        let margin_type = if is_cross_margin {
            "CROSSED"
        } else {
            "ISOLATED"
        };
        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("marginType".into(), margin_type.into());

        match self
            .request(Method::POST, "/fapi/v3/marginType", params)
            .await
        {
            Ok(_) => {
                info!("  ✓ {} margin mode has been set to {}", symbol, margin_type);
                Ok(())
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("No need to change")
                    || err_msg.contains("Margin type cannot be changed")
                {
                    info!(
                        "  ✓ {} margin mode is already {} or cannot be changed",
                        symbol, margin_type
                    );
                    Ok(())
                } else if err_msg.contains("Multi-Assets mode")
                    || err_msg.contains("-4168")
                    || err_msg.contains("4168")
                {
                    info!(
                        "  ⚠️ {} detected multi-assets mode, forcing cross margin mode",
                        symbol
                    );
                    info!(
                        "  💡 Tip: To use isolated margin mode, please disable multi-assets mode on the exchange"
                    );
                    Ok(())
                } else if err_msg.contains("unified")
                    || err_msg.contains("portfolio")
                    || err_msg.contains("Portfolio")
                {
                    info!("  ❌ {} detected unified account API", symbol);
                    Err(anyhow!(
                        "please use 'Spot & Futures Trading' API permission, not 'Unified Account API'"
                    ))
                } else {
                    info!("  ⚠️ Failed to set margin mode: {}", e);
                    // Don't error out completely, just log
                    Ok(())
                }
            }
        }
    }

    async fn get_market_price(&self, symbol: &str) -> Result<f64> {
        let url = format!("{}/fapi/v3/ticker/price?symbol={}", self.base_url, symbol);
        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(anyhow!("HTTP {}", resp.status()));
        }

        let result: Value = resp.json().await?;
        let price_str = result["price"]
            .as_str()
            .ok_or_else(|| anyhow!("unable to get price"))?;

        f64::from_str(price_str).context("failed to parse price")
    }

    async fn set_stop_loss(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        stop_price: f64,
    ) -> Result<()> {
        let side = if position_side == "SHORT" {
            "BUY"
        } else {
            "SELL"
        };

        let formatted_price = self.format_price(symbol, stop_price).await?;
        let formatted_qty = self.format_quantity_f64(symbol, quantity).await?;
        let prec = self.get_precision(symbol).await?;

        let price_str = Self::format_float_with_precision(formatted_price, prec.price_precision);
        let qty_str = Self::format_float_with_precision(formatted_qty, prec.quantity_precision);

        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("positionSide".into(), "BOTH".into());
        params.insert("type".into(), "STOP_MARKET".into());
        params.insert("side".into(), side.into());
        params.insert("stopPrice".into(), price_str.into());
        params.insert("quantity".into(), qty_str.into());
        params.insert("timeInForce".into(), "GTC".into());

        self.request(Method::POST, "/fapi/v3/order", params)
            .await
            .map(|_| ())
    }

    async fn set_take_profit(
        &self,
        symbol: &str,
        position_side: &str,
        quantity: f64,
        tp_price: f64,
    ) -> Result<()> {
        let side = if position_side == "SHORT" {
            "BUY"
        } else {
            "SELL"
        };

        let formatted_price = self.format_price(symbol, tp_price).await?;
        let formatted_qty = self.format_quantity_f64(symbol, quantity).await?;
        let prec = self.get_precision(symbol).await?;

        let price_str = Self::format_float_with_precision(formatted_price, prec.price_precision);
        let qty_str = Self::format_float_with_precision(formatted_qty, prec.quantity_precision);

        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("positionSide".into(), "BOTH".into());
        params.insert("type".into(), "TAKE_PROFIT_MARKET".into());
        params.insert("side".into(), side.into());
        params.insert("stopPrice".into(), price_str.into());
        params.insert("quantity".into(), qty_str.into());
        params.insert("timeInForce".into(), "GTC".into());

        self.request(Method::POST, "/fapi/v3/order", params)
            .await
            .map(|_| ())
    }

    async fn cancel_stop_loss_orders(&self, symbol: &str) -> Result<()> {
        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        let body = self
            .request(Method::GET, "/fapi/v3/openOrders", params)
            .await?;

        let orders = body
            .as_array()
            .ok_or_else(|| anyhow!("Invalid order data"))?;
        let mut canceled_count = 0;
        let mut cancel_errors = Vec::new();

        for order in orders {
            let order_type = order["type"].as_str().unwrap_or("");
            if order_type == "STOP_MARKET" || order_type == "STOP" {
                let order_id = order["orderId"].as_i64().unwrap_or(0);
                let pos_side = order["positionSide"].as_str().unwrap_or("");

                let mut cancel_params = Map::new();
                cancel_params.insert("symbol".into(), symbol.into());
                cancel_params.insert("orderId".into(), order_id.into());

                match self
                    .request(Method::DELETE, "/fapi/v1/order", cancel_params)
                    .await
                {
                    Ok(_) => {
                        canceled_count += 1;
                        info!(
                            "  ✓ Canceled stop-loss order (order ID: {}, type: {}, direction: {})",
                            order_id, order_type, pos_side
                        );
                    }
                    Err(e) => {
                        let err_msg = format!("order ID {}: {}", order_id, e);
                        info!("  ⚠ Failed to cancel stop-loss order: {}", err_msg);
                        cancel_errors.push(err_msg);
                    }
                }
            }
        }

        if canceled_count == 0 && cancel_errors.is_empty() {
            info!("  ℹ {} no stop-loss orders to cancel", symbol);
        } else if canceled_count > 0 {
            info!(
                "  ✓ Canceled {} stop-loss order(s) for {}",
                canceled_count, symbol
            );
        }

        if !cancel_errors.is_empty() && canceled_count == 0 {
            return Err(anyhow!(
                "failed to cancel stop-loss orders: {:?}",
                cancel_errors
            ));
        }
        Ok(())
    }

    async fn cancel_take_profit_orders(&self, symbol: &str) -> Result<()> {
        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        let body = self
            .request(Method::GET, "/fapi/v3/openOrders", params)
            .await?;

        let orders = body
            .as_array()
            .ok_or_else(|| anyhow!("Invalid order data"))?;
        let mut canceled_count = 0;
        let mut cancel_errors = Vec::new();

        for order in orders {
            let order_type = order["type"].as_str().unwrap_or("");
            if order_type == "TAKE_PROFIT_MARKET" || order_type == "TAKE_PROFIT" {
                let order_id = order["orderId"].as_i64().unwrap_or(0);
                let pos_side = order["positionSide"].as_str().unwrap_or("");

                let mut cancel_params = Map::new();
                cancel_params.insert("symbol".into(), symbol.into());
                cancel_params.insert("orderId".into(), order_id.into());

                match self
                    .request(Method::DELETE, "/fapi/v1/order", cancel_params)
                    .await
                {
                    Ok(_) => {
                        canceled_count += 1;
                        info!(
                            "  ✓ Canceled take-profit order (order ID: {}, type: {}, direction: {})",
                            order_id, order_type, pos_side
                        );
                    }
                    Err(e) => {
                        let err_msg = format!("order ID {}: {}", order_id, e);
                        info!("  ⚠ Failed to cancel take-profit order: {}", err_msg);
                        cancel_errors.push(err_msg);
                    }
                }
            }
        }

        if canceled_count == 0 && cancel_errors.is_empty() {
            info!("  ℹ {} no take-profit orders to cancel", symbol);
        } else if canceled_count > 0 {
            info!(
                "  ✓ Canceled {} take-profit order(s) for {}",
                canceled_count, symbol
            );
        }

        if !cancel_errors.is_empty() && canceled_count == 0 {
            return Err(anyhow!(
                "failed to cancel take-profit orders: {:?}",
                cancel_errors
            ));
        }
        Ok(())
    }

    async fn cancel_all_orders(&self, symbol: &str) -> Result<()> {
        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        self.request(Method::DELETE, "/fapi/v3/allOpenOrders", params)
            .await
            .map(|_| ())
    }

    async fn cancel_stop_orders(&self, symbol: &str) -> Result<()> {
        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        let body = self
            .request(Method::GET, "/fapi/v3/openOrders", params)
            .await?;

        let orders = body
            .as_array()
            .ok_or_else(|| anyhow!("Invalid order data"))?;
        let mut canceled_count = 0;

        for order in orders {
            let order_type = order["type"].as_str().unwrap_or("");
            if matches!(
                order_type,
                "STOP_MARKET" | "TAKE_PROFIT_MARKET" | "STOP" | "TAKE_PROFIT"
            ) {
                let order_id = order["orderId"].as_i64().unwrap_or(0);
                let mut cancel_params = Map::new();
                cancel_params.insert("symbol".into(), symbol.into());
                cancel_params.insert("orderId".into(), order_id.into());

                match self
                    .request(Method::DELETE, "/fapi/v3/order", cancel_params)
                    .await
                {
                    Ok(_) => {
                        canceled_count += 1;
                        info!(
                            "  ✓ Canceled take-profit/stop-loss order for {} (order ID: {}, type: {})",
                            symbol, order_id, order_type
                        );
                    }
                    Err(e) => {
                        info!("  ⚠ Failed to cancel order {}: {}", order_id, e);
                    }
                }
            }
        }

        if canceled_count == 0 {
            info!("  ℹ {} no take-profit/stop-loss orders to cancel", symbol);
        } else {
            info!(
                "  ✓ Canceled {} take-profit/stop-loss order(s) for {}",
                canceled_count, symbol
            );
        }
        Ok(())
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<Map<String, Value>> {
        let mut params = Map::new();
        params.insert("symbol".into(), symbol.into());
        params.insert("orderId".into(), order_id.into());

        let result = self.request(Method::GET, "/fapi/v3/order", params).await?;

        let mut response = Map::new();
        response.insert("orderId".into(), result["orderId"].clone());
        response.insert("symbol".into(), result["symbol"].clone());
        response.insert("status".into(), result["status"].clone());
        response.insert("side".into(), result["side"].clone());
        response.insert("type".into(), result["type"].clone());
        response.insert("time".into(), result["time"].clone());
        response.insert("updateTime".into(), result["updateTime"].clone());
        response.insert("commission".into(), 0.0.into());

        if let Some(val) = result["avgPrice"].as_str() {
            response.insert("avgPrice".into(), val.parse::<f64>().unwrap_or(0.0).into());
        } else if let Some(val) = result["avgPrice"].as_f64() {
            response.insert("avgPrice".into(), val.into());
        }

        if let Some(val) = result["executedQty"].as_str() {
            response.insert(
                "executedQty".into(),
                val.parse::<f64>().unwrap_or(0.0).into(),
            );
        } else if let Some(val) = result["executedQty"].as_f64() {
            response.insert("executedQty".into(), val.into());
        }

        Ok(response)
    }

    async fn format_quantity(&self, symbol: &str, quantity: f64) -> Result<String> {
        let formated = self.format_quantity_f64(symbol, quantity).await?;

        Ok(format!("{}", formated))
    }
}
