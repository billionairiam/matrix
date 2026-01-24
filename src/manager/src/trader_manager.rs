use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use futures::future::join_all;
use serde_json::{Map, Value, json};
use store::ai_model::AIModel;
use store::exchange::Exchange;
use store::store::Store;
use store::trader::Trader;
use tokio::sync::RwLock;
use tracing::info;
use trader::auto_trader::AutoTrader;
use trader::auto_trader::AutoTraderConfig;

// CompetitionCache competition data cache
struct CompetitionCache {
    data: RwLock<Option<Value>>,
    last_updated: RwLock<Instant>,
}

impl CompetitionCache {
    fn new() -> Self {
        Self {
            data: RwLock::new(None),
            last_updated: RwLock::new(Instant::now() - Duration::from_secs(3600)),
        }
    }
}

// TraderManager manages multiple trader instances
pub struct TraderManager {
    traders: RwLock<HashMap<String, Arc<AutoTrader>>>,
    competition_cache: CompetitionCache,
}

impl TraderManager {
    pub fn new() -> Self {
        Self {
            traders: RwLock::new(HashMap::new()),
            competition_cache: CompetitionCache::new(),
        }
    }

    pub async fn get_trader(&self, id: &str) -> Result<Arc<AutoTrader>> {
        let traders = self.traders.read().await;
        traders
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow!("trader ID '{}' does not exist", id))
    }

    pub async fn get_all_traders(&self) -> HashMap<String, Arc<AutoTrader>> {
        let traders = self.traders.read().await;
        traders.clone()
    }

    pub async fn get_trader_ids(&self) -> Vec<String> {
        let traders = self.traders.read().await;
        traders.keys().cloned().collect()
    }

    pub async fn start_all(&self) {
        let traders = self.traders.read().await;
        info!("🚀 Starting all traders...");

        for (_id, t) in traders.iter() {
            let t_clone = t.clone();
            let name = t.get_name();

            tokio::spawn(async move {
                info!("{}", &format!("▶️  Starting {}...", name));
                if let Err(err) = t_clone.run().await {
                    info!("{}", &format!("❌ {} runtime error: {:?}", name, err));
                }
            });
        }
    }

    pub async fn stop_all(&self) {
        let traders = self.traders.read().await;
        info!("⏹  Stopping all traders...");
        for t in traders.values() {
            t.stop().await;
        }
    }

    pub async fn auto_start_running_traders(&self, st: &Store) {
        let trader_list = match st.trader().await.list_all().await {
            Ok(list) => list,
            Err(e) => {
                info!("{}", &format!("⚠️ Failed to get trader list: {:?}", e));
                return;
            }
        };

        let mut running_trader_ids = HashMap::new();
        for trader_cfg in trader_list {
            if trader_cfg.is_running {
                running_trader_ids.insert(trader_cfg.id.clone(), true);
            }
        }

        if running_trader_ids.is_empty() {
            info!("📋 No traders to auto-restore");
            return;
        }

        let traders = self.traders.read().await;
        let mut started_count = 0;

        for (id, t) in traders.iter() {
            if running_trader_ids.contains_key(id) {
                let t_clone = t.clone();
                let name = t.get_name();

                tokio::spawn(async move {
                    info!("{}", &format!("▶️  Auto-restoring {}...", name));
                    if let Err(err) = t_clone.run().await {
                        info!("{}", &format!("❌ {} runtime error: {:?}", name, err));
                    }
                });
                started_count += 1;
            }
        }

        if started_count > 0 {
            info!("{}", &format!("✓ Auto-restored {} traders", started_count));
        }
    }

    pub async fn get_comparison_data(&self) -> Result<Value> {
        let traders_map = self.traders.read().await;
        let mut traders_data = Vec::new();

        for t in traders_map.values() {
            let account = match t.get_account_info().await {
                Ok(acc) => acc,
                Err(_) => continue,
            };

            let status = t.get_status().await?;

            traders_data.push(json!({
                "trader_id":       t.get_id(),
                "trader_name":     t.get_name(),
                "ai_model":        t.get_ai_model(),
                "exchange":        t.get_exchange(),
                "total_equity":    account["total_equity"],
                "total_pnl":       account["total_pnl"],
                "total_pnl_pct":   account["total_pnl_pct"],
                "position_count":  account["position_count"],
                "margin_used_pct": account["margin_used_pct"],
                "call_count":      status["call_count"],
                "is_running":      status["is_running"],
            }));
        }

        Ok(json!({
            "traders": traders_data,
            "count": traders_data.len()
        }))
    }

    // GetCompetitionData retrieves competition data (all traders across platform)
    pub async fn get_competition_data(&self) -> Result<Value> {
        // Check if cache is valid (within 30 seconds)
        {
            let cache_ts = self.competition_cache.last_updated.read().await;
            let cache_data = self.competition_cache.data.read().await;

            if cache_ts.elapsed() < Duration::from_secs(30) && cache_data.is_some() {
                info!(
                    "{}",
                    &format!(
                        "📋 Returning competition data cache (cache age: {:.1}s)",
                        cache_ts.elapsed().as_secs_f64()
                    )
                );
                return Ok(cache_data.clone().unwrap());
            }
        }

        // Get all trader list
        let all_traders: Vec<Arc<AutoTrader>> = {
            let map = self.traders.read().await;
            map.values().cloned().collect()
        };

        for t in &all_traders {
            info!(
                "{}",
                &format!(
                    "📋 Competition data includes trader: {} ({})",
                    t.get_name(),
                    t.get_id()
                )
            );
        }

        info!(
            "{}",
            &format!(
                "🔄 Refreshing competition data, trader count: {}",
                all_traders.len()
            ),
        );

        // Concurrently fetch trader data
        let mut traders_data = self.get_concurrent_trader_data(&all_traders).await?;

        // Sort by profit rate (descending)
        traders_data.sort_by(|a, b| {
            let pnl_a = a
                .get("total_pnl_pct")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let pnl_b = b
                .get("total_pnl_pct")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            pnl_b
                .partial_cmp(&pnl_a)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Limit to top 50
        let total_count = traders_data.len();
        let limit = 50;
        if traders_data.len() > limit {
            traders_data.truncate(limit);
        }

        let result = json!({
            "traders": traders_data,
            "count": traders_data.len(),
            "total_count": total_count
        });

        // Update cache
        {
            let mut cache_data = self.competition_cache.data.write().await;
            let mut cache_ts = self.competition_cache.last_updated.write().await;
            *cache_data = Some(result.clone());
            *cache_ts = Instant::now();
        }

        Ok(result)
    }

    // getConcurrentTraderData concurrently fetches data for multiple traders
    async fn get_concurrent_trader_data(
        &self,
        traders: &[Arc<AutoTrader>],
    ) -> Result<Vec<Map<String, Value>>> {
        let futures: Vec<_> = traders
            .iter()
            .map(|t| {
                let t = t.clone();
                async move {
                    // 设置 3 秒超时
                    let result = tokio::time::timeout(Duration::from_secs(3), async {
                        t.get_account_info().await
                    })
                    .await;

                    let mut data = Map::new();
                    match t.get_status().await {
                        Ok(s) => {
                            data.insert("is_running".to_string(), json!(s["is_running"]));
                        }
                        Err(_) => {
                            data.insert("is_running".to_string(), json!(false));
                        }
                    };

                    // 基础字段
                    data.insert("trader_id".to_string(), json!(t.get_id()));
                    data.insert("trader_name".to_string(), json!(t.get_name()));
                    data.insert("ai_model".to_string(), json!(t.get_ai_model()));
                    data.insert("exchange".to_string(), json!(t.get_exchange()));
                    data.insert(
                        "system_prompt_template".to_string(),
                        json!(t.get_system_prompt_template()),
                    );

                    match result {
                        Ok(Ok(account)) => {
                            // 成功获取
                            data.insert(
                                "total_equity".to_string(),
                                account.get("total_equity").cloned().unwrap_or(json!(0.0)),
                            );
                            data.insert(
                                "total_pnl".to_string(),
                                account.get("total_pnl").cloned().unwrap_or(json!(0.0)),
                            );
                            data.insert(
                                "total_pnl_pct".to_string(),
                                account.get("total_pnl_pct").cloned().unwrap_or(json!(0.0)),
                            );
                            data.insert(
                                "position_count".to_string(),
                                account.get("position_count").cloned().unwrap_or(json!(0)),
                            );
                            data.insert(
                                "margin_used_pct".to_string(),
                                account
                                    .get("margin_used_pct")
                                    .cloned()
                                    .unwrap_or(json!(0.0)),
                            );
                        }
                        Ok(Err(e)) => {
                            // 内部错误
                            info!(
                                "{}",
                                &format!(
                                    "⚠️ Failed to get account info for trader {}: {:?}",
                                    t.get_id(),
                                    e
                                )
                            );
                            data.insert("total_equity".to_string(), json!(0.0));
                            data.insert("total_pnl".to_string(), json!(0.0));
                            data.insert("total_pnl_pct".to_string(), json!(0.0));
                            data.insert("position_count".to_string(), json!(0));
                            data.insert("margin_used_pct".to_string(), json!(0.0));
                            data.insert("error".to_string(), json!("Failed to get account data"));
                        }
                        Err(_) => {
                            // 超时
                            info!(
                                "{}",
                                &format!(
                                    "⏰ Timeout getting account info for trader {}",
                                    t.get_id()
                                )
                            );
                            data.insert("total_equity".to_string(), json!(0.0));
                            data.insert("total_pnl".to_string(), json!(0.0));
                            data.insert("total_pnl_pct".to_string(), json!(0.0));
                            data.insert("position_count".to_string(), json!(0));
                            data.insert("margin_used_pct".to_string(), json!(0.0));
                            data.insert("error".to_string(), json!("Request timeout"));
                        }
                    }
                    data
                }
            })
            .collect();

        Ok(join_all(futures).await)
    }

    // 获取 Top 5 Traders
    pub async fn get_top_traders_data(&self) -> Result<Value> {
        // 复用 GetCompetitionData 逻辑
        let competition_data = self.get_competition_data().await?;

        let mut traders = competition_data["traders"]
            .as_array()
            .ok_or(anyhow!("Invalid data format"))?
            .clone();

        let limit = 5;
        if traders.len() > limit {
            traders.truncate(limit);
        }

        Ok(json!({
            "traders": traders,
            "count": traders.len()
        }))
    }

    // 从内存中移除 Trader
    pub async fn remove_trader(&self, trader_id: &str) {
        let mut traders = self.traders.write().await;
        if traders.remove(trader_id).is_some() {
            info!("{}", &format!("✓ Trader {} removed from memory", trader_id));
        }
    }

    // 加载指定用户的 Traders
    pub async fn load_user_traders_from_store(&self, st: &Store, user_id: &str) -> Result<()> {
        let traders_cfg = st.trader().await.list(user_id).await?;
        info!(
            "{}",
            &format!(
                "📋 Loading trader configurations for user {}: {} traders",
                user_id,
                traders_cfg.len()
            )
        );

        let ai_models = st.ai_model().await.list(user_id).await?;
        let exchanges = st.exchange().await.list(user_id).await?;

        // 准备好所有需要添加的 trader，最后统一插入
        let mut traders_to_add = Vec::new();

        for trader_cfg in traders_cfg {
            // 检查是否已存在 (需要读锁)
            if self.traders.read().await.contains_key(&trader_cfg.id) {
                info!(
                    "{}",
                    &format!("⚠️ Trader {} already loaded, skipping", trader_cfg.name)
                );
                continue;
            }

            // 匹配 AI Model
            let ai_model_cfg = ai_models
                .iter()
                .find(|m| m.id == trader_cfg.ai_model_id)
                .or_else(|| {
                    ai_models
                        .iter()
                        .find(|m| m.provider == trader_cfg.ai_model_id)
                }); // Legacy match

            if ai_model_cfg.is_none() {
                info!(
                    "{}",
                    &format!(
                        "⚠️ AI model {} for trader {} does not exist, skipping",
                        trader_cfg.ai_model_id, trader_cfg.name
                    )
                );
                continue;
            }
            let ai_model_cfg = ai_model_cfg.unwrap();

            if !ai_model_cfg.enabled {
                info!(
                    "{}",
                    &format!(
                        "⚠️ AI model {} for trader {} is not enabled, skipping",
                        trader_cfg.ai_model_id, trader_cfg.name
                    )
                );
                continue;
            }

            // 匹配 Exchange
            let exchange_cfg = exchanges.iter().find(|e| e.id == trader_cfg.exchange_id);

            if exchange_cfg.is_none() {
                info!(
                    "{}",
                    &format!(
                        "⚠️ Exchange {} for trader {} does not exist, skipping",
                        trader_cfg.exchange_id, trader_cfg.name
                    )
                );
                continue;
            }
            let exchange_cfg = exchange_cfg.unwrap();

            if !exchange_cfg.enabled {
                info!(
                    "{}",
                    &format!(
                        "⚠️ Exchange {} for trader {} is not enabled, skipping",
                        trader_cfg.exchange_id, trader_cfg.name
                    )
                );
                continue;
            }

            info!(
                "{}",
                &format!(
                    "📦 Loading trader {} (AI Model: {}, Exchange: {}, Strategy ID: {})",
                    trader_cfg.name, ai_model_cfg.provider, exchange_cfg.id, trader_cfg.strategy_id
                )
            );

            traders_to_add.push((trader_cfg, ai_model_cfg.clone(), exchange_cfg.clone()));
        }

        // 实际加载逻辑
        for (trader_cfg, ai_model_cfg, exchange_cfg) in traders_to_add {
            if let Err(e) = self
                .add_trader_from_store(&trader_cfg, &ai_model_cfg, &exchange_cfg, st)
                .await
            {
                info!(
                    "{}",
                    &format!("❌ Failed to load trader {}: {:?}", trader_cfg.name, e)
                );
            }
        }

        Ok(())
    }

    // 加载所有用户的 Traders
    pub async fn load_traders_from_store(&self, st: &Store) -> Result<()> {
        let user_ids = st.user().get_all_ids().await?;
        info!(
            "{}",
            &format!(
                "📋 Found {} users, loading all trader configurations...",
                user_ids.len()
            )
        );

        let mut all_traders_cfg = Vec::new();
        for user_id in &user_ids {
            match st.trader().await.list(user_id).await {
                Ok(traders) => {
                    info!(
                        "{}",
                        &format!("📋 User {}: {} traders", user_id, traders.len()),
                    );
                    all_traders_cfg.extend(traders);
                }
                Err(e) => {
                    info!(
                        "{}",
                        &format!("⚠️ Failed to get traders for user {}: {:?}", user_id, e)
                    );
                }
            }
        }

        info!(
            "{}",
            &format!(
                "📋 Total loaded trader configurations: {}",
                all_traders_cfg.len()
            )
        );

        for trader_cfg in all_traders_cfg {
            if self.traders.read().await.contains_key(&trader_cfg.id) {
                continue;
            }

            // 获取该用户的配置
            let ai_models = st
                .ai_model()
                .await
                .list(&trader_cfg.user_id)
                .await
                .unwrap_or_default();
            let exchanges = st
                .exchange()
                .await
                .list(&trader_cfg.user_id)
                .await
                .unwrap_or_default();

            // 匹配 AI Model
            let mut ai_model_cfg = ai_models.iter().find(|m| m.id == trader_cfg.ai_model_id);
            if ai_model_cfg.is_none() {
                if let Some(legacy_match) = ai_models
                    .iter()
                    .find(|m| m.provider == trader_cfg.ai_model_id)
                {
                    info!(
                        "{}",
                        &format!(
                            "⚠️ Trader {} using legacy provider match: {} -> {}",
                            trader_cfg.name, trader_cfg.ai_model_id, legacy_match.id
                        )
                    );
                    ai_model_cfg = Some(legacy_match);
                }
            }

            if ai_model_cfg.is_none() {
                info!(
                    "{}",
                    &format!(
                        "⚠️ AI model {} for trader {} does not exist, skipping",
                        trader_cfg.ai_model_id, trader_cfg.name
                    )
                );
                continue;
            }
            let ai_model_cfg = ai_model_cfg.unwrap();

            if !ai_model_cfg.enabled {
                info!(
                    "{}",
                    &format!(
                        "⚠️ AI model {} for trader {} is not enabled, skipping",
                        trader_cfg.ai_model_id, trader_cfg.name
                    ),
                );
                continue;
            }

            // 匹配 Exchange
            let exchange_cfg = exchanges.iter().find(|e| e.id == trader_cfg.exchange_id);
            if exchange_cfg.is_none() {
                info!(
                    "{}",
                    &format!(
                        "⚠️ Exchange {} for trader {} does not exist, skipping",
                        trader_cfg.exchange_id, trader_cfg.name
                    ),
                );
                continue;
            }
            let exchange_cfg = exchange_cfg.unwrap();

            if !exchange_cfg.enabled {
                info!(
                    "{}",
                    &format!(
                        "⚠️ Exchange {} for trader {} is not enabled, skipping",
                        trader_cfg.exchange_id, trader_cfg.name
                    ),
                );
                continue;
            }

            if let Err(e) = self
                .add_trader_from_store(&trader_cfg, ai_model_cfg, exchange_cfg, st)
                .await
            {
                info!(
                    "{}",
                    &format!("❌ Failed to add trader {}: {:?}", trader_cfg.name, e)
                );
            }
        }

        info!(
            "{}",
            &format!(
                "✓ Successfully loaded {} traders to memory",
                self.traders.read().await.len()
            )
        );
        Ok(())
    }

    // 内部方法：从配置添加 Trader
    async fn add_trader_from_store(
        &self,
        trader_cfg: &Trader,
        ai_model_cfg: &AIModel,
        exchange_cfg: &Exchange,
        st: &Store,
    ) -> Result<()> {
        {
            let map = self.traders.read().await;
            if map.contains_key(&trader_cfg.id) {
                return Err(anyhow!("trader ID '{}' already exists", trader_cfg.id));
            }
        }

        let strategy_config = if !trader_cfg.strategy_id.is_empty() {
            let strategy = st
                .strategy()
                .get(&trader_cfg.user_id, &trader_cfg.strategy_id)
                .await
                .map_err(|e| {
                    anyhow!(
                        "failed to load strategy {} for trader {}: {:?}",
                        trader_cfg.strategy_id,
                        trader_cfg.name,
                        e
                    )
                })?;

            let config = strategy.parse_config().map_err(|e| {
                anyhow!(
                    "failed to parse strategy config for trader {}: {:?}",
                    trader_cfg.name,
                    e
                )
            })?;

            info!(
                "{}",
                &format!(
                    "✓ Trader {} loaded strategy config: {}",
                    trader_cfg.name, strategy.name
                )
            );
            Some(config)
        } else {
            return Err(anyhow!(
                "trader {} has no strategy configured",
                trader_cfg.name
            ));
        };

        // 构建 AutoTraderConfig
        let mut config = AutoTraderConfig {
            id: trader_cfg.id.clone(),
            name: trader_cfg.name.clone(),
            ai_model: ai_model_cfg.provider.clone(),
            exchange: exchange_cfg.id.clone(),
            binance_api_key: String::new(),
            binance_secret_key: String::new(),
            hyperliquid_private_key: String::new(),
            hyperliquid_wallet_addr: String::new(),
            hyperliquid_testnet: exchange_cfg.testnet,
            use_qwen: ai_model_cfg.provider == "qwen",
            deepseek_key: String::new(),
            qwen_key: String::new(),
            custom_api_url: ai_model_cfg.custom_api_url.clone(),
            custom_model_name: ai_model_cfg.custom_model_name.clone(),
            initial_balance: trader_cfg.initial_balance,
            is_cross_margin: trader_cfg.is_cross_margin,
            strategy_config: strategy_config,
            aster_user: String::new(),
            aster_signer: String::new(),
            aster_private_key: String::new(),
            custom_api_key: String::new(),
            scan_interval_sec: trader_cfg.scan_interval_minutes as u64,
        };

        // 设置 API Keys
        match exchange_cfg.id.as_str() {
            "binance" => {
                config.binance_api_key = exchange_cfg.api_key.clone();
                config.binance_secret_key = exchange_cfg.secret_key.clone();
            }
            "hyperliquid" => {
                config.hyperliquid_private_key = exchange_cfg.api_key.clone();
                config.hyperliquid_wallet_addr = exchange_cfg.hyperliquid_wallet_addr.clone();
            }
            "aster" => {
                config.aster_user = exchange_cfg.aster_user.clone();
                config.aster_signer = exchange_cfg.aster_signer.clone();
                config.aster_private_key = exchange_cfg.aster_private_key.clone();
            }
            _ => {}
        }

        // 设置 AI Model Keys
        if ai_model_cfg.provider == "qwen" {
            config.qwen_key = ai_model_cfg.api_key.clone();
        } else if ai_model_cfg.provider == "deepseek" {
            config.deepseek_key = ai_model_cfg.api_key.clone();
        }

        let at = AutoTrader::new(
            config,
            Arc::new(Some(st.clone())),
            trader_cfg.user_id.clone(),
        )
        .await
        .map_err(|e| anyhow!("failed to create trader: {:?}", e))?;

        // 设置自定义 Prompt
        if !trader_cfg.custom_prompt.is_empty() {
            at.set_custom_prompt(&trader_cfg.custom_prompt).await;
            at.set_override_base_prompt(trader_cfg.override_base_prompt)
                .await;
            if trader_cfg.override_base_prompt {
                info!("✓ Set custom trading strategy prompt (overriding base prompt)");
            } else {
                info!("✓ Set custom trading strategy prompt (supplementing base prompt)");
            }
        }

        // 插入 Map (写锁)
        let mut map = self.traders.write().await;
        map.insert(trader_cfg.id.clone(), Arc::new(at));

        info!(
            "{}",
            &format!(
                "✓ Trader {} ({} + {}) loaded to memory",
                trader_cfg.name, ai_model_cfg.provider, exchange_cfg.id
            )
        );

        Ok(())
    }
}
