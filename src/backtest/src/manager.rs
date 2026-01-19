use anyhow::{Result, anyhow};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::config::BacktestConfig;
use crate::equity::resample_equity;
use crate::lock::delete_run_lock;
use crate::lock::load_run_lock;
use crate::lock::lock_is_stale;
use crate::registry::list_index_entries;
use crate::registry::remove_from_run_index;
use crate::registry::update_run_index;
use crate::runner::Runner;
use crate::storage::create_run_export;
use crate::storage::load_config;
use crate::storage::load_decision_trace;
use crate::storage::load_equity_points;
use crate::storage::load_metrics;
use crate::storage::load_run_ids;
use crate::storage::load_run_metadata;
use crate::storage::load_trade_events;
use crate::storage::save_config;
use crate::storage::save_run_metadata;
use crate::types::EquityPoint;
use crate::types::Metrics;
use crate::types::RunMetadata;
use crate::types::RunState;
use crate::types::StatusPayload;
use crate::types::TradeEvent;

use logger::info;
use mcp::Provider;
use store::decision::DecisionRecord;

pub type AIConfigResolver = Box<dyn Fn(&mut BacktestConfig) -> Result<()> + Send + Sync>;

pub struct Manager {
    state: RwLock<ManagerState>,
    mcp_client: Arc<dyn Provider>,
    ai_resolver: RwLock<Option<AIConfigResolver>>,
}

struct ManagerState {
    runners: HashMap<String, Arc<Runner>>,
    metadata: HashMap<String, RunMetadata>,
    cancels: HashMap<String, CancellationToken>,
}

impl Manager {
    pub fn new(default_client: Arc<dyn Provider>) -> Arc<Self> {
        Arc::new(Self {
            state: RwLock::new(ManagerState {
                runners: HashMap::new(),
                metadata: HashMap::new(),
                cancels: HashMap::new(),
            }),
            mcp_client: default_client,
            ai_resolver: RwLock::new(None),
        })
    }

    pub async fn set_ai_resolver(&self, resolver: AIConfigResolver) {
        let mut lock = self.ai_resolver.write().await;
        *lock = Some(resolver);
    }

    pub async fn start(self: Arc<Self>, mut cfg: BacktestConfig) -> Result<Arc<Runner>> {
        cfg.validate()?;
        self.resolve_ai_config(&mut cfg).await?;

        {
            let lock = self.state.read().await;
            if let Some(existing) = lock.runners.get(&cfg.run_id) {
                let state = existing.status.read().await;
                if *state == RunState::Running || *state == RunState::Paused {
                    return Err(anyhow!("run {} is already active", cfg.run_id));
                }
            }
        }

        let mut persist_cfg = cfg.clone();
        persist_cfg.ai_cfg.api_key = String::new();
        save_config(&cfg.run_id, &persist_cfg).await?;

        let runner: Arc<Runner> = Runner::new(cfg.clone(), self.mcp_client.clone()).await?;

        {
            let mut lock = self.state.write().await;
            lock.runners.insert(cfg.run_id.clone(), runner.clone());

            let meta = runner.current_metadata().await?;
            lock.metadata.insert(cfg.run_id.clone(), meta.clone());
        }

        if let Err(e) = runner.clone().start().await {
            let mut lock = self.state.write().await;
            lock.runners.remove(&cfg.run_id);
            lock.cancels.remove(&cfg.run_id);
            lock.metadata.remove(&cfg.run_id);
            return Err(e);
        }

        let meta = runner.current_metadata().await?;
        self.store_metadata(&cfg.run_id, Some(meta)).await;
        self.clone().launch_watcher(&cfg.run_id, runner.clone());

        Ok(runner)
    }

    pub async fn get_runner(&self, run_id: &str) -> Option<Arc<Runner>> {
        let lock = self.state.read().await;
        lock.runners.get(run_id).cloned()
    }

    pub async fn list_runs(&self) -> Result<Vec<RunMetadata>> {
        let local_copy: HashMap<String, RunMetadata> = {
            let lock = self.state.read().await;
            lock.metadata.clone()
        };

        let run_ids = load_run_ids().await?;
        let mut ordered = Vec::with_capacity(run_ids.len());

        if let Ok(entries) = list_index_entries().await {
            let mut seen = HashSet::new();
            for entry in entries {
                if run_ids.contains(&entry.run_id) {
                    ordered.push(entry.run_id.clone());
                    seen.insert(entry.run_id);
                }
            }
            for id in &run_ids {
                if !seen.contains(id) {
                    ordered.push(id.clone());
                }
            }
        } else {
            ordered = run_ids;
        }

        let mut metas = Vec::with_capacity(ordered.len());
        for run_id in ordered {
            if let Some(meta) = local_copy.get(&run_id) {
                metas.push(meta.clone());
                continue;
            }
            if let Ok(meta) = load_run_metadata(&run_id).await {
                metas.push(meta);
            }
        }

        // Sort by UpdatedAt descending
        metas.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

        Ok(metas)
    }

    pub async fn pause(&self, run_id: &str) -> Result<()> {
        if let Some(runner) = self.get_runner(run_id).await {
            runner.pause().await;
            self.refresh_metadata(run_id).await;
            Ok(())
        } else {
            Err(anyhow!("run {} not found", run_id).into())
        }
    }

    pub async fn resume(self: Arc<Self>, run_id: &str) -> Result<()> {
        if run_id.is_empty() {
            return Err(anyhow!("run_id is required"));
        }

        if let Some(runner) = self.get_runner(run_id).await {
            runner.resume().await;
            self.refresh_metadata(run_id).await;
            return Ok(());
        }

        let mut cfg = load_config(run_id).await?;
        cfg.validate()?;
        self.resolve_ai_config(&mut cfg).await?;

        let restored = Runner::new(cfg, self.mcp_client.clone()).await?;
        restored.restore_from_checkpoint().await?;

        {
            let mut lock = self.state.write().await;
            lock.runners.insert(run_id.to_string(), restored.clone());
            lock.metadata.insert(
                run_id.to_string(),
                restored.current_metadata().await.unwrap(),
            );
        }

        if let Err(e) = restored.clone().start().await {
            let mut lock = self.state.write().await;
            lock.runners.remove(run_id);
            lock.cancels.remove(run_id);
            lock.metadata.remove(run_id);
            return Err(e);
        }

        let meta = restored.current_metadata().await.unwrap();
        self.store_metadata(run_id, Some(meta)).await;
        self.clone().launch_watcher(run_id, restored);
        Ok(())
    }

    pub async fn stop(&self, run_id: &str) -> Result<()> {
        if let Some(runner) = self.get_runner(run_id).await {
            runner.stop().await;
            let _ = runner.wait().await;
            self.refresh_metadata(run_id).await;
            return Ok(());
        }

        let mut meta = self.load_metadata(run_id).await?;
        if meta.state == RunState::Stopped || meta.state == RunState::Completed {
            return Ok(());
        }
        meta.state = RunState::Stopped;
        self.store_metadata(run_id, Some(meta)).await;
        Ok(())
    }

    pub async fn wait(&self, run_id: &str) -> Result<Option<String>> {
        if let Some(runner) = self.get_runner(run_id).await {
            let res = runner.wait().await?;
            self.refresh_metadata(run_id).await;
            return Ok(res);
        }
        Err(anyhow!("run {} not found", run_id).into())
    }

    pub async fn update_label(&self, run_id: &str, label: &str) -> Result<RunMetadata> {
        let meta = self.load_metadata(run_id).await?;
        let mut meta_copy = meta.clone();
        meta_copy.label = Some(label.trim().to_string());
        self.store_metadata(run_id, Some(meta_copy.clone())).await;
        Ok(meta_copy)
    }

    pub async fn delete(&self, run_id: &str) -> Result<()> {
        if let Some(runner) = self.get_runner(run_id).await {
            runner.stop().await;
            let _ = runner.wait().await;
        }

        {
            let mut lock = self.state.write().await;
            if let Some(token) = lock.cancels.get(run_id) {
                token.cancel();
            }
            lock.cancels.remove(run_id);
            lock.runners.remove(run_id);
            lock.metadata.remove(run_id);
        }

        remove_from_run_index(run_id).await?;
        // Check if error is not exists, otherwise return error
        if let Err(e) = delete_run_lock(run_id) {
            // In Rust you'd check Error kind, here simplifying
            if !e.to_string().contains("No such file") {
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn load_metadata(&self, run_id: &str) -> Result<RunMetadata> {
        if let Some(runner) = self.get_runner(run_id).await {
            let meta = runner.current_metadata().await.unwrap();
            self.store_metadata(run_id, Some(meta.clone())).await;
            return Ok(meta);
        }
        let meta = load_run_metadata(run_id).await?;
        self.store_metadata(run_id, Some(meta.clone())).await;
        Ok(meta)
    }

    pub async fn load_equity(
        &self,
        run_id: &str,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<EquityPoint>> {
        let mut points = load_equity_points(run_id).await?;
        if !timeframe.is_empty() {
            points =
                resample_equity(points, timeframe).map_err(|e| anyhow!("{}", e.to_string()))?;
        }

        if limit > 0 && points.len() > limit {
            points = points.into_iter().rev().take(limit).rev().collect();
        }
        Ok(points)
    }

    pub async fn load_trades(&self, run_id: &str, limit: usize) -> Result<Vec<TradeEvent>> {
        let mut events = load_trade_events(run_id).await?;
        if limit > 0 && events.len() > limit {
            events = events.into_iter().rev().take(limit).rev().collect();
        }
        Ok(events)
    }

    pub async fn get_metrics(&self, run_id: &str) -> Result<Metrics> {
        Ok(load_metrics(run_id).await?)
    }

    pub async fn cleanup(&self, run_id: &str) {
        let mut lock = self.state.write().await;
        lock.runners.remove(run_id);
        if let Some(token) = lock.cancels.remove(run_id) {
            token.cancel();
        }
    }

    pub async fn status(&self, run_id: &str) -> Option<StatusPayload> {
        let runner = self.get_runner(run_id).await?;
        let meta = runner.clone().current_metadata().await.unwrap();
        let payload = runner.status_payload().await;
        self.store_metadata(run_id, Some(meta)).await;
        Some(payload)
    }

    pub fn launch_watcher(self: Arc<Self>, run_id: &str, runner: Arc<Runner>) {
        let run_id_clone = run_id.to_string().clone();
        // Spawn a background task
        tokio::spawn(async move {
            if let Err(e) = runner.wait().await {
                info!(
                    "backtest run {} finished with error: {:?}",
                    &run_id_clone, e
                );
            }
            runner.persist_metadata().await;
            let meta = runner.current_metadata().await.unwrap();

            self.store_metadata(run_id_clone.as_str(), Some(meta.clone()))
                .await;

            // Updating internal state
            {
                let mut lock = self.state.write().await;
                // Preserve existing labels/errors if missing in new meta?
                // (Logic simplified from Go code)
                if let Some(_existing) = lock.metadata.get(run_id_clone.as_str()) {
                    // Logic to merge label/error if needed
                }
                lock.metadata.insert(run_id_clone.clone(), meta);

                if let Some(token) = lock.cancels.remove(run_id_clone.as_str()) {
                    token.cancel();
                }
                lock.runners.remove(run_id_clone.as_str());
            }
        });
    }

    async fn refresh_metadata(&self, run_id: &str) {
        if let Some(runner) = self.get_runner(run_id).await {
            let meta = runner.current_metadata().await.unwrap();
            self.store_metadata(run_id, Some(meta)).await;
        }
    }

    async fn store_metadata(&self, run_id: &str, meta: Option<RunMetadata>) {
        let mut meta = match meta {
            Some(m) => m,
            None => return,
        };

        {
            let mut lock = self.state.write().await;
            if let Some(existing) = lock.metadata.get(run_id) {
                if meta.label.is_none() && existing.label.is_some() {
                    meta.label = existing.label.clone();
                }
                if meta.last_error.is_none() && existing.last_error.is_some() {
                    meta.last_error = existing.last_error.clone();
                }
            }
            lock.metadata.insert(run_id.to_string(), meta.clone());
        }

        let _ = save_run_metadata(&mut meta).await;
        if let Err(e) = update_run_index(&meta, None).await {
            info!("failed to update run index for {}: {:?}", run_id, e);
        }
    }

    async fn resolve_ai_config(&self, cfg: &mut BacktestConfig) -> Result<()> {
        let provider = cfg.ai_cfg.provider.trim();
        let api_key = cfg.ai_cfg.api_key.trim();

        if !provider.is_empty() && !provider.eq_ignore_ascii_case("inherit") && !api_key.is_empty()
        {
            return Ok(());
        }

        if api_key.is_empty() {
            return Err(anyhow!(
                "AI configuration missing key and no resolver configured"
            ));
        }

        let guard = self.ai_resolver.read().await;
        if let Some(resolver) = guard.as_ref() {
            match resolver(cfg) {
                Ok(_) => println!("config modify success: {:?}", cfg),
                Err(e) => println!("config modify failed: {}", e),
            }
        }

        Ok(())
    }

    pub async fn get_trace(&self, run_id: &str, cycle: i32) -> Result<DecisionRecord> {
        Ok(load_decision_trace(run_id, cycle).await?)
    }

    pub async fn export_run(&self, run_id: &str) -> Result<String> {
        Ok(create_run_export(run_id).await?)
    }

    pub async fn restore_runs(&self) -> Result<()> {
        let run_ids = load_run_ids().await?;
        for run_id in run_ids {
            let mut meta = match load_run_metadata(&run_id).await {
                Ok(m) => m,
                Err(e) => {
                    info!("skip run {}: {:?}", run_id, e);
                    continue;
                }
            };

            if meta.state == RunState::Running {
                let lock = load_run_lock(&run_id);
                let is_stale = match lock {
                    Ok(l) => lock_is_stale(&l),
                    Err(_) => true, // Error loading lock implies issues, treat as stale/gone
                };

                if is_stale {
                    if let Err(e) = delete_run_lock(&run_id) {
                        info!("failed to cleanup lock for {}: {:?}", run_id, e);
                    }
                    meta.state = RunState::Paused;
                    if let Err(e) = save_run_metadata(&mut meta).await {
                        info!("failed to mark {} paused: {:?}", run_id, e);
                    }
                }
            }

            {
                let mut lock = self.state.write().await;
                lock.metadata.insert(run_id.clone(), meta.clone());
            }

            if let Err(e) = update_run_index(&meta, None).await {
                info!("failed to sync index for {}: {:?}", run_id, e);
            }
        }
        Ok(())
    }

    pub async fn restore_runs_from_disk(&self) -> Result<()> {
        self.restore_runs().await
    }
}
