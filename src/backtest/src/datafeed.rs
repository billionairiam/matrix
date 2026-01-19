use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{TimeZone, Utc};

use super::config::BacktestConfig;
use market::data::build_data_from_klines;
use market::historical::get_klines_range;
use market::timeframe::tf_duration;
use market::types::{Data, Kline};

struct TimeframeSeries {
    klines: Vec<Kline>,
    close_times: Vec<i64>,
}

struct SymbolSeries {
    by_tf: HashMap<String, TimeframeSeries>,
}

/// DataFeed manages historical kline data and provides time-progressive snapshots.
pub struct DataFeed {
    cfg: BacktestConfig,
    symbols: Vec<String>,
    timeframes: Vec<String>,
    symbol_series: HashMap<String, SymbolSeries>,
    decision_times: Vec<i64>,
    primary_tf: String,
    longer_tf: Option<String>,
}

impl DataFeed {
    pub async fn new(cfg: BacktestConfig) -> Result<Self> {
        let mut df = DataFeed {
            symbols: cfg.symbols.clone(),
            timeframes: cfg.timeframes.clone(),
            primary_tf: cfg.decision_timeframe.clone(),
            cfg, // move cfg in
            symbol_series: HashMap::new(),
            decision_times: Vec::new(),
            longer_tf: None,
        };

        df.load_all().await?;
        Ok(df)
    }

    async fn load_all(&mut self) -> Result<()> {
        let start_dt = Utc.timestamp_opt(self.cfg.start_ts, 0).unwrap();
        let end_dt = Utc.timestamp_opt(self.cfg.end_ts, 0).unwrap();

        // longest timeframe used for auxiliary indicators
        let mut longest_dur = Duration::ZERO;
        for tf in &self.timeframes {
            let dur = tf_duration(tf)?;
            if dur > longest_dur {
                longest_dur = dur;
                self.longer_tf = Some(tf.clone());
            }
        }

        for symbol in &self.symbols {
            let mut by_tf = HashMap::new();

            for tf in &self.timeframes {
                let dur = tf_duration(tf)?;

                // Buffer calculation: 200 bars back
                let buffer = dur * 200;
                let mut fetch_start = start_dt - buffer;

                // Ensure we don't go before Epoch if logic dictates
                if fetch_start.timestamp() < 0 {
                    fetch_start = Utc.timestamp_opt(0, 0).unwrap();
                }

                let fetch_end = end_dt + dur;

                let klines = get_klines_range(symbol, tf, fetch_start, fetch_end)
                    .await
                    .context(anyhow!("fetch klines for {} {}", symbol, tf))?;

                if klines.is_empty() {
                    return Err(anyhow!("no klines for {} {}", symbol, tf));
                }

                // Extract close times for fast binary searching
                let close_times: Vec<i64> = klines.iter().map(|k| k.close_time).collect();

                by_tf.insert(
                    tf.clone(),
                    TimeframeSeries {
                        klines,
                        close_times,
                    },
                );
            }
            self.symbol_series
                .insert(symbol.clone(), SymbolSeries { by_tf });
        }

        // Generate backtest progress timeline using the primary timeframe of the first symbol
        let first_symbol = self
            .symbols
            .first()
            .ok_or_else(|| anyhow!("no symbols provided"))?;

        let primary_series = self
            .symbol_series
            .get(first_symbol)
            .and_then(|ss| ss.by_tf.get(&self.primary_tf))
            .ok_or_else(|| anyhow!("missing primary series data"))?;

        let start_ms = start_dt.timestamp_millis();
        let end_ms = end_dt.timestamp_millis();

        for &ts in &primary_series.close_times {
            if ts < start_ms {
                continue;
            }
            if ts > end_ms {
                break;
            }
            self.decision_times.push(ts);

            // Align other symbols; report error early if data is missing
            for symbol in self.symbols.iter().skip(1) {
                if let Some(ss) = self.symbol_series.get(symbol) {
                    if !ss.by_tf.contains_key(&self.primary_tf) {
                        return Err(anyhow!(
                            "symbol {} missing timeframe {}",
                            symbol,
                            self.primary_tf
                        ));
                    }
                }
            }
        }

        if self.decision_times.is_empty() {
            return Err(anyhow!("no decision bars in range"));
        }

        Ok(())
    }

    pub fn decision_bar_count(&self) -> usize {
        self.decision_times.len()
    }

    pub fn decision_timestamp(&self, index: usize) -> i64 {
        self.decision_times[index]
    }

    /// slice_up_to finds the slice of klines ending at or before ts.
    fn slice_up_to(&self, symbol: &str, tf: &str, ts: i64) -> Option<&[Kline]> {
        let ss = self.symbol_series.get(symbol)?;
        let series = ss.by_tf.get(tf)?;

        let idx = series.close_times.partition_point(|&t| t <= ts);

        if idx == 0 {
            return None;
        }
        Some(&series.klines[..idx])
    }

    /// BuildMarketData constructs the market state at a specific timestamp.
    /// Returns: (PrimaryMap, MultiMap)
    pub fn build_market_data(
        &self,
        ts: i64,
    ) -> Result<(
        HashMap<String, Data>,
        HashMap<String, HashMap<String, Data>>,
    )> {
        let mut result = HashMap::with_capacity(self.symbols.len());
        let mut multi = HashMap::with_capacity(self.symbols.len());

        for symbol in &self.symbols {
            let mut per_tf = HashMap::with_capacity(self.timeframes.len());

            for tf in &self.timeframes {
                let series_slice = match self.slice_up_to(symbol, tf, ts) {
                    Some(s) if !s.is_empty() => s,
                    _ => continue,
                };

                // Handle longer timeframe logic
                let mut longer_slice: Option<&[Kline]> = None;
                if let Some(ref l_tf) = self.longer_tf {
                    if l_tf != tf {
                        longer_slice = self.slice_up_to(symbol, l_tf, ts);
                    }
                }

                let data = build_data_from_klines(symbol, series_slice, longer_slice)?;

                // Store clone in result maps (market::Data should be lightweight or ref-counted)
                per_tf.insert(tf.clone(), data.clone());

                if tf == &self.primary_tf {
                    result.insert(symbol.clone(), data);
                }
            }

            if !per_tf.contains_key(&self.primary_tf) {
                return Err(anyhow!("no primary data for {} at {}", symbol, ts));
            }
            multi.insert(symbol.clone(), per_tf);
        }

        Ok((result, multi))
    }

    /// Returns references to (current_kline, next_kline).
    pub fn decision_bar_snapshot(&self, symbol: &str, ts: i64) -> (Option<&Kline>, Option<&Kline>) {
        let Some(ss) = self.symbol_series.get(symbol) else {
            return (None, None);
        };
        let Some(series) = ss.by_tf.get(&self.primary_tf) else {
            return (None, None);
        };

        // Find exact match for timestamp
        if let Ok(idx) = series.close_times.binary_search(&ts) {
            let curr = &series.klines[idx];
            let next = series.klines.get(idx + 1);
            return (Some(curr), next);
        }

        (None, None)
    }
}
