use std::collections::HashMap;
use std::f64;

use crate::config::BacktestConfig;
use crate::storage::load_equity_points;
use crate::storage::load_trade_events;
use crate::types::BacktestState;
use crate::types::EquityPoint;
use crate::types::Metrics;
use crate::types::TradeEvent;
use anyhow::{Result, anyhow};

/// CalculateMetrics reads existing logs and calculates summary metrics.
pub async fn calculate_metrics(
    run_id: &str,
    cfg: Option<&BacktestConfig>,
    state: Option<&BacktestState>,
) -> Result<Metrics> {
    let cfg = cfg.ok_or(anyhow!("config is nil"))?;

    // Load data (Placeholder functions, implementation depends on your storage layer)
    let points = load_equity_points(run_id).await?;
    let events = load_trade_events(run_id).await?;

    let mut metrics = Metrics {
        symbol_stats: HashMap::new(),
        ..Default::default()
    };

    metrics.liquidated = determine_liquidation(&events, state);

    let mut initial_balance = cfg.initial_balance;
    if initial_balance <= 0.0 {
        initial_balance = 1.0;
    }

    let mut last_equity = initial_balance;
    if let Some(last_pt) = points.last() {
        if last_pt.equity > 0.0 {
            last_equity = last_pt.equity;
        }
    } else if let Some(s) = state {
        if s.equity > 0.0 {
            last_equity = s.equity;
        }
    }

    metrics.total_return_pct = ((last_equity - initial_balance) / initial_balance) * 100.0;
    metrics.max_drawdown_pct = calculate_max_drawdown(&points, state);
    metrics.sharpe_ratio = calculate_sharpe_ratio(&points);

    fill_trade_metrics(&mut metrics, &events);

    Ok(metrics)
}

fn determine_liquidation(events: &Vec<TradeEvent>, state: Option<&BacktestState>) -> bool {
    if let Some(s) = state {
        if s.liquidated {
            return true;
        }
    }
    // Iterate in reverse to find recent liquidation quickly, though standard iteration works too
    for evt in events.iter().rev() {
        if evt.liquidation_flag {
            return true;
        }
    }
    false
}

fn calculate_max_drawdown(points: &[EquityPoint], state: Option<&BacktestState>) -> f64 {
    if points.is_empty() {
        return state.map(|s| s.max_drawdown_pct).unwrap_or(0.0);
    }

    let mut peak = points[0].equity;
    if peak <= 0.0 {
        peak = 1.0;
    }

    let mut max_dd = 0.0;

    for pt in points {
        if pt.equity > peak {
            peak = pt.equity;
        }
        if peak <= 0.0 {
            continue;
        }
        let dd = (peak - pt.equity) / peak * 100.0;
        if dd > max_dd {
            max_dd = dd;
        }
    }

    if let Some(s) = state {
        if s.max_drawdown_pct > max_dd {
            max_dd = s.max_drawdown_pct;
        }
    }

    max_dd
}

fn calculate_sharpe_ratio(points: &[EquityPoint]) -> f64 {
    if points.len() < 2 {
        return 0.0;
    }

    let mut returns = Vec::with_capacity(points.len() - 1);
    let mut prev = points[0].equity;

    for i in 1..points.len() {
        let curr = points[i].equity;
        if prev <= 0.0 {
            prev = curr;
            continue;
        }
        let ret = (curr - prev) / prev;
        returns.push(ret);
        prev = curr;
    }

    if returns.is_empty() {
        return 0.0;
    }

    let len = returns.len() as f64;
    let mean: f64 = returns.iter().sum::<f64>() / len;

    let variance: f64 = returns
        .iter()
        .map(|r| {
            let diff = r - mean;
            diff * diff
        })
        .sum::<f64>()
        / len;

    let std = variance.sqrt();

    if std == 0.0 {
        if mean > 0.0 {
            return 999.0;
        }
        if mean < 0.0 {
            return -999.0;
        }
        return 0.0;
    }

    mean / std
}

fn fill_trade_metrics(metrics: &mut Metrics, events: &[TradeEvent]) {
    let mut total_trades = 0;
    let mut win_trades = 0;
    let mut loss_trades = 0;
    let mut total_win_amount = 0.0;
    let mut total_loss_amount = 0.0;

    for evt in events {
        let mut include = evt.liquidation_flag || evt.action.starts_with("close");
        if evt.realized_pnl != 0.0 {
            include = true;
        }

        if !include {
            continue;
        }

        total_trades += 1;

        // Get mutable reference to symbol stats (create if not exists)
        let stats = metrics.symbol_stats.entry(evt.symbol.clone()).or_default();

        stats.total_trades += 1;
        stats.total_pnl += evt.realized_pnl;

        if evt.realized_pnl > 0.0 {
            win_trades += 1;
            total_win_amount += evt.realized_pnl;
            stats.winning_trades += 1;
        } else if evt.realized_pnl < 0.0 {
            loss_trades += 1;
            total_loss_amount += -evt.realized_pnl; // make positive
            stats.losing_trades += 1;
        }
    }

    metrics.trades = total_trades;
    if total_trades > 0 {
        metrics.win_rate = (win_trades as f64 / total_trades as f64) * 100.0;
    }

    if win_trades > 0 {
        metrics.avg_win = total_win_amount / win_trades as f64;
    }

    if loss_trades > 0 {
        metrics.avg_loss = -(total_loss_amount / loss_trades as f64);
    }

    if total_loss_amount > 0.0 {
        metrics.profit_factor = total_win_amount / total_loss_amount;
    } else if total_win_amount > 0.0 {
        metrics.profit_factor = 999.0;
    }

    let mut best_symbol = String::new();
    let mut best_pnl = f64::NEG_INFINITY;
    let mut worst_symbol = String::new();
    let mut worst_pnl = f64::INFINITY;

    // Recalculate derived stats for each symbol
    for (symbol, stats) in &mut metrics.symbol_stats {
        if stats.total_trades > 0 {
            if stats.total_pnl > best_pnl {
                best_pnl = stats.total_pnl;
                best_symbol = symbol.clone();
            }
            if stats.total_pnl < worst_pnl {
                worst_pnl = stats.total_pnl;
                worst_symbol = symbol.clone();
            }

            stats.avg_pnl = stats.total_pnl / stats.total_trades as f64;
            stats.win_rate = (stats.winning_trades as f64 / stats.total_trades as f64) * 100.0;
        }
    }

    metrics.best_symbol = if best_pnl == f64::NEG_INFINITY {
        String::new()
    } else {
        best_symbol
    };
    metrics.worst_symbol = if worst_pnl == f64::INFINITY {
        String::new()
    } else {
        worst_symbol
    };
}
