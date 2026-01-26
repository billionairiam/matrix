use std::collections::HashMap;
use std::f64;

use crate::types::PositionSnapshot;
use anyhow::{Result, anyhow};

const EPSILON: f64 = 1e-8;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Side {
    Long,
    Short,
}

impl Side {
    pub fn as_str(&self) -> &str {
        match self {
            Side::Long => "long",
            Side::Short => "short",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "long" => Some(Side::Long),
            "short" => Some(Side::Short),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub side: Side,
    pub quantity: f64,
    pub entry_price: f64,
    pub leverage: i32,
    pub margin: f64,
    pub notional: f64,
    pub liquidation_price: f64,
    pub open_time: i64,
}

#[derive(Debug, Clone)]
pub struct BacktestAccount {
    initial_balance: f64,
    cash: f64,
    fee_rate: f64,
    slippage_rate: f64,
    positions: HashMap<String, Position>,
    realized_pnl: f64,
}

impl BacktestAccount {
    /// Create a new backtest account.
    /// fee_bps: Basis points (e.g., 10 = 0.1%)
    /// slippage_bps: Basis points
    pub fn new(initial_balance: f64, fee_bps: f64, slippage_bps: f64) -> Self {
        BacktestAccount {
            initial_balance,
            cash: initial_balance,
            fee_rate: fee_bps / 10000.0,
            slippage_rate: slippage_bps / 10000.0,
            positions: HashMap::new(),
            realized_pnl: 0.0,
        }
    }

    fn position_key(symbol: &str, side: &Side) -> String {
        format!("{}:{}", symbol.to_uppercase(), side.as_str())
    }

    /// Open a position.
    /// Returns: (Position Reference, Fee, Execution Price)
    pub fn open(
        &mut self,
        symbol: &str,
        side_str: &str,
        quantity: f64,
        leverage: i32,
        price: f64,
        ts: i64,
    ) -> Result<(&Position, f64, f64)> {
        if quantity <= 0.0 {
            return Err(anyhow!("quantity must be positive"));
        }
        if leverage <= 0 {
            return Err(anyhow!("leverage must be positive"));
        }

        let side = Side::from_str(side_str).ok_or_else(|| anyhow!("invalid side: {}", side_str))?;

        let exec_price = apply_slippage(price, self.slippage_rate, &side, true);
        let notional = exec_price * quantity;
        let margin = notional / leverage as f64;
        let fee = notional * self.fee_rate;

        if margin + fee > self.cash + EPSILON {
            return Err(anyhow!("insufficient cash: need {:.2}", margin + fee));
        }

        self.cash -= margin + fee;

        let key = Self::position_key(symbol, &side);
        let symbol_upper = symbol.to_uppercase();

        // Use standard HashMap Entry API
        let pos = self.positions.entry(key).or_insert(Position {
            symbol: symbol_upper,
            side: side.clone(),
            quantity: 0.0,
            entry_price: 0.0,
            leverage: 0,
            margin: 0.0,
            notional: 0.0,
            liquidation_price: 0.0,
            open_time: 0,
        });

        if pos.quantity < EPSILON {
            // New position
            pos.quantity = quantity;
            pos.entry_price = exec_price;
            pos.leverage = leverage;
            pos.margin = margin;
            pos.notional = notional;
            pos.open_time = ts;
            pos.liquidation_price = compute_liquidation(exec_price, leverage, &side);
        } else {
            // Increase existing position
            if leverage != pos.leverage {
                // Weighted average leverage approximation
                let weighted_margin = pos.margin + margin;
                if weighted_margin > 0.0 {
                    let total_notional = pos.notional + notional;
                    pos.leverage = (total_notional / weighted_margin).round() as i32;
                }
            }

            // Weighted Average Price
            pos.entry_price = ((pos.entry_price * pos.quantity) + (exec_price * quantity))
                / (pos.quantity + quantity);

            pos.notional += notional;
            pos.margin += margin;
            pos.quantity += quantity;
            pos.liquidation_price = compute_liquidation(pos.entry_price, pos.leverage, &side);
        }

        Ok((pos, fee, exec_price))
    }

    /// Close a position.
    /// Returns: (Realized PnL, Fee, Execution Price)
    pub fn close(
        &mut self,
        symbol: &str,
        side_str: &str,
        mut quantity: f64,
        price: f64,
    ) -> Result<(f64, f64, f64)> {
        let side = Side::from_str(side_str).ok_or_else(|| anyhow!("invalid side: {}", side_str))?;

        let key = Self::position_key(symbol, &side);

        // We need to mutate the position, but also potentially remove it.
        // We'll get a mutable reference first.
        let pos = self
            .positions
            .get_mut(&key)
            .ok_or_else(|| anyhow!("no active {} position for {}", side_str, symbol))?;

        if pos.quantity <= EPSILON {
            return Err(anyhow!("no active {} position for {}", side_str, symbol));
        }

        // Handle logic: if qty is 0 or close to 0, close entire position
        if quantity <= 0.0 || quantity > pos.quantity + EPSILON {
            if quantity.abs() <= EPSILON {
                quantity = pos.quantity;
            } else {
                return Err(anyhow!("invalid close quantity"));
            }
        }

        let exec_price = apply_slippage(price, self.slippage_rate, &side, false);
        let notional = exec_price * quantity;
        let fee = notional * self.fee_rate;

        let realized = calculate_realized_pnl(pos, quantity, exec_price);

        let margin_portion = pos.margin * (quantity / pos.quantity);

        // Update Account State
        self.cash += margin_portion + realized - fee;
        self.realized_pnl += realized - fee;

        // Update Position State
        pos.quantity -= quantity;
        pos.notional -= notional;
        pos.margin -= margin_portion;

        let should_remove = pos.quantity <= EPSILON;

        // Drop the mutable borrow so we can remove if needed
        if should_remove {
            self.positions.remove(&key);
        }

        Ok((realized, fee, exec_price))
    }

    /// Returns (Total Equity, Unrealized PnL, PnL per symbol map)
    pub fn total_equity(
        &self,
        price_map: &HashMap<String, f64>,
    ) -> (f64, f64, HashMap<String, f64>) {
        let mut unrealized = 0.0;
        let mut margin = 0.0;
        let mut per_symbol = HashMap::new();

        for pos in self.positions.values() {
            // In Go code, it iterates positions. If price is missing, it implies 0.0 or panic?
            // Assuming 0.0 if not found to be safe, or logic depends on caller ensuring map existence.
            let price = *price_map.get(&pos.symbol).unwrap_or(&0.0);

            let pnl = calculate_unrealized_pnl(pos, price);
            unrealized += pnl;
            margin += pos.margin;

            let key = format!("{}:{}", pos.symbol, pos.side.as_str());
            per_symbol.insert(key, pnl);
        }

        (self.cash + margin + unrealized, unrealized, per_symbol)
    }

    pub fn positions(&self) -> Vec<&Position> {
        self.positions.values().collect()
    }

    pub fn position_leverage(&self, symbol: &str, side_str: &str) -> i32 {
        if let Some(side) = Side::from_str(side_str) {
            let key = Self::position_key(symbol, &side);
            if let Some(pos) = self.positions.get(&key) {
                if pos.quantity > EPSILON {
                    return pos.leverage;
                }
            }
        }
        0
    }

    // Getters
    pub fn cash(&self) -> f64 {
        self.cash
    }
    pub fn initial_balance(&self) -> f64 {
        self.initial_balance
    }
    pub fn realized_pnl(&self) -> f64 {
        self.realized_pnl
    }

    /// Restore from snapshots (Checkpointing)
    pub fn restore_from_snapshots(
        &mut self,
        cash: f64,
        realized: f64,
        snaps: &Vec<PositionSnapshot>,
    ) {
        self.cash = cash;
        self.realized_pnl = realized;
        self.positions.clear();

        for snap in snaps {
            // Attempt to parse side string from snapshot
            if let Some(side) = Side::from_str(&snap.side) {
                let pos = Position {
                    symbol: snap.symbol.clone(),
                    side: side.clone(),
                    quantity: snap.quantity,
                    entry_price: snap.avg_price,
                    leverage: snap.leverage,
                    margin: snap.margin_used,
                    notional: snap.quantity * snap.avg_price,
                    liquidation_price: snap.liquidation_price,
                    open_time: snap.open_time,
                };
                let key = Self::position_key(&pos.symbol, &side);
                self.positions.insert(key, pos);
            }
        }
    }
}

fn apply_slippage(price: f64, rate: f64, side: &Side, is_open: bool) -> f64 {
    if rate <= 0.0 {
        return price;
    }
    let mut adjust = 1.0;

    match side {
        Side::Long => {
            if is_open {
                adjust += rate; // Buy: Price goes up (worse)
            } else {
                adjust -= rate; // Sell: Price goes down (worse)
            }
        }
        Side::Short => {
            if is_open {
                adjust -= rate; // Sell: Price goes down (worse)
            } else {
                adjust += rate; // Buy: Price goes up (worse)
            }
        }
    }
    price * adjust
}

fn compute_liquidation(entry: f64, leverage: i32, side: &Side) -> f64 {
    if leverage <= 0 {
        return 0.0;
    }
    let lev = leverage as f64;
    match side {
        Side::Long => entry * (1.0 - 1.0 / lev),
        Side::Short => entry * (1.0 + 1.0 / lev),
    }
}

fn calculate_realized_pnl(pos: &Position, qty: f64, price: f64) -> f64 {
    match pos.side {
        Side::Long => (price - pos.entry_price) * qty,
        Side::Short => (pos.entry_price - price) * qty,
    }
}

pub fn calculate_unrealized_pnl(pos: &Position, price: f64) -> f64 {
    match pos.side {
        Side::Long => (price - pos.entry_price) * pos.quantity,
        Side::Short => (pos.entry_price - price) * pos.quantity,
    }
}
