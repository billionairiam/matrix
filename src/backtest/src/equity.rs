use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::types::{EquityPoint, TradeEvent};
use anyhow::Result;
use market::timeframe::tf_duration;

/// Resamples equity curve based on timeframe.
/// Uses BTreeMap to automatically handle sorting of bucket keys.
pub fn resample_equity(points: Vec<EquityPoint>, timeframe: &str) -> Result<Vec<EquityPoint>> {
    if timeframe.is_empty() {
        return Ok(points);
    }

    // Call mock market duration function
    let dur = tf_duration(timeframe)?;

    if points.is_empty() {
        return Ok(points);
    }

    let dur_ms = dur.as_millis();
    if dur_ms <= 0 {
        return Ok(points);
    }

    // BTreeMap keeps keys (buckets) sorted automatically
    let mut bucket_map: BTreeMap<i64, EquityPoint> = BTreeMap::new();

    for pt in points {
        // Integer division truncates, grouping by interval
        let bucket = (pt.timestamp / dur_ms as i64) * dur_ms as i64;

        let mut bucket_point = pt.clone();
        bucket_point.timestamp = bucket;

        // Insert or overwrite (Go logic overwrites with the last point found for that bucket)
        bucket_map.insert(bucket, bucket_point);
    }

    // Collect values in key-sorted order
    Ok(bucket_map.into_values().collect())
}

/// Limits the number of data points within a given range (uniform sampling).
pub fn limit_equity_points(points: Vec<EquityPoint>, limit: usize) -> Vec<EquityPoint> {
    if limit == 0 || points.len() <= limit {
        return points;
    }

    let len = points.len();
    let step = len as f64 / limit as f64;
    let mut result = Vec::with_capacity(limit);

    for i in 0..limit {
        let mut idx = (step * i as f64).round() as usize;
        if idx >= len {
            idx = len - 1;
        }
        // We clone here because we are creating a new subset
        result.push(points[idx].clone());
    }

    result
}

/// Limits the number of trade events (uniform sampling).
/// Duplicate logic of limit_equity_points but for TradeEvent type.
pub fn limit_trade_events(events: Vec<TradeEvent>, limit: usize) -> Vec<TradeEvent> {
    if limit == 0 || events.len() <= limit {
        return events;
    }

    let len = events.len();
    let step = len as f64 / limit as f64;
    let mut result = Vec::with_capacity(limit);

    for i in 0..limit {
        let mut idx = (step * i as f64).round() as usize;
        if idx >= len {
            idx = len - 1;
        }
        result.push(events[idx].clone());
    }

    result
}

/// Ensures timestamps are sorted in ascending order.
pub fn align_equity_timestamps(mut points: Vec<EquityPoint>) -> Vec<EquityPoint> {
    points.sort_by(|a, b| {
        if a.timestamp < b.timestamp {
            Ordering::Less
        } else if a.timestamp > b.timestamp {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    });
    points
}
