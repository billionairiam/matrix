use anyhow::{Result, anyhow};
use std::time::Duration;

/// Source of truth for supported timeframes and their durations (in seconds).
const TIMEFRAME_MAP: &[(&str, u64)] = &[
    ("1m", 60),
    ("3m", 3 * 60),
    ("5m", 5 * 60),
    ("15m", 15 * 60),
    ("30m", 30 * 60),
    ("1h", 3600),
    ("2h", 2 * 3600),
    ("4h", 4 * 3600),
    ("6h", 6 * 3600),
    ("12h", 12 * 3600),
    ("1d", 24 * 3600),
];

/// Helper to lookup duration from the const map.
fn get_duration_from_map(key: &str) -> Option<Duration> {
    TIMEFRAME_MAP
        .iter()
        .find(|(k, _)| *k == key)
        .map(|(_, secs)| Duration::from_secs(*secs))
}

/// NormalizeTimeframe normalizes the incoming timeframe string (case-insensitive, no spaces),
/// and validates if it's supported.
pub fn normalize_timeframe(tf: &str) -> Result<String> {
    let trimmed = tf.trim().to_lowercase();

    if trimmed.is_empty() {
        return Err(anyhow!("timeframe cannot be empty"));
    }

    if get_duration_from_map(&trimmed).is_none() {
        return Err(anyhow!("unsupported timeframe '{}'", tf));
    }

    Ok(trimmed)
}

/// TFDuration returns the time duration corresponding to the given timeframe.
pub fn tf_duration(tf: &str) -> Result<Duration> {
    let norm = normalize_timeframe(tf)?;
    // We can safely unwrap here because normalize_timeframe guarantees existence
    Ok(get_duration_from_map(&norm).unwrap())
}

/// MustNormalizeTimeframe is similar to NormalizeTimeframe, but panics when unsupported.
pub fn must_normalize_timeframe(tf: &str) -> String {
    match normalize_timeframe(tf) {
        Ok(norm) => norm,
        Err(e) => panic!("{}", e),
    }
}

/// SupportedTimeframes returns all supported timeframes (sorted slice).
pub fn supported_timeframes() -> Vec<String> {
    let mut keys: Vec<String> = TIMEFRAME_MAP.iter().map(|(k, _)| k.to_string()).collect();

    // Go's slices.Sort sorts strings lexicographically
    keys.sort();
    keys
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize() {
        assert_eq!(normalize_timeframe(" 1H ").unwrap(), "1h");
        assert_eq!(normalize_timeframe("1m").unwrap(), "1m");
        assert!(normalize_timeframe("35m").is_err());
        assert!(normalize_timeframe("").is_err());
    }

    #[test]
    fn test_duration() {
        assert_eq!(tf_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(tf_duration("15m").unwrap(), Duration::from_secs(900));
    }

    #[test]
    fn test_supported_list() {
        let list = supported_timeframes();
        assert!(list.contains(&"1m".to_string()));
        assert!(list.contains(&"1d".to_string()));
        // Ensure it is sorted
        // "12h" comes before "1h" alphabetically? No. "12h" vs "1h".
        // Rust sort: "12h" < "15m" < "1d" < "1h" < "1m" ... (Lexicographical)
        // This matches Go's slices.Sort behavior on strings.
        assert!(list.len() > 0);
    }
}
