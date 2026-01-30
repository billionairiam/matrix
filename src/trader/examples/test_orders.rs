use std::env;
use trader::binance_futures::FuturesTrader;
use trader::Trader;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    // Usage: program [action] [symbol] [quantity] [leverage] [api_key] [secret_key]
    let mut args = env::args().skip(1);
    
    let action = args.next().unwrap_or_else(|| "balance".to_string());
    let symbol = args.next().unwrap_or_else(|| "BTCUSDT".to_string());
    let quantity = args
        .next()
        .and_then(|q| q.parse::<f64>().ok())
        .unwrap_or(0.001);
    let leverage = args
        .next()
        .and_then(|l| l.parse::<i32>().ok())
        .unwrap_or(5);
    
    // Get API keys from command line or environment variables
    let api_key = args
        .next()
        .or_else(|| env::var("BINANCE_API_KEY").ok())
        .expect("BINANCE_API_KEY not provided and not set in environment");
    
    let secret_key = args
        .next()
        .or_else(|| env::var("BINANCE_SECRET_KEY").ok())
        .expect("BINANCE_SECRET_KEY not provided and not set in environment");

    println!("{}", "=".repeat(70));
    println!("Binance Futures Order Test Tool");
    println!("{}", "=".repeat(70));
    println!("Symbol: {}", symbol);
    println!("Quantity: {}", quantity);
    println!("Leverage: {}", leverage);
    println!("Action: {}", action);
    println!("{}", "=".repeat(70));

    let trader = FuturesTrader::new(&api_key, &secret_key);
    
    // Initialize trader
    println!("\n📍 Initializing trader...");
    match trader.init().await {
        Ok(_) => println!("✓ Trader initialized successfully"),
        Err(e) => {
            eprintln!("❌ Failed to initialize trader: {:?}", e);
            eprintln!("\n⚠️ WARNING: Trader initialization failed. Some operations may not work correctly.");
            eprintln!("   Attempting to continue anyway...\n");
        }
    }

    match action.as_str() {
        "balance" => test_balance(&trader).await,
        "positions" => test_positions(&trader).await,
        "long" => test_open_long(&trader, &symbol, quantity, leverage).await,
        "short" => test_open_short(&trader, &symbol, quantity, leverage).await,
        "leverage" => test_set_leverage(&trader, &symbol, leverage).await,
        "close-long" => test_close_long(&trader, &symbol).await,
        "close-short" => test_close_short(&trader, &symbol).await,
        _ => {
            println!("Unknown action: {}", action);
            println!("\nAvailable actions:");
            println!("  balance       - Get account balance");
            println!("  positions     - Get open positions");
            println!("  long          - Open long position");
            println!("  short         - Open short position");
            println!("  close-long    - Close long position");
            println!("  close-short   - Close short position");
            println!("  leverage      - Set leverage");
        }
    }

    println!("\n{}", "=".repeat(70));
}

async fn test_balance(trader: &FuturesTrader) {
    println!("\n📊 Testing get_balance...");
    match trader.get_balance().await {
        Ok(balance) => {
            println!("✓ get_balance succeeded!");
            if let Some(total) = balance.get("totalWalletBalance") {
                println!("  Total Wallet Balance: {}", total);
            }
            if let Some(available) = balance.get("availableBalance") {
                println!("  Available Balance: {}", available);
            }
            if let Some(unrealized) = balance.get("totalUnrealizedProfit") {
                println!("  Unrealized Profit: {}", unrealized);
            }
        }
        Err(e) => {
            eprintln!("❌ get_balance failed: {:?}", e);
        }
    }
}

async fn test_positions(trader: &FuturesTrader) {
    println!("\n📍 Testing get_positions...");
    match trader.get_positions().await {
        Ok(positions) => {
            println!("✓ get_positions succeeded!");
            println!("  Positions count: {}", positions.len());
            for pos in positions {
                if let Some(symbol) = pos.get("symbol") {
                    println!("  Symbol: {}", symbol);
                }
                if let Some(side) = pos.get("side") {
                    println!("    Side: {}", side);
                }
                if let Some(amt) = pos.get("positionAmt") {
                    println!("    Amount: {}", amt);
                }
                if let Some(entry) = pos.get("entryPrice") {
                    println!("    Entry Price: {}", entry);
                }
                if let Some(mark) = pos.get("markPrice") {
                    println!("    Mark Price: {}", mark);
                }
                if let Some(pnl) = pos.get("unRealizedProfit") {
                    println!("    Unrealized PnL: {}", pnl);
                }
            }
        }
        Err(e) => {
            eprintln!("❌ get_positions failed: {:?}", e);
        }
    }
}

async fn test_open_long(trader: &FuturesTrader, symbol: &str, quantity: f64, leverage: i32) {
    println!("\n📈 Testing open_long for {} with quantity {} and leverage {}...", symbol, quantity, leverage);
    match trader.open_long(symbol, quantity, leverage).await {
        Ok(result) => {
            println!("✓ open_long succeeded!");
            println!("Response: {:?}", result);
        }
        Err(e) => {
            eprintln!("❌ open_long failed: {:?}", e);
        }
    }
}

async fn test_open_short(trader: &FuturesTrader, symbol: &str, quantity: f64, leverage: i32) {
    println!("\n📉 Testing open_short for {} with quantity {} and leverage {}...", symbol, quantity, leverage);
    match trader.open_short(symbol, quantity, leverage).await {
        Ok(result) => {
            println!("✓ open_short succeeded!");
            println!("Response: {:?}", result);
        }
        Err(e) => {
            eprintln!("❌ open_short failed: {:?}", e);
        }
    }
}

async fn test_close_long(trader: &FuturesTrader, symbol: &str) {
    println!("\n🔄 Testing close_long for {}...", symbol);
    match trader.close_long(symbol, 0.0).await {
        Ok(result) => {
            println!("✓ close_long succeeded!");
            println!("Response: {:?}", result);
        }
        Err(e) => {
            eprintln!("❌ close_long failed: {:?}", e);
        }
    }
}

async fn test_close_short(trader: &FuturesTrader, symbol: &str) {
    println!("\n🔄 Testing close_short for {}...", symbol);
    match trader.close_short(symbol, 0.0).await {
        Ok(result) => {
            println!("✓ close_short succeeded!");
            println!("Response: {:?}", result);
        }
        Err(e) => {
            eprintln!("❌ close_short failed: {:?}", e);
        }
    }
}

async fn test_set_leverage(trader: &FuturesTrader, symbol: &str, leverage: i32) {
    println!("\n⚙️ Testing set_leverage for {} to {}x...", symbol, leverage);
    match trader.set_leverage(symbol, leverage).await {
        Ok(_) => {
            println!("✓ set_leverage succeeded!");
        }
        Err(e) => {
            eprintln!("❌ set_leverage failed: {:?}", e);
        }
    }
}
