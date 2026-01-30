# Binance Futures 订单测试工具

## 推荐方式：运行 Examples 程序

由于 `binance-rs` 库内部的运行时管理，集成测试可能会产生运行时冲突。建议使用独立的 examples 程序来测试。

## 快速开始

### 设置环境变量

```bash
export BINANCE_API_KEY="your-api-key"
export BINANCE_SECRET_KEY="your-secret-key"
```

### 测试命令

```bash
cd /home/liang/matrix

# 测试账户余额
cargo run --example test_orders --release -- balance

# 测试获取持仓
cargo run --example test_orders --release -- positions

# 测试开仓 Long（0.001 BTC, 5倍杠杆）
cargo run --example test_orders --release -- long BTCUSDT 0.001 5

# 测试开仓 Short（0.01 ETH, 5倍杠杆）
cargo run --example test_orders --release -- short ETHUSDT 0.01 5

# 测试平仓 Long
cargo run --example test_orders --release -- close-long BTCUSDT

# 测试平仓 Short
cargo run --example test_orders --release -- close-short ETHUSDT

# 测试设置杠杆（10倍）
cargo run --example test_orders --release -- leverage BTCUSDT 10 10
```

## 命令行参数说明

```
cargo run --example test_orders --release -- [action] [symbol] [quantity] [leverage]
```

参数说明：
- `action` - 操作类型：
  - `balance` - 获取账户余额
  - `positions` - 获取开仓头寸
  - `long` - 开仓多头（需要 symbol, quantity, leverage）
  - `short` - 开仓空头（需要 symbol, quantity, leverage）
  - `close-long` - 平仓多头（需要 symbol）
  - `close-short` - 平仓空头（需要 symbol）
  - `leverage` - 设置杠杆（需要 symbol, leverage）

- `symbol` - 交易对，默认 BTCUSDT
- `quantity` - 数量，默认 0.001
- `leverage` - 杠杆倍数，默认 5

## 测试流程建议

### 1️⃣ 验证连接 - 测试余额

```bash
cargo run --example test_orders --release -- balance
```

输出示例：
```
✓ get_balance succeeded!
  Total Wallet Balance: 1000.0
  Available Balance: 950.0
  Unrealized Profit: 10.0
```

### 2️⃣ 查看持仓 - 测试获取持仓

```bash
cargo run --example test_orders --release -- positions
```

### 3️⃣ 测试开仓 - 开小额多头

```bash
cargo run --example test_orders --release -- long BTCUSDT 0.001 5
```

### 4️⃣ 验证持仓 - 确认开仓成功

```bash
cargo run --example test_orders --release -- positions
```

### 5️⃣ 测试平仓 - 平掉测试头寸

```bash
cargo run --example test_orders --release -- close-long BTCUSDT
```

## 环境要求

- Rust 1.70+
- tokio (async runtime)
- binance-rs (Binance API client)

## 文件位置

- 测试工具：`/home/liang/matrix/src/trader/examples/test_orders.rs`
- 测试说明：`/home/liang/matrix/src/trader/TESTING.md`

## 注意事项

⚠️ **重要警告**：这些测试会真正执行交易操作！

- ✅ 首先在 **Testnet** 账户进行测试
- ✅ 使用非常小的交易数量（如 0.001 BTC）
- ✅ 确保账户有足够的保证金
- ✅ 测试完成后及时平仓所有持仓
- ❌ 不要在生产账户上运行未经验证的代码

## 常见问题

### 问题1："-4061" 错误（头寸模式不匹配）
```
Error: Order's position side does not match user's setting.
```

**解决方案**：
1. 确认账户已启用对冲模式（Hedge Mode）
2. 检查 Binance 网页界面账户设置
3. 确认 API 密钥权限正确

### 问题2：保证金不足错误
```
Error: Insufficient margin. Required: 4969.72, Available: 4959.88
```

**解决方案**：
1. 检查账户当前余额（使用 balance 操作）
2. 减少交易数量
3. 降低杠杆倍数

### 问题3：API 密钥错误
```
Error: Unauthorized - Invalid API Key
```

**解决方案**：
1. 确认 `BINANCE_API_KEY` 和 `BINANCE_SECRET_KEY` 正确设置
2. 检查 API 密钥是否已过期
3. 验证 API 密钥权限（需要期货交易权限）

## 调试技巧

运行带完整日志的测试：

```bash
# 启用详细日志
RUST_LOG=debug cargo run --example test_orders --release -- balance

# 启用完整 backtrace
RUST_BACKTRACE=1 cargo run --example test_orders --release -- long BTCUSDT 0.001 5
```

## 下一步

- 在成功测试后，集成到自动交易系统中
- 在 `auto_trader.rs` 中验证 `open_long` 和 `open_short` 的调用
- 监控完整的交易流程

