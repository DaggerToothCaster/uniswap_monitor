# UniswapV2 交易监控服务

这是一个基于Rust开发的UniswapV2交易监控服务，可以实时监控Factory合约和交易对的各种事件，并提供REST API和WebSocket接口给前端使用。

## 功能特性

- 🔍 监控UniswapV2 Factory合约的PairCreated事件
- 📊 监控每个交易对的Mint、Burn、Swap事件
- 💾 数据持久化存储到PostgreSQL数据库
- 🚀 提供REST API获取交易对列表和K线数据
- ⚡ WebSocket实时推送事件数据
- 📈 K线数据计算和聚合

## 技术栈

- **Rust** - 主要编程语言
- **Tokio** - 异步运行时
- **Ethers-rs** - 以太坊交互库
- **Axum** - Web框架
- **SQLx** - 数据库操作
- **PostgreSQL** - 数据库

## 快速开始

### 1. 环境准备

确保你已经安装了：
- Rust (1.70+)
- PostgreSQL
- 以太坊节点访问权限（Infura/Alchemy等）

### 2. 配置环境变量

复制 `.env.example` 到 `.env` 并填入你的配置：

```bash
cp .env.example .env
```

编辑 `.env` 文件：
```env
# Database Configuration
DATABASE_URL=postgresql://username:password@localhost/uniswap_monitor
DB_MAX_CONNECTIONS=10

# Ethereum Configuration
ETH_RPC_URL=https://mainnet.infura.io/v3/YOUR_INFURA_KEY
FACTORY_ADDRESS=0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f
START_BLOCK=10000835
POLL_INTERVAL=12  # 轮询间隔（秒）

# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=3000
```

### 3. 数据库设置

创建数据库：
```bash
createdb uniswap_monitor
```

运行迁移：
```bash
psql -d uniswap_monitor -f migrations/001_initial.sql
```

### 4. 运行服务

```bash
cargo run
```

服务将在 `http://localhost:3000` 启动。

## API 接口

### REST API

#### 获取所有交易对
```
GET /api/pairs
```

#### 获取K线数据
```
GET /api/pairs/{address}/kline?interval=1h&limit=100
```

参数：
- `interval`: 时间间隔 (1m, 5m, 15m, 1h, 4h, 1d)
- `limit`: 返回数量限制

### WebSocket

连接到 `ws://localhost:3000/api/ws` 可以实时接收事件数据。

## 项目结构

```
src/
├── main.rs          # 主程序入口
├── config.rs        # 配置管理
├── database.rs      # 数据库操作
├── models.rs        # 数据模型
├── event_listener.rs # 事件监听器
└── api.rs          # API服务器
```

## 开发

### 运行测试
```bash
cargo test
```

### 代码格式化
```bash
cargo fmt
```

### 代码检查
```bash
cargo clippy
```

## 部署

### Docker部署

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/uniswap-monitor /usr/local/bin/
CMD ["uniswap-monitor"]
```

### 系统服务

创建systemd服务文件 `/etc/systemd/system/uniswap-monitor.service`：

```ini
[Unit]
Description=UniswapV2 Monitor Service
After=network.target

[Service]
Type=simple
User=uniswap
WorkingDirectory=/opt/uniswap-monitor
ExecStart=/opt/uniswap-monitor/target/release/uniswap-monitor
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## 监控和日志

服务使用 `tracing` 库进行日志记录。日志级别可以通过 `RUST_LOG` 环境变量控制：

```bash
RUST_LOG=info cargo run
```

## 贡献

欢迎提交Issue和Pull Request！

## 许可证

MIT License
