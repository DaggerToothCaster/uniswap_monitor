# UniswapV2 交易监控服务

这是一个基于Rust开发的UniswapV2交易监控服务，采用模块化架构设计，支持独立部署和组合部署。

## 🏗️ 架构设计

### 服务拆分
- **事件监听服务** (`event-service`): 专门负责监听区块链事件
- **API服务** (`api-service`): 提供REST API和WebSocket接口
- **组合服务** (`combined-service`): 同时运行事件监听和API服务

### 模块化结构
```
src/
├── lib.rs                    # 库入口
├── main.rs                   # 默认主程序 (组合模式)
├── bin/                      # 独立可执行文件
│   ├── event_service.rs      # 事件监听服务
│   ├── api_service.rs        # API服务
│   └── combined_service.rs   # 组合服务
├── types/                    # 类型定义
│   ├── mod.rs
│   ├── models.rs             # 数据模型
│   ├── events.rs             # 事件类型
│   └── api_types.rs          # API类型
├── config/                   # 配置管理
│   └── mod.rs
├── database/                 # 数据库操作
│   ├── mod.rs
│   ├── operations.rs         # 数据库操作
│   └── utils.rs              # 工具函数
├── event_listener/           # 事件监听
│   ├── mod.rs
│   ├── base_listener.rs      # 基础监听器
│   ├── factory_listener.rs   # 工厂事件监听
│   └── swap_listener.rs      # 交换事件监听
├── api/                      # API服务
│   ├── mod.rs
│   ├── routes.rs             # 路由定义
│   ├── handlers.rs           # 处理函数
│   └── websocket.rs          # WebSocket处理
└── services/                 # 服务层
    ├── mod.rs
    ├── event_service.rs      # 事件服务
    └── api_service.rs        # API服务
```

## 🚀 快速开始

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

### 3. 数据库设置

```bash
createdb uniswap_monitor
psql -d uniswap_monitor -f migrations/001_initial.sql
```

### 4. 运行服务

#### 方式一：组合服务（推荐用于开发）
```bash
# 默认方式
cargo run

# 或者显式指定
cargo run --bin combined-service
```

#### 方式二：独立服务（推荐用于生产）
```bash
# 启动事件监听服务
cargo run --bin event-service

# 在另一个终端启动API服务
cargo run --bin api-service
```

#### 方式三：仅启动特定服务
```bash
# 仅事件监听
cargo run --bin event-service

# 仅API服务
cargo run --bin api-service
```

## 🔧 主要改进

### 1. 数据类型统一
- 将 `BigDecimal` 替换为 `rust_decimal::Decimal`
- 提供更好的性能和精度控制

### 2. 事件监听分离
- **工厂事件监听器**: 专门处理新交易对创建事件
- **交换事件监听器**: 专门处理Swap、Mint、Burn事件
- 独立的区块处理和错误恢复机制

### 3. 服务独立部署
- 事件监听和API服务可以独立启动/停止
- 支持水平扩展和独立维护
- 通过共享数据库和消息通道保持数据一致性

### 4. 模块化架构
- 按功能拆分文件，避免单文件过大
- 清晰的依赖关系和接口定义
- 便于单元测试和集成测试

## 📊 API接口

### REST API

#### 获取所有交易对
```
GET /api/pairs?chain_id=1
```

#### 获取K线数据
```
GET /api/pairs/{chain_id}/{address}/kline?interval=1h&limit=100
```

#### 获取交易记录
```
GET /api/pairs/{chain_id}/{address}/trades?limit=50&offset=0
```

#### 获取流动性记录
```
GET /api/pairs/{chain_id}/{address}/liquidity?limit=50&offset=0
```

#### 获取钱包交易
```
GET /api/wallets/{address}/transactions?chain_id=1&limit=50
```

### WebSocket

连接到 `ws://localhost:3000/api/ws` 可以实时接收事件数据。

## 🛠️ 开发

### 构建
```bash
cargo build --release
```

### 测试
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

## 🚀 部署

### Docker部署

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# 复制所有二进制文件
COPY --from=builder /app/target/release/event-service /usr/local/bin/
COPY --from=builder /app/target/release/api-service /usr/local/bin/
COPY --from=builder /app/target/release/combined-service /usr/local/bin/

# 默认运行组合服务
CMD ["combined-service"]
```

### 分离部署

#### 事件监听服务
```bash
# 构建事件监听服务
cargo build --release --bin event-service

# 运行
./target/release/event-service
```

#### API服务
```bash
# 构建API服务
cargo build --release --bin api-service

# 运行
./target/release/api-service
```

### 在MAC上构建Linux运行程序
1. 确保 musl 工具链完整安装
```bash
# 安装 musl 交叉编译器 (使用 Homebrew)
brew install FiloSottile/musl-cross/musl-cross

# 或者安装更完整的工具链
brew install x86_64-unknown-linux-musl
```
2. 配置 Cargo 正确使用链接器
编辑或创建 ~/.cargo/config 文件，添加：

```bash
[target.x86_64-unknown-linux-musl]
linker = "x86_64-linux-musl-gcc"
ar = "x86_64-linux-musl-ar"
```
3. 设置必要的环境变量
```bash
# 对于 ring 等加密库特别重要
export CC_x86_64_unknown_linux_musl="x86_64-linux-musl-gcc"
export AR_x86_64_unknown_linux_musl="x86_64-linux-musl-ar"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="x86_64-linux-musl-gcc"
```
4. 处理特殊 crate (如 ring)
```bash
# 为 ring crate 设置特殊环境变量
export TARGET_CC="x86_64-linux-musl-gcc"
export TARGET_AR="x86_64-linux-musl-ar"
export RING_COMLETION="x86_64-unknown-linux-musl"
```
5. 清理并重新构建
```bash
cargo clean
cargo build --release --target x86_64-unknown-linux-musl -v
```
## 🔍 监控和日志

服务使用 `tracing` 库进行日志记录。日志级别可以通过 `RUST_LOG` 环境变量控制：

```bash
RUST_LOG=info cargo run --bin event-service
RUST_LOG=debug cargo run --bin api-service
```

## 📈 性能优化

### 事件监听优化
- 独立的工厂和交换事件监听器
- 可配置的批次大小
- 智能错误恢复和重试机制

### API服务优化
- 连接池管理
- 查询优化和索引
- WebSocket连接管理

### 数据库优化
- 合理的索引设计
- 分页查询支持
- 连接池配置

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 许可证

MIT License
