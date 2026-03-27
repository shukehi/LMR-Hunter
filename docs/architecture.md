# 架构文档

## 1. 文档目的

本文档定义 LMR-Hunter 的系统边界、模块职责、数据流、接口原则、状态机和持久化设计。

本文件回答的是“系统如何实现”。

## 2. 运行模式

系统分为四种运行模式：

### 2.1 Observe Mode

只采集数据，不生成交易意图。

用途：

- 验证行情接入稳定性
- 验证原始数据质量
- 验证特征计算是否正确
- 从第一天开始建立实时强平数据仓库

### 2.2 Shadow Mode

生成虚拟订单和虚拟成交，不向交易所发送真实委托。

用途：

- 评估信号质量
- 评估挂单距离
- 评估不同延迟场景（本地 ~300ms vs VPS ~50ms）对成交率的影响
- 在模拟撮合中反映真实延迟惩罚
- 验证多级建仓模式在低延迟下的效果

### 2.3 Testnet Mode

接入测试网，验证完整的订单执行链路。

用途：

- 验证签名与鉴权
- 验证委托、撤单、成交回报
- 验证状态恢复和异常处理

注意：

- 测试网不用于验证策略有效性
- 测试网仅用于验证工程流程

### 2.4 Live Mode

使用最小仓位接入实盘。

进入 Live Mode 的前提在 [风控文档](risk-controls.md) 中定义。

## 3. 模块划分

系统至少拆分为以下模块：

### 3.1 Market Data Gateway

职责：

- 建立公共行情 WebSocket 连接
- 标准化强平、成交、可选深度、K 线数据
- 向下游模块广播标准事件

### 3.2 Feature Engine

职责：

- 计算滑动窗口强平聚合
- 计算 `VWAP_15M` 基准价
- 计算价格偏离率
- 在启用时计算盘口深度摘要

### 3.3 Signal Engine

职责：

- 组合事件层、偏离层，以及可选的流动性层信号
- 判断是否生成交易意图
- 生成标准化信号对象

### 3.4 Shadow Simulator

职责：

- 在非实盘模式下模拟挂单、成交、退出
- 记录不同参数下的理论结果
- 在撮合判断中加入配置化的延迟惩罚

### 3.5 Execution Engine

职责：

- 只接收已通过预交易风控审核的订单意图
- 发送下单和撤单请求
- 处理订单状态变化
- 同步持仓与执行结果
- 支持“Maker 入场、风险优先退出”的执行约束
- 在 `PARTIALLY_FILLED` 场景下执行剩余数量的保守处置

### 3.6 Risk Engine

职责：

- 控制仓位、日损、暂停、熔断
- 在策略层和账户层拦截危险操作
- 在下单前执行预交易风控审核

### 3.7 Persistence Layer

职责：

- 保存原始事件、信号、订单、成交、风控事件
- 提供回放和审计数据源

### 3.8 Monitoring and Alerting

职责：

- 输出运行日志
- 健康检查
- 错误告警
- 服务心跳

## 4. 数据流

```text
Market WSS/REST -> Market Data Gateway -> Feature Engine -> Signal Engine -> Risk Engine
                                                                             -> Shadow Simulator
                                                                             -> Execution Engine

Execution Engine -> User Data Stream -> Position State -> Risk Engine
Risk Engine -> Execution Engine / Shadow Simulator (approved intents only)

All Modules -> Persistence Layer -> Replay / Metrics / Audit
```

## 5. 时间语义

系统必须同时保留两类时间戳：

- `Exchange Time`
  - 来自交易所 payload 的事件时间，例如 `E` 或交易时间 `T`
- `Local Receipt Time`
  - 本地进程实际收到消息并完成解码的时间

冻结规则：

- 滑动窗口聚合默认基于 `Exchange Time`
- 延迟度量默认基于 `Local Receipt Time - Exchange Time`
- 影子模式的延迟惩罚，也必须基于这两类时间戳计算
- 原始数据表必须同时落这两类时间戳，禁止只保留本地时间

## 6. 状态机

系统的状态至少分为两层：

主运行状态：

- `IDLE`
- `WATCHING`
- `COOLDOWN`
- `HALTED`

订单生命周期状态：

- `SIGNAL_READY`
- `RISK_REJECTED`
- `ORDER_PENDING`
- `PARTIALLY_FILLED`
- `ORDER_FILLED`
- `EXIT_PENDING`
- `CANCEL_PENDING`
- `CANCELLED`
- `REJECTED`
- `EXPIRED`
- `CLOSED`

状态要求：

- 任意时刻只能处于一个主状态
- 状态变迁必须落日志
- 状态变迁必须可回放
- 必须保留交易所原始状态与内部归一化状态的映射

`PARTIALLY_FILLED` 的 MVP 行为冻结如下：

- 一旦检测到订单部分成交，立即对剩余未成交数量发起撤单
- 剩余数量进入 `CANCEL_PENDING`
- 已成交数量独立转入持仓管理
- 对已成交部分单独执行止盈、时间止损和价格止损

该规则的目标是避免在第一次部分成交后，剩余挂单在二次下杀中继续扩大风险敞口

## 7. 数据源与接口能力

### 7.1 市场数据能力

MVP 必需的数据能力：

- 强平事件流
- 实时成交流
- K 线或成交聚合流

MVP 的研究前提：

- 不依赖交易所提供的全市场历史强平查询能力
- 通过自建实时仓库沉淀研究数据

`P3` 增强能力：

- 盘口深度流

### 7.2 账户能力

MVP 至少需要以下账户能力：

- 查询账户权益
- 查询当前持仓
- 设置杠杆
- 下单
- 撤单
- 查询当前挂单
- 监听订单回报

### 7.3 接口原则

接口设计必须遵循以下原则：

- 行情优先使用 WebSocket
- 订单状态优先使用私有用户流
- 交易指令必须幂等
- 接口错误必须可重试或可安全失败
- 所有交易相关精度必须配置化
- 系统重启后，必须先做 REST 状态对账，再恢复实时监听

### 7.4 交易所精度前置确认

编码前必须明确以下信息：

- 最小下单数量
- 最小名义价值
- 价格精度
- 数量精度
- Post-Only 规则
- Maker/Taker 手续费
- 单向/双向持仓模式

### 7.5 重启对账流程

系统启动或异常恢复时，必须按以下顺序执行：

1. 暂不进入 `WATCHING`
2. 拉取当前持仓
3. 拉取当前挂单
4. 将 REST 返回结果与本地 SQLite 状态做对账
5. 修正本地状态中的孤儿持仓、孤儿订单、错误状态映射
6. 仅在对账完成后，才允许重新进入 `WATCHING`

Binance USDⓈ-M Futures 当前可用的关键对账接口：

- `GET /fapi/v2/positionRisk`
- `GET /fapi/v1/openOrders`

## 8. 持久化设计

### 8.1 存储方案

MVP 使用 `SQLite`。

后续如有需要，可迁移到 PostgreSQL，但不应在 MVP 阶段先行复杂化。

### 8.2 必要数据表

建议至少建立以下表：

- `raw_liquidations`
- `raw_trades`
- `raw_klines`
- `signals`
- `shadow_orders`
- `live_orders`
- `fills`
- `positions`
- `risk_events`
- `service_heartbeats`

Observe Mode 优化阶段新增（已落地）：

- `liquidation_episodes` — 连续冲击聚合（阶段 3）；`episode_id = "{symbol}_{start_event_ts}"` 可追溯到 `raw_liquidations`
- `episode_outcomes` — episode 结束后 15 分钟的价格路径观测（阶段 4）；由 `outcome_processor` 后台异步回填

启用深度增强后再增加：

- `raw_depth_snapshots`

### 8.3 核心审计字段

每一笔信号或订单至少需要保留：

- 触发时间
- 强平总额
- 强平方向
- 交易所事件时间
- 本地接收时间
- 当前价格
- 基准价格
- 偏离率
- 盘口深度摘要（若启用）
- 理论挂单价
- 实际挂单价
- 挂单延迟
- 是否成交
- 成交时间
- 最大有利偏移
- 最大不利偏移
- 退出时间
- 退出价格
- 退出原因
- 实际手续费
- 实际滑点
- 最终盈亏

### 8.4 影子审计要求

Shadow Mode 下，即使没有成交，也要保留以下信息：

- 若挂单更近是否会成交
- 若挂单更远是否能降低回撤
- 若不做止盈止损，后续路径如何演化
- 若止损采用主动退出，理论损失与被动退出的差异

Shadow Mode 的撮合判断冻结如下：

- 若信号在交易所时间 `T` 触发，系统本地配置延迟为 `latency_ms`
- 模拟挂单的生效时间不得早于 `T + latency_ms`
- 只有在该时点之后的市场数据满足成交条件，才允许视为“可成交”
- 禁止用 `T` 时刻之前或同时刻的成交数据回填虚拟成交

## 9. 技术栈选型

### 9.1 编程语言

MVP 使用 `Python 3.11+`。

选择理由：

- 出 MVP 最快，个人项目开发效率优先
- WebSocket、REST、SQLite 生态成熟
- 数据分析和回放脚本可复用同一语言
- 策略逻辑的计算量极低（事件驱动、低频），Python 性能不构成瓶颈

不选 Rust/Go 的理由：

- MVP 阶段的首要目标是验证策略可行性，不是优化延迟
- 若策略验证通过且延迟成为瓶颈，再考虑核心路径用 Rust 重写

### 9.2 核心依赖

| 依赖 | 用途 | 说明 |
|------|------|------|
| `websockets` 或 `aiohttp` | WebSocket 连接 | 行情和用户流 |
| `httpx` 或 `aiohttp` | REST 请求 | 下单、撤单、对账 |
| `sqlite3`（标准库） | 数据持久化 | MVP 无需外部数据库 |
| `pydantic` | 配置与数据模型 | 参数校验、类型安全 |
| `structlog` 或 `loguru` | 结构化日志 | 审计与调试 |

可选依赖：

| 依赖 | 用途 | 阶段 |
|------|------|------|
| `ccxt` | 交易所抽象层 | 若需快速接入可用，但建议首版直连 Binance API 以控制行为 |
| `pandas` | 回放与参数研究 | 阶段 2 影子模式分析 |
| `numpy` | VWAP 和统计计算 | 阶段 2 |

### 9.3 项目结构建议

```
lmr-hunter/
├── config/          # 配置文件（YAML/TOML）
├── src/
│   ├── gateway/     # Market Data Gateway
│   ├── features/    # Feature Engine
│   ├── signal/      # Signal Engine
│   ├── shadow/      # Shadow Simulator
│   ├── execution/   # Execution Engine
│   ├── risk/        # Risk Engine
│   ├── persistence/ # Persistence Layer
│   └── monitor/     # Monitoring & Alerting
├── scripts/         # 回放、分析、研究脚本
├── tests/
└── data/            # SQLite 数据库文件
```

## 10. 部署环境与延迟预算

### 10.1 默认部署方案：东京 VPS

项目默认采用东京 VPS 部署，从阶段 1 开始即在 VPS 上运行。

| 项目 | 推荐值 |
|------|--------|
| 区域 | 东京（`ap-northeast-1`） |
| 供应商 | Vultr / Linode / AWS Lightsail |
| 配置 | 1-2 vCPU、2-4 GB RAM |
| 月成本 | ~10-30 USD |
| 目标延迟 | WebSocket 单程 < 50ms |

选择东京的理由：

- Binance 匹配引擎部署在东京，物理距离最近
- 实测 WebSocket 延迟通常在 2-30ms
- 相比本地（~300ms），延迟降低 6-10 倍，解锁多项策略能力

### 10.2 延迟预算分解

| 环节 | 本地网络（参考） | 东京 VPS（默认） |
|------|-------------------|-------------------|
| 交易所事件 → 收到推送 | ~300ms | ~2-30ms |
| 信号计算 + 风控检查 | ~10-50ms | ~10-50ms |
| 下单请求 → 到达交易所 | ~300ms | ~2-30ms |
| **总延迟** | **~650-700ms** | **~20-110ms** |

### 10.3 低延迟解锁的能力

东京 VPS 部署（总延迟 < 100ms）相比本地部署（总延迟 ~700ms），解锁以下关键能力：

**1. 主动响应式下单**

- 300ms 下：必须提前挂单等待强平砸下来，位置靠猜
- 50ms 下：检测到强平信号后再下单，精度显著提升，成交率大幅提高

**2. 精确止损**

- 300ms 下：止损滑点预期 ~0.3%，每笔滑点损失 ~9 USD（3x 杠杆, 1000 USD 账户）
- 50ms 下：止损滑点预期 ~0.05%，每笔滑点损失 ~1.5 USD
- 盈亏平衡胜率从 ~51.6% 降至 ~48%

**3. 多级建仓**

- 300ms 下：三单总延迟 900ms，市场早已回弹或继续崩，不可行
- 50ms 下：三单总延迟 ~150ms，可在同一事件窗口内分批建仓，降低平均入场成本

**4. 多交易对监控**

- 300ms 下：WebSocket 延迟堆叠，监控 3-5 个交易对是极限
- 50ms 下：可同时监控 15-20 个交易对，捕获面扩大 4-5 倍
- 交易频率从每天 0-2 次可能提升到 3-8 次

**5. Orderbook 实时深度分析**

- 50ms 下可实时处理 Orderbook 增量更新
- 检测"买单墙被吃掉" → 判断强平规模
- 检测"卖单突然增厚" → 判断是否有更大抛压即将到来
- 信号质量从"有/无强平"升级到"强平多大、还会持续多久"

**6. 竞争排位**

- 同类"清算猎手"中，延迟决定排队顺序
- 50ms vs 300ms 的差距可能决定是否能成交
- 限价单在 Orderbook 中的排位更靠前

### 10.4 各阶段部署策略

| 阶段 | 部署位置 | 说明 |
|------|----------|------|
| 阶段 1（Observe） | 东京 VPS | 从第一天起采集真实低延迟数据，延迟分布更有参考价值 |
| 阶段 2（Shadow） | 东京 VPS | Shadow Mode 直接使用真实延迟，无需模拟延迟惩罚 |
| 阶段 3（Testnet） | 东京 VPS | 验证低延迟下的完整执行链路 |
| 阶段 4（Live） | 东京 VPS | 正式实盘 |

本地开发机仅用于代码编写和调试，不用于运行任何模式的长期任务。

### 10.5 延迟作为可配置参数

系统必须将延迟视为策略参数的一部分：

- `estimated_latency_ms`：配置在参数文件中，Shadow Mode 使用此值做延迟惩罚
- Observe Mode 阶段持续采集实际延迟分布（P50, P95, P99）
- 若实际延迟显著偏离配置值，应触发告警
- 部署后首要任务：实测 WebSocket 连接延迟，确认实际延迟 < 50ms

### 10.6 VPS 部署前置确认

在 VPS 上启动任何模式之前，必须完成以下确认：

- WebSocket 连接延迟实测 < 50ms（连续 1 小时，P95）
- 系统时钟与 NTP 同步，偏差 < 10ms
- SQLite 数据库文件的磁盘 I/O 不构成瓶颈
- 进程管理工具（`systemd` 或 `supervisor`）已配置自动重启

## 11. 部署与工程原则

项目默认采用以下工程原则：

- 先实现单进程、单标的、可回放的版本
- 先保证稳定和可审计，再考虑低延迟优化
- 所有参数必须配置化
- 所有关键动作必须落日志
- 所有订单相关流程必须支持重启恢复

## 12. 本文档边界

本文件不定义：

- 策略命题本身
- 失效条件和策略假设
- 账户级、策略级、系统级风控门槛
- 项目阶段验收标准

这些内容分别放在：

- [策略文档](strategy.md)
- [风控文档](risk-controls.md)
- [开发计划](development-plan.md)
