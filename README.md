# LMR-Hunter

LMR-Hunter 是一个面向个人交易者的 BTC 永续合约自动化交易项目，核心目标是识别由强制平仓引发的短期流动性真空，并通过被动限价单参与价格修复。

## 文档入口

建议按以下顺序阅读：

1. [文档地图](docs/index.md)
2. [项目状态](docs/status.md)
3. [策略文档](docs/strategy.md)
4. [术语表](docs/glossary.md)
5. [架构文档](docs/architecture.md)
6. [风控文档](docs/risk-controls.md)
7. [开发计划](docs/development-plan.md)

审查与优化记录：

- [审查记录目录](docs/reviews/index.md)
- [第一轮结构性优化计划](docs/reviews/optimization-plan.md)
- [第二轮运行态审查计划](docs/reviews/runtime-optimization-plan.md)

## 当前状态

**Observe Mode 已上线运行（2026-03-27）**

系统正在 VPS 上持续采集 BTC 永续合约强平数据，核心功能已全部实现并通过测试：

- 阶段 1：数据完整性保证（写库重试 + 隔离文件 + DEGRADED 状态机）
- 阶段 2：时间语义修复（所有特征以交易所 event_ts 对齐）
- 阶段 3：Episode 模型（连续强平聚合为研究样本）
- 阶段 4：Outcome 回填（episode 结束 15min 后计算 MAE/MFE/rebound 指标）

当前阶段目标：积累 ≥20 个 episode 样本，验证研究数据可信性，为 Shadow Mode 准入建立基线。

完整状态、当前阻塞项和默认下一步见 [docs/status.md](docs/status.md)。

**本地开发快速上手：**

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

## 项目范围

MVP 只覆盖以下内容：

- 市场：`Binance Futures`
- 交易标的：`BTCUSDT` 永续合约
- 方向：优先支持多头抄底场景
- 执行方式：全自动监听、全自动挂单、全自动离场
- 运行模式：观察模式、影子模式、测试网模式、最小仓位实盘模式

MVP 明确不做：

- 多交易所套利
- 高频抢单
- 多币种扩展
- 复杂机器学习信号
- 主观人工择时干预

## 核心原则

项目遵循以下原则：

- 不预测趋势，只响应异常事件
- 不和 HFT 竞争速度，利用东京 VPS 低延迟提升成交率和止损精度
- 不把聊天记录直接当规范，只以模块化文档为准
- 不在样本不足、风控未闭环前接入实盘

## 部署环境

项目默认部署于东京 VPS（距 Binance 匹配引擎最近），端到端延迟目标 < 100ms。

低延迟解锁的关键能力：主动响应式下单、精确止损、多级建仓、多交易对监控、Orderbook 实时深度分析。

## 研发顺序

推荐推进顺序：

0. 东京 VPS 环境搭建与延迟基准测试
1. `Observe Mode`（VPS 上运行）
2. `Shadow Mode`（含多级建仓对比实验）
3. `Testnet Mode`
4. 最小仓位 `Live Mode`

在此之前，不应直接开始写实盘交易逻辑。
