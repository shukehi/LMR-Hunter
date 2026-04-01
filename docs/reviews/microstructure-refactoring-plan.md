# LMR-Hunter 微观结构重构计划 (Microstructure Refactoring Plan)

根据最新的 `strategy.md` (基于市场微观结构与第一性原理)，系统需要从“绝对偏离（VWAP）”转向“相对基差（Index Price）”和“真实流动性破坏（Orderbook Depth）”。

本次重构旨在将底层逻辑与物理世界的实际驱动力对齐，为接下来的 Shadow Mode 建立正确的数据基础。

## 阶段 1：网关层升级 (Data Gateway Upgrade - The Senses)
**目标**：感知真实的公允价值和真实的盘口厚度。
**文件**：`src/gateway/binance_ws.py`, `src/gateway/models.py`

1. **移除对 K 线的依赖**：
   - 取消订阅 `btcusdt@kline_1m` 流（VWAP 不再需要）。
2. **新增现货指数价格流**：
   - 订阅币安的现货指数流。对于 `BTCUSDT` 永续，对应的底层指数流通常是 `<symbol>@markPrice` 或专门的指数流。我们需要获取实时的 `Index Price` 作为新的公允价值锚点。
3. **升级深度数据处理 (Depth)**：
   - 目前已经订阅了 `depth20@100ms`。我们需要在 `Depth` 模型中不仅提取 `mid_price`，还要聚合计算买一到买 N（例如前 0.2% 价格范围内）的**总挂单名义金额（Limit Buy Depth in USDT）**。
4. **新增主动成交标识 (Taker Buy/Sell)**：
   - 确保 `Trade` 消息（`aggTrade` 流）被正确解析出 `is_buyer_maker` 字段，以判断这笔成就是主动买（Taker Buy）还是主动卖（Taker Sell）。

## 阶段 2：特征引擎重构 (Feature Calculator Overhaul - The Brain)
**目标**：计算真实的冲击比例和基差偏离，取代天真的均线计算。
**文件**：`src/features/calculator.py`

1. **废除旧特征**：
   - 彻底删除 `_vwap` 计算逻辑。
   - 删除 `kline` 的接收接口（`on_kline`）。
2. **新增基差偏离率 (Basis_Bps)**：
   - `Basis_Bps = (Perp_Mid_Price - Index_Price) / Index_Price * 10000`
   - 这取代了原来的 `deviation_bps`。
3. **新增冲击比例 (Impact Ratio)**：
   - `Impact Ratio = liq_notional_window / current_bid_depth`
   - 要求在计算快照时，必须结合最新鲜的盘口厚度（`current_bid_depth`）进行相除。
4. **重构衰减层 (Order Flow Imbalance)**：
   - 在滑动窗口内，不仅记录强平额，还要统计主动买单金额（Taker Buy Volume）。
   - 定义新的信号：必须出现一定量的 Taker Buy 介入，才将 `is_accelerating` 标记为 `False`（即允许入场）。

## 阶段 3：事件与结果模型同步 (Episode & Outcome Adjustment - The Memory)
**目标**：使离线分析库能正确反映微观结构的特征。
**文件**：`src/features/episode.py`, `src/features/outcome.py`, `src/storage/schema.py`

1. **Episode 记录升级**：
   - 移除 `pre_event_vwap` 字段。
   - 新增 `pre_event_index_price`（事发前的现货指数价格）。
   - 新增 `max_impact_ratio`（记录本次事件中，抛压击穿盘口的最严重比例）。
   - 记录 `max_basis_bps`（最深负基差）。
2. **Outcome 评估升级**：
   - 止盈不再是向 VWAP 回归，而是计算向 `pre_event_index_price` 或 0 基差的回归延迟（`rebound_to_basis_ms`）。
   - 修改 `rebound_depth_bps` 为 `basis_rebound_depth`。
3. **数据库 Schema 迁移**：
   - 编写一次性的 SQLite migration 脚本或直接重建开发数据库，以匹配上述新字段。

## 阶段 4：主流程和监控更新 (Main Loop Integration)
**目标**：连接新的组件并调整告警/记录。
**文件**：`src/main.py`

1. **连接数据流**：将新加入的 Index Price 流注入到 `FeatureCalculator`。
2. **精简引导流程**：不再需要启动时通过 REST 拉取 15 根 K 线进行“预热”。启动即可直接工作。
3. **更新信号写入**：更新向 `signals` 表写入数据的逻辑，包含新的 `impact_ratio` 和 `basis_bps`。
4. **日志调整**：重写终端打印的日志摘要，高亮显示真实的“基差”和“冲击率”，而不是原来的“偏离率”。

## 执行顺序建议

1. **Step 1: 模型与网关**（修改 `models.py` 和 `binance_ws.py`，接通 Index Price）。
2. **Step 2: 大脑重构**（重写 `calculator.py`）。
3. **Step 3: 存储与事件**（修改 Schema, `episode.py`, `outcome.py`）。
4. **Step 4: 组装与测试**（修改 `main.py` 并运行 `pytest` 修复中断的测试用例）。
