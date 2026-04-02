# 策略修复计划：从第一性原理重新定义

**文档版本**: v1.1
**日期**: 2026-04-02
**状态**: 待执行
**背景**: 本次审查发现当前实现在策略定义、信号逻辑、数据基础三个层面均存在偏移，需系统性修复后才能支撑 Shadow Mode 推进。

**v1.1 变更**：补充 5 处审查修正——Phase 3 基础设施依赖、forceOrder 截断设计决策说明、Open Interest 可观测性注记、两个算法细节定义、Phase 4 时间对齐规则。

---

## 1. 第一性原理：策略的物理基础

### 1.1 永续合约的结构性事实

永续合约不是现货，它是一个**有锚点约束的杠杆工具**。

- 永续合约价格（Perp）通过**资金费率机制**强制锚定现货指数（Index）
- 当 `perp < index`（负基差）→ 资金费率为负 → 空头向多头支付资金费
- 负基差触发三重收敛力：
  1. **套利者**（最强）：买 Perp + 卖现货 = 无风险套利，直接消灭价差
  2. **资金费率压力**（次强）：空头持续支付费用，被动平仓压力推升 Perp
  3. **抄底资金**（辅助）：散户和机构看到低价买入，间接支撑 Perp
- 这个机制意味着：**负基差存在就是套利者的无风险收益**，资本会自动消灭它

### 1.2 强平事件的因果链

```
场景：大量杠杆多头持仓 → 价格触及强平线

步骤：
  1. 交易所执行强制平仓 → 产生「被动市价卖单」（Involuntary Taker Sell）
     → 关键：这笔卖单没有信息含量，不代表任何人对价格的判断

  2. 大量被动卖单冲击买盘 → Perp 价格被砸低
     → 现货指数反应慢（多所均价，流动性深）→ Basis 打开负值

  3. 套利者检测到负基差：买 Perp + 卖现货 = 无风险利润
     → 套利买入推升 Perp → Basis 收敛归零

  4. 同时：抄底资金也入场 → 进一步推升 Perp
```

**我们的 Edge**：在步骤 2 之后、步骤 3 完成之前，做多 Perp，等待 Basis 归零。

这是**物理性的还原力**，不是统计规律：只要负基差存在、套利资金能进入，修复就会发生。

### 1.3 策略的完整定义

> **策略名称**：清算驱动的基差修复（Liquidation-Driven Basis Reversion）
>
> **入场条件**：强制平仓产生的被动卖压使永续合约相对现货指数出现显著负基差
>
> **盈利机制**：套利者买入被错误定价的永续合约，使基差收敛归零，我们随套利者方向获利
>
> **退出条件**：基差修复（归零）或修复失败（止损）

---

## 2. 正确的信号结构

信号的时序至关重要。当前实现混淆了「前置预警」、「入场触发」和「入场确认」。

### 2.1 信号时序图

```
时间轴：

T-∞  ~ T-0:  【前置背景】大量杠杆多头持仓积累
             （参考指标：Open Interest；当前系统未接入，待 Shadow Mode 评估是否引入）
T-3m ~ T-0:  【前置预警】Taker Sell 压力 spike（被动卖单开始涌现）
             （使用 aggTrade.is_buyer_maker 计算，不受 forceOrder 1000ms 截断影响）
T-0:          【触发事件】强平级联发生，Basis 负值打开
T-0 ~ T+2m:  【入场窗口】Basis < -30 bps 且 Taker Buy 开始回升（套利者入场）
T+2m ~ T+15m:【持仓期】等待 Basis 收敛
T+N:          【退出】Basis > -5 bps（修复完成）或 Basis < 入场 -80 bps（止损）
```

### 2.2 四层信号定义（修订版）

| 层级 | 信号 | 作用 | 正确时序 | 当前问题 |
|------|------|------|---------|---------|
| **L1 前置预警** | `taker_sell_peak_in_window(180s) > 65%` | 「有被动卖压正在发生」 | T-3min 内出现过 | ❌ 当前作为同步条件，实际上已过峰值 |
| **L2 核心触发** | `basis_bps < -30`（perp vs index） | 「错误定价已形成」 | T=0，当前值 | ❌ 当前数据库存的是 VWAP 偏离，不是 basis |
| **L3 冲击验证** | `impact_ratio > threshold`（liq/depth） | 「强平量确实击穿了流动性」 | T=0 前 depth，freshness 受基础设施约束 | ❌ 当前无独立 freshness 约束，threshold 待 Shadow Mode 标定 |
| **L4 入场确认** | `taker_buy_ratio_rising()` | 「套利/抄底资金已进场」 | T=0 之后，买方比例上升 | ❌ 未区分"峰值"和"回升方向" |

> **关于 L1 的数据源选择**：L1 使用 `taker_sell_ratio`（来自 aggTrade `is_buyer_maker`），而非 `liq_notional_window`（来自 forceOrder stream）。原因：Binance forceOrder stream 存在 **1000ms 截断**——每个 symbol 每秒最多推送 1 条强平事件，大量并发强平被静默丢弃，导致 `liq_notional_window` 严重低估真实强平量。aggTrade 不受此限制，是更可靠的被动卖压代理指标。
>
> **关于 L3 的局限性**：`impact_ratio` 分子（`liq_notional_5s`）同样来自被截断的 forceOrder 数据，真实冲击量被低估。L3 应理解为**下界估计**而非精确值，其主要作用是过滤掉"强平量极小但 basis 恰好偏负"的噪音样本。

---

## 3. 当前代码的偏移盘点

### 3.1 数据基础层（最严重）

**问题**：`index_price` 没有原始持久化路径

```python
# main.py - on_mark_price 回调
async def on_mark_price(mp: MarkPrice) -> None:
    if _calc is not None:
        _calc.update_index_price(mp.index_price, mp.event_ts)
    # ❌ 缺失：await _writer.write_mark_price(mp)
```

**后果**：
- `basis_bps` 只存在于运行时内存，进程重启即丢失
- 历史 episode 的 `basis_bps` 无法从数据库重建
- 所有基于 `basis` 的回测结论，依赖内存计算，无法事后审计
- `raw_mark_prices` 表不存在于 schema

### 3.2 核心度量层（根本性错误）

**问题**：数据库中 `max_deviation_bps` 使用 VWAP 偏离，不是基差

```python
# episode.py 中的主度量
max_deviation_bps: float | None  # = (mid_price - VWAP) / VWAP × 10000 ← 错误锚点
```

**VWAP 和 Index 的本质区别**：

| 特性 | VWAP_15m | Index Price |
|------|----------|-------------|
| 含义 | 过去 15 分钟的成交均价 | 当前现货市场公允价值 |
| 滞后性 | 严重滞后（平均滞后 7.5 分钟） | 实时（< 1s） |
| 在崩盘时 | 仍反映高价位（慢速均值） | 已随现货同步下跌 |
| 套利锚点 | ❌ 套利者不对标 VWAP | ✅ 套利者对标 Index |

VWAP 偏离衡量的是「相对历史均价跌了多少」，而我们需要的是「相对当前现货公允价值低了多少」。这是完全不同的两件事。

### 3.3 Outcome 度量层（语义错误）

**问题**：`rebound_to_basis_zero_ms` 定义为「价格首次 >= pre_event_index_price」

```python
# outcome.py 中的止盈判断（逻辑上的错误）
# 当前近似：perp_price >= index_price_at_episode_start
# 正确应该：(perp_price - current_index_price) / current_index_price × 10000 >= -5
```

**问题场景**：
- Episode 发生时：perp = $83,000，index = $84,000（basis = -119 bps）
- 15 分钟后：perp = $82,500，index = $82,500（basis = 0 bps）
- **当前系统判断**：价格 $82,500 < $84,000 → ❌ 「未修复」
- **正确判断**：basis 从 -119 bps → 0 bps → ✅ 「已完全修复」

整个市场同步下移时，基差已经修复，但当前系统会把它记录为「修复失败」。

### 3.4 Impact Ratio 时效性（因果污染）

**问题**：当前允许强平前 5 秒的深度数据参与计算

```python
# calculator.py
MAX_STALE_MS: int = 5_000  # ← 应为 100ms
```

强平在 100ms 内就能吃穿一档买盘。使用 5 秒前的深度，等于「用已经不存在的流动性计算冲击率」。`impact_ratio` 因此严重低估真实冲击。

### 3.5 信号时序（逻辑倒置）

**问题**：taker_sell_ratio 作为同步条件，实际应为前置条件

回测数据（2021-05-19）已证明：basis 最深时（-434 bps），taker_sell_ratio 只有 0.587，低于 65% 门限。原因：卖压发生在基差崩溃之前，等基差最深时卖压已经过去。

当前信号设计：`taker_sell_ratio > 65% AND basis < -30` → **永远不会同时成立**

### 3.6 研究准入（VWAP 卡门）

**问题**：`research_report.py` 要求 `pre_event_vwap IS NOT NULL`

即使 basis chain 完整（`pre_event_index_price` + `max_basis_bps` 都有值），一旦 VWAP 字段缺失，样本就被拒绝。相当于用错误的旧指标拦截正确的新指标。

---

## 4. 修复计划

**执行原则**：
1. 先修数据基础，再修上层逻辑（没有正确的 index_price，其他修复都无意义）
2. 每个阶段有明确的验收标准，不达标不进入下一阶段
3. 旧字段（VWAP 系列）保留兼容，但不再控制任何主路径

### Phase 1：数据基础 — index_price 原始持久化

**目标**：让 basis_bps 从「运行时状态」升级为「可审计事实」

**范围**：
- `src/storage/schema.py`：新增 `raw_mark_prices` 表
- `src/storage/writer.py`：新增 `write_mark_price()` 方法
- `src/main.py`：在 `on_mark_price` 中调用 `write_mark_price()`

**表结构**：
```sql
CREATE TABLE IF NOT EXISTS raw_mark_prices (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_ts     INTEGER NOT NULL,   -- 本地收到时间戳 (ms)
    event_ts    INTEGER NOT NULL,   -- 交易所事件时间戳 (ms)
    symbol      TEXT    NOT NULL,
    mark_price  REAL    NOT NULL,   -- 标记价格
    index_price REAL    NOT NULL,   -- 现货指数价格（公允价值锚点）
    basis_bps   REAL    NOT NULL    -- (mark_price - index_price) / index_price × 10000
);
CREATE INDEX IF NOT EXISTS idx_mark_price_symbol_ts ON raw_mark_prices(symbol, event_ts);
```

**验收标准**：
- [ ] `raw_mark_prices` 表存在，每秒至少写入 1 条 BTCUSDT 记录
- [ ] 任意一个 signal 记录，可通过 JOIN `raw_mark_prices` 重建当时的 basis_bps
- [ ] 进程重启后，历史 basis 数据不依赖内存状态即可查询

---

### Phase 2：Episode 核心字段修复

**目标**：让 episode 的特征字段对齐第一性原理

**范围**：
- `src/features/episode.py`：正确填充 `pre_event_index_price`、`max_basis_bps`、`max_impact_ratio`
- `src/storage/writer.py`：确认 `write_episode()` 写入新字段

**字段语义确认**：

| 字段 | 正确定义 | 当前状态 |
|------|---------|---------|
| `pre_event_index_price` | 首笔强平时的现货指数价格 | 代码已有但未验证写入 |
| `max_basis_bps` | episode 期间最深的负基差（最小值） | 代码已有但依赖内存 index |
| `max_impact_ratio` | episode 期间 impact_ratio 峰值（需满足 100ms freshness） | 待收紧 freshness 约束 |
| `pre_event_vwap` | [DEPRECATED] 保留兼容，不参与主逻辑 | 保留 |
| `max_deviation_bps` | [DEPRECATED] 保留兼容，不参与主逻辑 | 保留 |

**验收标准**：
- [ ] 新产生的 episode 记录中，`pre_event_index_price` 和 `max_basis_bps` 均非 NULL
- [ ] `max_basis_bps` 的值与 `raw_mark_prices` 中对应时间段的 basis 一致

---

### Phase 3：Impact Ratio Freshness 收紧

**目标**：让 `impact_ratio` 真正表示「强平瞬间击穿了多少事前流动性」，消除因果方向污染

**范围**：
- `src/features/calculator.py`：将 depth freshness 独立拆分，设置合理上界

**⚠️ 基础设施前提（重要约束）**：

当前 `raw_depth_snapshots` 采用 **1 秒降采样**（每秒写入 1 条）。这意味着：
- 将 `MAX_DEPTH_STALE_MS` 设为 100ms → `impact_ratio` 将**几乎永远为 None**（深度数据本身就是 1s 级的）
- 100ms freshness 目标，必须配套 100ms 级别的深度采集频率

**两阶段实施方案**：

| 阶段 | 条件 | `MAX_DEPTH_STALE_MS` | 说明 |
|------|------|---------------------|------|
| **过渡方案（当前）** | depth stream 仍为 1s 采样 | `1500ms` | 1s 采样间隔 + 500ms 处理裕量，保证 `impact_ratio` 有实际可用值 |
| **目标方案（长期）** | depth stream 升级至 ≤100ms | `100ms` | 完全符合因果语义，强平前 100ms 内的深度 |

**修改逻辑（过渡方案）**：
```python
# 现有（错误）：所有特征共用同一个 stale 阈值
MAX_STALE_MS: int = 5_000

# 修复后：各特征独立 freshness 约束，语义明确
MAX_INDEX_STALE_MS: int = 2_000   # index price：2s（markPrice@1s 流最大间隔）
MAX_DEPTH_STALE_MS: int = 1_500   # bid depth：1.5s（过渡方案，待 depth 升频后收紧至 100ms）
MAX_MID_STALE_MS:   int = 500     # mid price：500ms
```

**不变的因果约束**（与采样频率无关，必须严格执行）：
- `depth.event_ts <= liq.event_ts`（**绝对禁止**使用强平之后才到达的深度数据）

**验收标准**：
- [ ] 单元测试：depth 在强平前 1000ms → impact_ratio 有效（过渡方案下）
- [ ] 单元测试：depth 在强平前 2000ms → impact_ratio = None
- [ ] 单元测试：depth 在强平后任意时刻 → impact_ratio = None（因果约束）
- [ ] depth stream 升频后，`MAX_DEPTH_STALE_MS` 可独立收紧至 100ms，无需改动其他代码

---

### Phase 4：Outcome 语义修复 — 基差相对度量

**目标**：让「是否修复」基于相对基差收敛，而非绝对价格阈值

**范围**：
- `src/features/outcome.py`：重写 `rebound_to_basis_zero_ms` 计算逻辑
- `src/storage/writer.py`：`get_episodes_pending_outcome()` 同步返回 index_price 时间序列

**正确的修复判断逻辑**：

```
输入：
  - episode 结束后 15 分钟内的 raw_trades（perp 价格时间序列，毫秒级）
  - 同时间段的 raw_mark_prices（index 价格时间序列，1 秒级）

时间对齐规则（重要）：
  raw_trades 是 tick 级（毫秒），raw_mark_prices 是 1 秒级。
  对每笔成交 trade(T)，向前追溯最近一条 index_price：
    - 若 T - index_price.event_ts <= 2000ms → 可用，计算 basis_bps
    - 若 T - index_price.event_ts  > 2000ms → index 数据过旧，basis_bps = None，跳过此点
  （2000ms 对应 markPrice@1s 流的最大正常间隔 1s + 1s 网络/处理裕量）

计算：
  对每个时间点 T（basis_bps 非 None）：
    basis_bps(T) = (trade.price - index_price) / index_price × 10000

判断修复：
  rebound_to_basis_zero_ms = 首个 basis_bps(T) >= -5 的时间点 T - episode.end_event_ts
  如果 15 分钟内无此时间点 → None（修复失败）
  如果 15 分钟内 basis_bps 始终为 None → None（数据不足，不计入统计）
```

**验收标准**：
- [ ] 测试场景 A：perp 和 index 同步下跌，价差收窄 → 判断「修复」（当前系统会错判为「未修复」）
- [ ] 测试场景 B：perp 上涨但 index 上涨更多，价差扩大 → 判断「未修复」（当前系统可能错判为「修复」）
- [ ] 测试场景 C：basis 从 -100 bps 收敛到 -3 bps → 在正确时间点记录 `rebound_to_basis_zero_ms`

---

### Phase 5：信号时序修复

**目标**：让四层信号在正确的时间窗口内协作触发

**范围**：
- `src/features/calculator.py`：增加「前置卖压」滑动窗口（T-3min 内的 taker_sell 峰值）
- `src/main.py`：更新信号触发逻辑

**正确的信号触发条件**：

```python
# 前置预警（前3分钟内是否出现过卖压 spike）
precondition_met = calc.taker_sell_peak_in_window(lookback_sec=180) > 0.65

# 核心触发（当前时刻）
basis_trigger = snap.basis_bps is not None and snap.basis_bps < -30

# 冲击验证（当前时刻，需满足 freshness 约束）
# IMPACT_THRESHOLD 待 Shadow Mode 数据标定，当前阶段仅记录，不作硬门槛
impact_trigger = snap.impact_ratio is not None and snap.impact_ratio > IMPACT_THRESHOLD

# 入场确认（taker_buy_ratio 正在回升）
buy_recovery = calc.taker_buy_ratio_rising()  # 见下方算法定义

# 最终信号
signal_fired = precondition_met AND basis_trigger AND buy_recovery
# impact_trigger 作为质量过滤，决定仓位大小而非是否入场
```

**算法定义**：

`taker_sell_peak_in_window(lookback_sec=180)` — 前置卖压峰值
```
在过去 lookback_sec 秒内，以 30 秒为滑动窗口，计算每个窗口的 taker_sell_ratio。
返回所有窗口中的最大值（峰值）。
若窗口内成交量不足（< 最小阈值），该窗口跳过。
```
- 设计意图：捕捉"过去 3 分钟内曾经出现过大卖压"，而不要求卖压此刻仍然持续
- 参数 `lookback_sec=180` 可配置，Shadow Mode 标定后冻结

`taker_buy_ratio_rising()` — 买方力量回升确认
```
计算两个窗口的 taker_buy_ratio 均值：
  recent_avg  = 过去 0~10 秒的 taker_buy_ratio 均值
  prior_avg   = 过去 10~30 秒的 taker_buy_ratio 均值
返回：recent_avg > prior_avg（正斜率）
若任一窗口成交量不足，返回 False（不确认入场）
```
- 设计意图：确认买方力量**正在增强**（套利者/抄底者已开始入场），而非仅仅"当前买方占多数"
- 区分「卖压高峰已过、买方正在接管」（True）与「市场仍在单边下砸中」（False）

**验收标准**：
- [ ] 当 basis < -30 但前 3 分钟无卖压 spike → 不触发信号
- [ ] 当前 3 分钟有卖压 spike，basis < -30，buy_ratio 回升 → 触发信号
- [ ] 日志中明确区分「前置预警」和「信号触发」两个状态

---

### Phase 6：研究准入去 VWAP 依赖

**目标**：sample 的准入标准与策略的物理因果链对齐

**范围**：
- `src/analysis/research_report.py`：修改 `qualify_episode()` 函数

**新的准入标准**：
```python
def qualify_episode(ep) -> bool:
    """
    Episode 进入研究样本的准入条件（对齐第一性原理）。

    必填（反映因果链完整性）：
      - pre_event_index_price IS NOT NULL  ← 基差基准
      - max_basis_bps IS NOT NULL          ← 错误定价存在
      - max_basis_bps < -15               ← 有意义的负基差（非噪音）

    可选（提高样本质量）：
      - max_impact_ratio IS NOT NULL       ← 流动性真空确认
      - liq_notional_total > 50000        ← 非微小事件

    弃用（不再控制准入）：
      - pre_event_vwap IS NOT NULL        ← [DEPRECATED] 不影响准入
    """
    return (
        ep.get("pre_event_index_price") is not None
        and ep.get("max_basis_bps") is not None
        and ep.get("max_basis_bps", 0) < -15
    )
```

**验收标准**：
- [ ] `pre_event_vwap` 为 NULL 但 `pre_event_index_price` 和 `max_basis_bps` 完整时，样本通过准入
- [ ] 日报和研究报告的「信号摘要」使用 basis_bps 而非 deviation_bps

---

## 5. 执行顺序与依赖关系

```
Phase 1（index_price 持久化）
    └─→ Phase 2（episode 字段修复）← 依赖 raw_mark_prices 表
    └─→ Phase 4（outcome 语义修复）← 依赖 raw_mark_prices 时间序列
    └─→ Phase 3（impact ratio）← 独立，可并行

Phase 2 + Phase 3 完成
    └─→ Phase 5（信号时序）← 依赖正确的 basis 和 impact_ratio

Phase 2 + Phase 4 完成
    └─→ Phase 6（研究准入）← 依赖字段可信
```

**推荐执行顺序**：Phase 1 → Phase 3 → Phase 2 → Phase 4 → Phase 5 → Phase 6

Phase 1 是唯一硬前置，其他可部分并行。

---

## 6. 未修复前的运行约束

在 Phase 1-4 完成之前：

- ❌ 不允许将当前 `max_deviation_bps` 数据用于策略参数冻结
- ❌ 不允许将当前 `rebound_to_vwap_ms` 用于「修复率」统计
- ❌ 不允许推进 Shadow Mode（信号逻辑未完成修复）
- ✅ 允许继续 Observe Mode 数据收集（数据在入库，修复后可重算）
- ✅ 允许用 Binance 历史数据做离线回测验证（独立路径，不受上述约束）

---

## 7. 验证里程碑

| 里程碑 | 条件 | 意义 |
|-------|------|------|
| **M1: 数据可信** | Phase 1+2 完成，raw_mark_prices 稳定写入 7 天 | basis 计算可审计 |
| **M2: 信号可信** | Phase 3+5 完成，信号触发逻辑通过测试 | 信号不再有因果污染 |
| **M3: 研究可信** | Phase 4+6 完成，outcome 基于相对基差 | 修复率统计真实反映策略 |
| **M4: Shadow 就绪** | M1+M2+M3 全部达成，收集 ≥ 30 个有效样本 | 可推进 Shadow Mode |

---

## 8. 附录：与已有文档的关系

本文档是 [`first-principles-regression-plan.md`](./first-principles-regression-plan.md) 的**上层定义文档**。

- `first-principles-regression-plan.md`：记录 4 类技术债的修复方案（Phase A/B/C/D）
- 本文档（`strategy-repair-plan.md`）：从策略逻辑出发，解释「为什么要修」以及「修完之后策略是什么样的」

两个文档的 Phase 对应关系：

| 本文档 | first-principles-regression-plan.md |
|--------|--------------------------------------|
| Phase 1 | 阶段 A（核心锚点持久化） |
| Phase 2 | 阶段 A 的延伸 |
| Phase 3 | 阶段 C（Impact Ratio freshness） |
| Phase 4 | 阶段 B（Outcome 语义） |
| Phase 5 | 新增（信号时序修复，原文档未覆盖） |
| Phase 6 | 阶段 D（研究准入去 VWAP 依赖） |
