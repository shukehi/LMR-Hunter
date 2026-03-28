# 运行态优化计划（2026-03-27 审查后）

## 1. 文档目的

本文档定义本次“整仓库代码 + VPS 运行态”审查之后的优化顺序、默认决策、交付物与验收标准。

本文档回答的是：

- 当前 Observe Mode 为什么还不能作为可信研究基线
- 哪些问题会直接污染 episode/outcome 结论
- 应该先修什么、冻结什么、如何验证修复已经生效

本文件是 2026-03-27 的运行态审查记录，不替代 [策略文档](../strategy.md)、[术语表](../glossary.md)、[架构文档](../architecture.md)、[风控文档](../risk-controls.md)、[开发计划](../development-plan.md) 和 [项目状态](../status.md) 这些长期规范文档。

本文件与 [`reviews/optimization-plan.md`](./optimization-plan.md) 的关系如下：

- `optimization-plan.md` 记录的是第一轮结构性优化路线
- 本文档记录的是 2026-03-27 运行态复盘后必须执行的第二轮修复计划

## 2. 审查基线

本次审查确认的事实如下：

- `quant-vps` 上 `lmr-hunter.service` 正在运行
- 实际运行命令为 `/opt/lmr-hunter/.venv/bin/python -m src.main`
- VPS 运行环境为 `Python 3.12.3`
- VPS 虚拟环境内测试通过：`37 passed`
- 本机默认 `Python 3.9`，与项目正式运行基线不一致

运行证据显示，当前系统已经不是“未实现状态”，而是“已上线 Observe Mode，但研究数据不可信”状态。

## 3. 当前关键问题

### 3.1 最高优先级问题：trade 样本持续丢失

2026-03-27 10:33:54 UTC 到 11:08:55 UTC 的运行日志显示：

- `入队=193295`
- `写库=81404`
- `丢弃=103514`

这不是偶发抖动，而是吞吐设计与真实流量长期失配：

- 当前默认批量大小为 `200`
- 当前 flush 周期为 `5s`
- 理论写入能力约为 `40 trades/s`
- 实际观测入队速率约为 `92 trades/s`

结论冻结如下：

- 只要维持当前参数，`raw_trades` 就会持续丢样本
- 基于这些 `raw_trades` 计算出的 `episode_outcomes` 不能作为研究结论依据
- 在问题修复前，不允许继续做参数冻结，不允许推进 Shadow Mode

### 3.2 状态语义错误：系统在丢数时仍报告 `OK`

当前实现里：

- 队列满导致的样本丢弃只会增加 `discarded`
- `is_degraded` 只由连续 flush 失败触发
- 心跳和主日志的 `状态=OK/DEGRADED` 仅依赖 `is_degraded`

因此出现了以下错误状态：

- 已经确认发生大规模样本丢弃
- 主日志仍输出 `状态=OK`
- 运维层无法从心跳状态上判断当前 run 是否还可用于研究

这会导致错误的研究信心，比单纯的写库失败更危险。

### 3.3 停机流程不完整

当前停机流程虽然会：

- 记录收到信号
- flush active episode
- flush 一部分 trade queue
- 关闭数据库连接

但仍存在两个问题：

- 后台周期任务没有统一取消与收口
- `BinanceGateway.stop()` 只修改 `_running` 标志，没有主动关闭当前 websocket

结论冻结如下：

- 现有“优雅退出”不是完整的生命周期收口
- 需要显式管理 websocket、后台任务、writer drain 和 DB close 顺序

### 3.4 环境契约没有落盘

当前项目实际上依赖以下事实才能跑通：

- Python `3.12.3`
- 虚拟环境内已安装运行依赖和测试依赖
- 入口路径是 `/opt/lmr-hunter`
- 测试需要从仓库根目录解析 `src` 包

但仓库当前没有完整定义这些契约：

- `requirements.txt` 只有运行依赖，没有测试依赖
- 没有 `pytest.ini`、`pyproject.toml` 或等效配置来声明 Python 版本与测试入口
- `.env.example` 没有暴露 Observe Mode 实际使用的关键参数
- README 仍写“尚未开始代码实现”

结论冻结如下：

- VPS 成功运行依赖的是“机器状态”，不是“仓库自描述能力”
- 在契约补齐前，项目不可视为可复现部署

### 3.5 样本边界语义仍有一处歧义

`EpisodeBuilder` 的文档写的是：

- gap 超过阈值时关闭
- duration 超过阈值时关闭

而实现使用的是 `>=`。

当前测试只锁住了 `duration == max_duration` 这条边界，没有锁住 `gap == gap_ms` 的边界。因此：

- 当前行为虽然稳定
- 但文档、实现、测试还没有三方完全闭合

## 4. 冻结规则

在以下条件全部满足前，系统只能停留在 Observe Mode：

- `raw_trades` 不再因为队列满而持续丢样本
- 心跳状态能够真实反映研究数据是否可用
- 停机流程可以证明不会留下不一致状态
- 环境契约可以在新机器上直接复现

在以下条件满足前，不允许：

- 冻结任何策略阈值
- 使用 `episode_outcomes` 作为策略有效性证据
- 推进 Shadow Mode 参数扫描
- 以“当前 VPS 已在线运行”为理由跳过工程治理补齐

## 5. 优化总目标

本轮优化后的系统必须满足以下目标：

- trade 路径的采集吞吐有足够余量，不再稳定丢样本
- 一旦发生样本丢弃，系统状态必须立刻转为 `DEGRADED`
- 运行中断时，websocket、后台任务、writer、DB 都能按顺序退出
- Python 版本、依赖、测试入口、配置模板都由仓库明确定义
- 研究报告能够区分“可信 run”与“污染 run”

## 6. 分阶段优化方案

> **完成状态（2026-03-28）**
>
> | 阶段 | 状态 | 提交 |
> |---|---|---|
> | 阶段 A trade 吞吐与完整性语义 | ✅ 完成 | `6d658ec` + `30d4574` |
> | 阶段 B 停机与生命周期管理 | ✅ 完成 | `6d658ec` |
> | 阶段 C 环境契约与仓库自描述 | ✅ 完成 | `6d658ec` |
> | 阶段 D 受污染数据可见性 | ✅ 完成 | `7302ab3` |
> | 阶段 E 剩余语义歧义收口 | ⬜ 待做 | — |

### 阶段 A：修复 trade 吞吐与完整性语义

目标：

- 消除当前稳定态下的持续丢数
- 将“队列满”从普通 warning 升级为研究数据失效事件

范围：

- `src/storage/writer.py`
- `src/main.py`
- `config/.env.example`
- 对应测试

冻结决策：

- 新增显式参数：
  - `TRADE_BATCH_SIZE`
  - `TRADE_FLUSH_INTERVAL_SEC`
  - `TRADE_QUEUE_MAXLEN`
  - `TRADE_FLUSH_MAX_BATCHES_PER_TICK`
- 默认值改为：
  - `TRADE_BATCH_SIZE=2000`
  - `TRADE_FLUSH_INTERVAL_SEC=1`
  - `TRADE_QUEUE_MAXLEN=50000`
  - `TRADE_FLUSH_MAX_BATCHES_PER_TICK=20`
- `trade_flusher` 每轮执行时，最多执行 `TRADE_FLUSH_MAX_BATCHES_PER_TICK` 次 flush
- 单轮最大排空能力固定为：
  - `TRADE_BATCH_SIZE * TRADE_FLUSH_MAX_BATCHES_PER_TICK`
  - 按默认值即 `2000 * 20 = 40000 trades/tick`
- 若达到单轮上限后队列仍非空：
  - 本轮立即停止继续 flush，主动让出事件循环
  - 记录一次 `QUEUE_BACKLOG` 风险事件，级别为 `WARN`
  - 输出 backlog warning，带上剩余 `trade_queue_size`
  - 不因 backlog 本身标记 `DEGRADED`
- 只有发生真实样本损坏时，才允许将状态升级为 `DEGRADED`
- writer 完整性计数器拆分为：
  - `written`
  - `retried`
  - `overflow_dropped`
  - `isolated`
  - `lost`
- 现有 `discarded` 字段不再混合表达两种含义；若保留兼容字段，则定义为：
  - `discarded = overflow_dropped + isolated + lost`
- 一旦发生以下任一事件，当前 run 立即标记为 `DEGRADED`，且进程生命周期内不自动恢复为 `OK`：
  - `overflow_dropped > 0`
  - `isolated > 0`
  - `lost > 0`
  - 连续 flush 失败达到阈值

要做的事：

- 将 writer 的完整性状态从“仅看 flush 失败”改为“任何真实样本损坏都降级”
- heartbeat 中新增以下字段或等效 notes：
  - `trade_queue_size`
  - `overflow_dropped`
  - `isolated`
  - `lost`
- 在主日志和告警中明确区分：
  - 队列回压丢弃
  - 写库失败隔离
  - 永久丢失
- 在 `stats_reporter` 中输出“研究可用性状态”，不能再只输出系统运行状态

交付物：

- 可配置的 trade drain 参数
- 新的 integrity 状态机
- 对 overflow 的风险事件和告警
- 覆盖 steady-state backlog 的测试

验收标准：

- 在与 VPS 接近的流量回放下，队列不再持续增长到满
- `overflow_dropped == 0`
- `isolated == 0`
- `lost == 0`
- 如果人为制造 overflow，主日志、心跳、风险事件、告警都必须转为 `DEGRADED`
- 如果人为制造 backlog 但未发生样本损坏，系统必须输出 `QUEUE_BACKLOG` 风险事件，且状态保持非 `DEGRADED`

### 阶段 B：修复停机与生命周期管理

目标：

- 保证服务停止时不会依赖 systemd 超时强杀来收尾
- 保证最后一批 trade、episode 和 outcome 不处于半关闭状态

范围：

- `src/main.py`
- `src/gateway/binance_ws.py`
- 必要测试

冻结决策：

- `BinanceGateway` 必须持有当前 websocket 连接引用
- `stop()` 必须主动关闭 websocket，而不是只改 `_running`
- `main()` 不再直接 `asyncio.gather(...)` 永久阻塞；改为显式创建任务并统一协调关闭
- 新的停机顺序固定为：
  1. 设置 shutdown 标志
  2. 停止接收新 websocket 数据
  3. 取消周期性后台任务
  4. flush active episode
  5. drain trade queue
  6. 关闭数据库连接
  7. 等待任务结束并记录最终退出日志

要做的事：

- 为 `trade_flusher`、`stats_reporter`、`outcome_processor` 增加取消处理
- 保证 signal handler 不会重复并发触发多次 shutdown
- 明确 shutdown 超时策略，并在超时时输出结构化错误日志

交付物：

- 可证明可收口的 shutdown 流程
- task cancellation 测试
- websocket stop/close 测试或可复现验证脚本

验收标准：

- 通过 `systemctl stop lmr-hunter.service` 触发停机后，日志必须出现完整退出链路
- 退出日志中必须出现：
  - 收到信号
  - 停止网关
  - flush 结果
  - 数据库关闭
  - 主进程正常退出
- 关闭过程中不得再出现关闭后的写库异常
- `systemctl status lmr-hunter.service` 必须显示正常停止，不得出现 failed 状态
- `journalctl -u lmr-hunter.service` 不得出现以下任一特征：
  - stop timeout
  - forced kill
  - main process exited with signal
  - systemd 在 `TimeoutStopSec` 后接管清理

### 阶段 C：补齐环境契约与仓库自描述能力

目标：

- 让“VPS 能跑”变成“仓库本身定义了怎么跑”
- 消除本机与 VPS 的解释器/依赖歧义

范围：

- `requirements.txt`
- 新增 `requirements-dev.txt`
- 新增 `pyproject.toml`
- `config/.env.example`
- `README.md`
- `docs/architecture.md`
- `docs/development-plan.md`

冻结决策：

- 正式运行基线固定为 VPS 当前版本 `Python 3.12.3`
- 测试基线固定为 VPS 当前版本 `Python 3.12.3`
- 仓库必须提供一个直接可执行的测试入口，无需手工设置 `PYTHONPATH`
- `pyproject.toml` 作为单一真相来源，负责定义：
  - Python 版本要求
  - 包元数据
  - pytest 配置
- 采用 `setuptools` + editable install 方案解决 `src` 包导入
- 标准测试安装流程固定为：
  - `python -m pip install -r requirements-dev.txt`
  - 其中 `requirements-dev.txt` 必须包含：
    - `-r requirements.txt`
    - `pytest`
    - `pytest-asyncio`
    - `-e .`
- 标准测试命令固定为：
  - `python -m pytest -q`
- `.env.example` 必须覆盖 Observe Mode 当前真实使用的关键变量

要做的事：

- 在 `pyproject.toml` 中声明 Python 版本要求，并与 VPS `3.12.3` 对齐
- 在 `pyproject.toml` 中加入 pytest 配置：
  - `asyncio_default_fixture_loop_scope = "function"`
- 在 `pyproject.toml` 中定义可 editable install 的当前项目包
- 将以下实际配置项加入 `.env.example`：
  - `DB_PATH`
  - `LIQ_WINDOW_SEC`
  - `EPISODE_GAP_SEC`
  - `EPISODE_NOISE_THRESHOLD_USDT`
  - `EPISODE_MAX_DURATION_SEC`
  - `OUTCOME_CHECK_SEC`
  - `LIQ_SIGNAL_SYMBOL`
  - `LIQ_SIGNAL_SIDE`
  - `LATENCY_P95_ALERT_MS`
  - `TRADE_BATCH_SIZE`
  - `TRADE_FLUSH_INTERVAL_SEC`
  - `TRADE_QUEUE_MAXLEN`
- README 文案改为反映当前真实状态：
  - 已实现并部署 Observe Mode
  - 当前主要问题是运行态数据完整性与工程契约

交付物：

- 明确的 Python/pytest 契约
- 可复现的本地开发与测试说明
- 更新后的 `.env.example`
- 更新后的 README
- 更新后的 `docs/architecture.md`
- 更新后的 `docs/development-plan.md`

验收标准：

- 在一台新机器上，仅按仓库说明即可跑通测试
- 无需手工设置 `PYTHONPATH`
- 本地与 VPS 的运行方式不再出现隐含差异
- 不允许依赖未在仓库中声明的 pytest 插件或 shell 环境变量

### 阶段 D：修复受污染数据的可见性

目标：

- 防止历史坏数据继续进入研究结论
- 给后续日报、回放、参数研究一个明确的可信边界

范围：

- `daily_report.py` 或后续研究脚本
- schema 或 metadata 记录
- 运维文档

冻结决策：

- 任何 run 只要出现：
  - `overflow_dropped > 0`
  - `isolated > 0`
  - `lost > 0`
  - `DEGRADED`
  就不得作为策略参数依据
- 2026-03-27 当前这份 VPS 数据默认视为“部分污染样本”，只能用于链路观察，不能用于净期望判断

要做的事：

- 为 report 增加 run validity 检查
- 明确标注“可信 run / 污染 run”
- 对已有数据库给出一次性人工判定说明

交付物：

- 数据可信性判定规则
- 报告层的 validity 标记
- 运维操作说明

验收标准：

- 报表不会再在坏数据上输出“可用于研究”的结论
- 运维人员能明确知道哪些时间段必须剔除

### 阶段 E：收口剩余语义歧义

目标：

- 让文档、实现、测试在 episode 边界上完全一致

范围：

- `src/features/episode.py`
- `tests/test_episode_model.py`
- `docs/architecture.md` 或相关说明文档

冻结决策：

- `gap` 与 `duration` 的边界语义必须统一写死
- 若保留当前 `>=` 行为，则文档必须同步改为“达到或超过阈值即关闭”
- 若改回严格 `>`，则测试必须跟着更新

要做的事：

- 补一条 `gap == gap_ms` 的精确边界测试
- 统一文档表述

交付物：

- 边界语义回归测试
- 文档/实现一致性修复

验收标准：

- `gap == gap_ms`
- `gap > gap_ms`
- `duration == max_duration_ms`
- `duration > max_duration_ms`

以上 4 个边界全部有明确、单一、可回归的定义。

## 7. 推荐执行顺序

推荐按以下顺序推进：

1. 阶段 A：trade 吞吐与完整性语义
2. 阶段 B：停机与生命周期管理
3. 阶段 C：环境契约与仓库自描述
4. 阶段 D：坏数据可见性与报告 gating
5. 阶段 E：剩余语义歧义收口

原因：

- 阶段 A 决定当前数据是否还值得继续采
- 阶段 B 决定运维动作是否可靠
- 阶段 C 决定后续修复是否可复现
- 阶段 D 决定历史样本是否会继续误导研究
- 阶段 E 是必要收口，但不应抢在完整性修复之前

## 8. Shadow Mode 准入门槛

只有以下条件全部满足，才允许继续推进 Shadow Mode：

- 最近连续 `72h` Observe Mode 运行中 `overflow_dropped == 0`
- 最近连续 `72h` Observe Mode 运行中 `isolated == 0`
- 最近连续 `72h` Observe Mode 运行中 `lost == 0`
- 心跳状态与真实完整性状态完全一致
- 手动 `SIGTERM` 演练能稳定完整退出
- 新机器可按仓库文档完成本地测试与启动

若任一条件不满足：

- 只能继续修运行态
- 不允许进入“调参数”阶段

## 9. 最终交付判断

本轮优化完成的定义不是“代码更漂亮”，而是以下事实成立：

- Observe Mode 产出的 `raw_trades` 与 `episode_outcomes` 可用于研究
- 任何样本损坏都会立刻被状态、日志、告警和报表看见
- 服务停止与重启都不会留下不确定状态
- 环境契约已由仓库显式定义，而不是靠 VPS 上的手工状态

在达到这个定义之前，当前系统应被视为：

- 已具备 Observe Mode 原型
- 但尚未形成可信研究基线
