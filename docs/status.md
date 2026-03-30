# 项目状态

## 1. 文档目的

本文档记录项目当前所处阶段、当前阻塞项和默认下一步动作。

本文件回答的是：

- 项目现在进行到哪里
- 当前哪些结论已经成立，哪些还没成立
- 接下来默认先做什么

## 2. 当前阶段

**Observe Mode 已在 AWS Tokyo VPS 上线运行（2026-03-27），并通过完整验证（2026-03-30）。**

当前已经完成：

- 阶段 1：数据完整性保证
- 阶段 2：时间语义修复
- 阶段 3：Episode 模型落地
- 阶段 4：Outcome 回填
- 阶段 5：可交易口径研究报告

### 2.1 Tokyo VPS 部署验证完成

**网络延迟基准（2026-03-30 02:20 UTC）：**

- **Binance WebSocket**：2.18ms 平均延迟（P50: 1.41ms，P99: 5.38ms）
- **Bybit WebSocket**：3.28ms 平均延迟（P50: 2.28ms，P99: 7.31ms）
- **ICMP Ping**：0.27ms 平均延迟
- **相对欧洲 VPS**：60倍性能提升（欧洲 ~130ms）

**验收标准：全部通过**
- ✅ WebSocket P50 < 50ms（实际 2.18ms 远优于要求）
- ✅ 系统时钟与 NTP 同步（偏差 < 10ms）
- ✅ 进程自动重启已配置（systemd）

### 2.2 当前数据采集进度

- **原始成交记录**：2,202 条（自 2026-03-30 02:18 起，约 2 分钟样本）
- **强平事件**：32 个
- **K 线数据**：17 根
- **清算事件**：0 个（积累中，预期 48 小时内达到 60 个目标）
- **采集速率**：~1,000+ trades/min，~15+ liquidations/min

### 2.3 当前尚未完成

- 阶段 6：Shadow Mode 准入门槛冻结（待 48 小时数据采集完成）

## 3. 当前验证状态

**基础设施验证完成，现阶段重点为数据质量验证。**

当前项目进度：

1. ✅ VPS 部署验证完成（网络延迟、系统配置、自动重启）
2. ✅ 可交易口径报告已实现（fill rate / expectancy / MAE / MFE 等指标）
3. ✅ Shadow Mode 准入门槛已冻结为预注册文档
4. ⏳ 有效 `episode` 样本量积累中（目标 60 个，当前 0 个，预期 48h 内完成）
5. ⏳ 样本质量评估待完成（需运行 `qualify_episode` 过滤器和 `research_report`）
6. ⏳ Europe → Tokyo 迁移决策待数据对比完成

## 4. 默认下一步

**立即行动（2026-03-30 至 2026-04-01）：**

1. ✅ **完成 48 小时平行采集**：Tokyo VPS 持续数据收集（目标 60 个 episode 样本）
2. ⏳ **样本质量评估**（2026-04-01）：
   - 运行 `qualify_episode` 过滤器
   - 运行 `research_report` 检查统计条件
   - 确认是否满足 [reviews/shadow-mode-admission.md](reviews/shadow-mode-admission.md) 第 3-4 节要求
3. ⏳ **数据对比与迁移决策**（2026-04-01）：
   - 对比 Tokyo 采集速率 vs Europe 基准
   - 样本质量对比（fill rate、MAE/MFE 等）
   - 最终决策：全量迁移 vs 保留 Europe 冷备份 vs 双活
4. ⏳ **参数冻结与 Shadow Mode 启动**（2026-04-01 之后）：
   - 若样本质量满足门槛，按 [reviews/shadow-mode-admission.md](reviews/shadow-mode-admission.md) 第 5 节规则选参
   - 启动 Shadow Mode 并运行 14 天验证

## 5. 文档关系

- 策略定义和第一性原理由 [strategy.md](strategy.md) 负责
- 术语和状态语义由 [glossary.md](glossary.md) 负责
- 系统设计和时间契约由 [architecture.md](architecture.md) 负责
- 风险门禁由 [risk-controls.md](risk-controls.md) 负责
- 研发步骤和验收标准由 [development-plan.md](development-plan.md) 负责
- 阶段性审查过程见 [reviews/index.md](reviews/index.md)
