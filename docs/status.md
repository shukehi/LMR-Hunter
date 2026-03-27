# 项目状态

## 1. 文档目的

本文档记录项目当前所处阶段、当前阻塞项和默认下一步动作。

本文件回答的是：

- 项目现在进行到哪里
- 当前哪些结论已经成立，哪些还没成立
- 接下来默认先做什么

## 2. 当前阶段

**Observe Mode 已上线运行（2026-03-27）。**

当前已经完成：

- 阶段 1：数据完整性保证
- 阶段 2：时间语义修复
- 阶段 3：Episode 模型落地
- 阶段 4：Outcome 回填

当前尚未完成：

- 阶段 5：可交易口径研究报告
- 阶段 6：Shadow Mode 准入门槛冻结

## 3. 当前阻塞项

当前项目仍不能把“市场观察”直接当作“策略已验证”，主要阻塞项为：

1. 有效 `episode` 样本量仍不足，尚未达到稳定研究基线
2. 主报告口径还未完全切换到 fill rate / expectancy / MAE / MFE 等可交易指标
3. Shadow Mode 的准入清单还未作为单一门禁冻结

## 4. 默认下一步

默认按以下顺序推进：

1. 继续积累有效 `episode` 样本，目标至少 `20` 条
2. 完成阶段 5 的可交易口径报告，明确净期望是否为正
3. 将可交易研究结论回写到策略、风控和开发计划主文档
4. 冻结 Shadow Mode 准入门槛
5. 仅在以上条件满足后，才推进 Shadow Mode 参数扫描与虚拟撮合

## 5. 文档关系

- 策略定义和第一性原理由 [strategy.md](strategy.md) 负责
- 术语和状态语义由 [glossary.md](glossary.md) 负责
- 系统设计和时间契约由 [architecture.md](architecture.md) 负责
- 风险门禁由 [risk-controls.md](risk-controls.md) 负责
- 研发步骤和验收标准由 [development-plan.md](development-plan.md) 负责
- 阶段性审查过程见 [reviews/index.md](reviews/index.md)
