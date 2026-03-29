# 文档地图

## 1. 文档目的

本文档给出仓库内文档的阅读顺序、职责边界和主规范位置。

本文件回答的是：

- 每份文档分别回答什么问题
- 当前真相应该以哪份文档为准
- 审查记录与长期规范应如何区分

## 2. 阅读顺序

首次进入项目时，建议按以下顺序阅读：

1. [项目状态](status.md) - 了解当前阶段、阻塞项和下一步
2. [策略文档](strategy.md) - 了解第一性原理定义、策略边界和可交易命题
3. [术语表](glossary.md) - 统一术语和时间语义
4. [架构文档](architecture.md) - 了解系统模块、数据流和持久化契约
5. [风控文档](risk-controls.md) - 了解风险边界、暂停条件和上线门槛
6. [开发计划](development-plan.md) - 了解后续研发顺序、验收标准和门禁
7. [审查记录目录](reviews/index.md) - 查看历史复盘和阶段性优化计划

## 3. 文档分工

| 文档 | 主问题 | 是否长期规范 |
| --- | --- | --- |
| [status.md](status.md) | 当前运行到哪一阶段、现在先做什么 | 是 |
| [strategy.md](strategy.md) | 为什么做、何时做、何时不做 | 是 |
| [glossary.md](glossary.md) | 关键术语、时间语义、状态语义是什么意思 | 是 |
| [architecture.md](architecture.md) | 系统边界、模块职责、数据流、存储契约 | 是 |
| [risk-controls.md](risk-controls.md) | 风险边界、禁止事项、暂停恢复、实盘门槛 | 是 |
| [development-plan.md](development-plan.md) | 研发阶段、验收门禁、后续排期 | 是 |
| [reviews/index.md](reviews/index.md) | 审查记录入口 | 否 |
| [reviews/optimization-plan.md](reviews/optimization-plan.md) | 第一轮结构性优化审查记录 | 否 |
| [reviews/runtime-optimization-plan.md](reviews/runtime-optimization-plan.md) | 运行态复盘后的第二轮审查记录 | 否 |
| [reviews/shadow-mode-admission.md](reviews/shadow-mode-admission.md) | Shadow Mode 准入条件预注册（样本合格标准、统计门槛、参数选择规则） | 否 |
| [market-sessions.md](market-sessions.md) | 全球主要市场开盘时间与各盘口对策略的影响 | 是 |

## 4. 维护规则

- 第一性原理定义、策略命题和可交易性判断以 [strategy.md](strategy.md) 为准。
- `episode`、`outcome`、`event_ts`、`DEGRADED` 等术语以 [glossary.md](glossary.md) 为准。
- 当前阶段状态和下一步动作以 [status.md](status.md) 为准。
- 审查记录中的结论，只有在回写到长期规范文档后，才视为项目正式规则。
- 同一条规则只允许有一个主维护位置，其他文档应引用而不是重复改写。
