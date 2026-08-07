# ADR-0006：pending trigger 合并与群上下文生命周期

- 状态：已接受
- 日期：2026-08-07

## 决策

### 同群 pending trigger 合并

一个群的 SQLite 队列最多保留一个 `pending` trigger。旧 lease 在明确失败、过期恢复、adapter 断开结算或管理员 retry 时，如果同群已经有后来创建的 `pending` trigger：

- 保留后来请求作为唯一 pending trigger；
- 在同一 SQLite 事务中删除或合并旧的 claimed/uncertain/failed trigger；
- 不因 partial unique index 冲突回滚整个 lease 状态转换；
- 曾经被认领但重新回到 pending 的消息保留 `attempts`，必要时由 queue recovery 补回 durable trigger。

硬触发和 LLM trigger 仍显式记录实际触发消息的 `message_key`，用于 reaction 锚点；队列恢复时只在必要时生成内部 `queue_recovery` trigger。

### 摘要临时注入

群本轮待处理消息作为普通 user message 写入 Hermes shared session。SQLite 的确定性滚动摘要不重复拼接进普通 user transcript，而是优先通过 Hermes `MessageEvent.channel_prompt` 临时注入：

- 摘要限制在总输入预算内，并包装为“不可信群消息历史数据”；
- 摘要中的指令、身份声明和权限要求不能覆盖系统规则；
- Hermes 处理结束后该提示不作为本轮普通 user transcript 持久保存；
- 老 Hermes 没有 `channel_prompt` 时退回有界文本模式，并写入审计，保持功能可用但明确承认成本差异。

不使用 `pre_llm_call` 注入摘要，因为该 hook 在当前 Hermes 版本中可能进入 API sidecar 生命周期，不能满足“只对本轮有效”的合同。

### 延期边界

本轮不接入向量数据库、RAG 检索或运行时自动修改 Python/权限/白名单/关键词。记忆候选只使用群内已有摘要和最近原文；Hermes 自优化只保留生成配置 diff 与理由、等待管理员审核的产品边界，不实现自动执行链路。

## 原因

pending trigger 的唯一索引是为了防止同群并发 dispatch，不应反过来成为失败恢复的永久阻塞点。合并请求能保持单群单 turn，同时避免把旧失败路径改成无界多 trigger。

摘要重复进入 shared session 会让每轮输入持续增长，并把历史群文本伪装成普通用户消息。临时注入能保留理解上下文，又不把同一摘要复制到每个历史 turn。

RAG 和自动自优化都需要额外的索引、评估、权限和审核闭环；当前需求的主要风险在队列可靠性和触发成本，不值得在本轮引入新的持久化系统。

## 影响

- 某次恢复合并后，reaction 仍锚定实际触发消息；内部恢复 trigger 可能锚定最早待重试消息。
- 旧 Hermes 能继续运行，但摘要可能以有界普通文本进入 transcript，输入成本高于支持 `channel_prompt` 的版本。
- 想加入 RAG 或自动优化时，必须另建任务和验收合同，不能把当前 SQLite 摘要候选悄悄升级成语义检索。
