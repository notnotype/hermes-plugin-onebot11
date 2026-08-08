# ADR-0006：TurnAnchor、摘要临时注入与延期边界

- 状态：已接受
- 日期：2026-08-07

## 决策

### TurnAnchor 生命周期

一个群可以有多个 `pending` TurnAnchor，但同一时间只有一个活动 lease，并按 `anchor_seq` 串行处理。每个 anchor：

- 绑定一条真实的 pending 消息和一个 `anchor_kind`；
- 固定“上一个 anchor 之后到当前 anchor”为本轮 batch；
- authority、角色、reaction 和 reply 都从这条真实消息推导；
- 后续消息不会被旧 turn 通过动态 `peek` 偷吃。

硬触发、selector、管理员 flush 和 recovery 都必须携带明确的 message key。selector 指向的消息消失时，旧判断直接失效，不能回退到队列最早消息。失败恢复保留自己的 anchor；单群单活动 lease和最早 anchor backoff 保证顺序。

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

TurnAnchor 的唯一职责是保存真实触发消息和 batch 边界；并发限制由 dispatcher 的单群活动 lease 负责，而不是靠“每群一个 pending trigger”的唯一索引。这样既保留多用户/多次硬触发的 authority，又避免旧失败恢复把群卡死。

摘要重复进入 shared session 会让每轮输入持续增长，并把历史群文本伪装成普通用户消息。临时注入能保留理解上下文，又不把同一摘要复制到每个历史 turn。

RAG 和自动自优化都需要额外的索引、评估、权限和审核闭环；当前需求的主要风险在队列可靠性和触发成本，不值得在本轮引入新的持久化系统。

## 影响

- 多个 pending anchor 会增加少量 SQLite 行数，但不会增加同群并发 Agent turn；最早 anchor 失败或退避时，后续 anchor 按序等待。
- 内部 recovery anchor 可能锚定最早待重试消息，但仍使用真实消息的 authority 和 reaction 规则。
- 旧 Hermes 能继续运行，但摘要可能以有界普通文本进入 transcript，输入成本高于支持 `channel_prompt` 的版本。
- 想加入 RAG 或自动优化时，必须另建任务和验收合同，不能把当前 SQLite 摘要候选悄悄升级成语义检索。
