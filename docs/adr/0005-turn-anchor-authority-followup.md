# ADR-0005：TurnAnchor、authority 与独立 followup

状态：Accepted（2026-08-07）

## 背景

群共享一个 Hermes session，但一批 pending 消息可能包含多个用户的独立任务。把多个触发合并为一个 turn 会产生不可回答的问题：本轮到底继承哪个用户的角色和工具权限？使用最后消息、最高权限或权限交集都会让业务语义与授权来源脱节。

## 决策

采用以下一一对应关系：

```text
一条锚点消息 -> 一个 TurnAnchor -> 一个不可变 authority -> 一个独立 Hermes followup
```

- @、mention、关键词和显式自定义规则属于精确触发；入队事务为该消息创建 `message` anchor。
- 自动 evaluator 只从未锚定消息快照选择一个真实 `anchor_seq`。输入不含角色或工具配置，输出不含 caller、role、tools 或自由文本指令。
- `message` anchor 的 authority 完全继承锚点消息真实发送者；`operator` anchor 只由管理员 `/onebot flush` 创建并继承发命令者。
- turn 开始时快照角色和精确工具集合。配置变化从下一 turn 生效；白名单、目标、lease 和 adapter 生命周期仍可立即 fencing。
- batch 只包含上一个 anchor 边界之后、当前 `anchor_seq` 之前（含锚点）的消息。锚点之后的消息不会进入当前 turn。
- 多个 anchor 在同一 shared session 串行执行，不使用 steer。最早 uncertain、failed、legacy hold 或 backoff anchor 阻塞后续 anchor。
- `⏳` 表示 anchor 已持久排队，`👀` 表示该 anchor 当前正在处理。set 不恢复重放；unset 有限恢复。
- 角色允许、作用域正确且 lease 有效时，写工具直接执行。unknown 不自动重试，同一 turn 的同一 unknown 动作禁止重复调用。

## 上下文与缓存

主 Agent 正常读取 shared Hermes session 历史。当前 TurnAnchor batch 以结构化 JSONL 物化为本轮 user message，包含 `seq`、消息 ID、发送者、turn-start role、reply、segment/media markers、正文和 anchor 标记。

`pre_llm_call` 注入 authority reminder。Hermes 将该内容追加到当前 user request，不修改稳定 system prompt，因此保留 system prefix cache；实际工具授权只认 binding、lease 和不可变权限快照。

## 恢复

schema v9 把可证明的一对一旧 trigger 迁移为 message anchor。旧 LLM trigger、活跃 lease、uncertain/failed batch 或缺失锚点的记录不能安全推断 authority，转为 `legacy hold` 或未锚定 pending，由管理员处理后重新明确触发。

## 取舍

本方案不引入工作流 DAG、持久动作 ledger、WS 原始 spool 或多 authority turn。群内任务保持串行，牺牲并行吞吐换取 shared session 历史顺序、权限可解释性和简单恢复合同。

