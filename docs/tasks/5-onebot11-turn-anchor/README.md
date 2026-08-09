# Task 5：OneBot 11 TurnAnchor 与 shared session 收口

- 关联需求：OneBot 11 原始需求中的“群一个 shared session + 队列上下文”
- 状态：PR #12 已合并；Issue #13 的 `fix/i13-onebot11-closeout` 在 PR #12 基础上补齐恢复顺序、authority shrink、旧 task/epoch fencing、畸形 WS 隔离、`get_image` 和图片发送前置检查，已创建插件 PR #14 且 CI 通过。真实 Agent 图片 pipeline、真人并发、unknown resolve 和 Hermes 独立 PR 仍未完成。
- 分支：`fix/i13-onebot11-closeout`

## 目标

在一个群共享 Hermes session 的前提下，把每次触发固定成一个可审计的 TurnAnchor：

- anchor 对应一条真实 OneBot 消息；
- 当前 turn 只消费上一个 anchor 之后、当前 anchor 之前的固定 batch；
- 同群可以有多个 pending anchor，但同一时间只有一个活动 lease；
- authority、角色、权限、reaction 和 reply 都来自当前 anchor，不依赖 session 最近来源缓存。

## 已实现

1. SQLite schema v11 保留消息、anchor、authority 快照、lease phase、出站 marker、失败退避、摘要和 operation ledger，并支持真实 v7/v8/v9/v10 表结构迁移。
2. hard trigger、selector、管理员 flush 和 recovery 都写入明确 anchor kind。
3. claim 按 anchor 序号串行认领；后续消息不会被旧 turn 偷吃。
4. selector 使用显式 message key；目标消息消失时丢弃旧判断，不静默改绑到最早消息。
5. queue turn 从 anchor 真实消息推导 `CallerContext`，写入 anchor id/seq/kind/message id 元数据。
6. `👀` reaction 和 reply 只使用真实 anchor message id；内部 hash 不作为 OneBot 目标。
7. `trusted_user` 通过 `roles.trusted_user.users` 配置，只允许明确配置的只读工具，不能修改权限或白名单；旧 authority 只能按当前角色收紧，不能扩大。
8. 旧 Hermes 不支持 strict auxiliary 或 `channel_prompt` 时安全降级并审计；不回退主 Agent。
9. OneBot 入站 `file` 图片先通过 `get_image` 解析；出站图片使用 Hermes 允许目录中的 `base64://` segment；Hermes 媒体结果全部明确成功才确认 image-only turn，unknown 不重试、不 fallback。

## 不纳入

- RAG、向量库和语义记忆检索；
- Hermes 运行时自动修改 Python、权限、白名单或关键词；
- queued `⏳` reaction；
- OneBot 12、原始 WS spool 和 exactly-once 非幂等出站。

## 验证

- 纯插件：`pytest -q`、`ruff check .`、editable install、`import onebot11`。
- Hermes 组合：`scripts/verify_hermes_integration.py` 使用临时 `HERMES_HOME` 验证真实注册、9 个工具、4 个 hooks、shared session、TurnAnchor authority、reconnect、queue recovery、图片 base64 segment 和 strict auxiliary；当前结果为 `263 passed`，strict auxiliary 定向 `3 passed`。
- 外部：只允许群 `1072992996`、用户 `2056963663`，机器人 QQ `3101482118`；已验证真实历史 message ID 的 reaction 添加/移除、TurnAnchor batch 边界、重启 pending 保留和白名单外拒绝，并在隔离 queue 上验证真实 OneBot image-only、文字+图片和多图 `base64://` segment。该 smoke 未经过生产 Agent pipeline；真人并发、部分成功/unknown resolve 和生产 schema 10→11 迁移仍需联调。

## 计划出入

计划中的“所有 pending trigger 合并为一个”被 TurnAnchor 取代：为了保留每个真实触发消息的 authority 和独立 follow-up，当前实现允许多个 pending anchor，但仍保持单群单活动 lease、严格顺序和失败阻塞。没有增加原始 WS spool 或新的数据库。
