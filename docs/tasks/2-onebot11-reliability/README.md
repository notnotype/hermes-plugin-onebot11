# OneBot 11 可靠性与安全完善

- 关联需求：OneBot 11 插件整体完善计划
- 状态：PR #12 已合并；Issue #13 的 `fix/i13-onebot11-closeout` 完成恢复顺序、authority 收紧、旧 task fencing、畸形 WS 隔离、`get_image` 输入解析和图片 preflight，已创建插件 PR #14 且 CI 通过。Hermes strict auxiliary 与媒体合同分别保留在独立 worktree，Arch 仍未部署本轮代码。
- 开始日期：2026-08-05

## 目标

- 群固定一个 shared Hermes session；群消息全部先入持久队列，每个真实 anchor 固定自己的 batch 边界并按序串行 follow-up。
- @、关键词、always 和显式旁路 LLM trigger 驱动群 turn。
- 通过不可变 `CallerContext`、`ChatTarget` 和 `(session_id, turn_id)` binding 做权限和出站隔离。
- 队列崩溃可恢复，OneBot 非幂等出站结果未知时停止自动重放并交给管理员 resolve。

## 已实现

1. SQLite WAL 队列：schema 版本迁移、消息和 TurnAnchor 去重、条数/字节边界、摘要、lease heartbeat、tombstone、`uncertain` 和人工 retry/discard。
2. 群级 dispatcher：每群一个活动 lease，启动恢复过期租约和持久 trigger request，支持 pause/resume。
3. 触发器：Unicode `casefold` 关键词、@、always、问句/记忆候选、5 秒 trailing debounce、60 秒活跃窗口和显式 auxiliary LLM 三态判断；模型失败按不触发。
4. 权限：普通用户和 trusted_user 默认/配置都只能只读，超级管理员列表和角色工具并集；群管理写工具预览 + 同群同管理员短期单次确认。
5. 协议可靠性：WS 有界接收、同 chat 顺序、inflight 限制、失败关闭连接；HTTP 查询有限重试，非幂等动作永不自动重试；媒体 SSRF/类型/大小限制；支持 LLBot reaction action。
6. 生命周期：部分发送、连接中断、取消或 turn 在发送后失败均进入 `uncertain`；同一 adapter reconnect 会重新打开 SQLite/dispatcher，内存 active 状态回到 idle；群 turn 默认显示 👀，收尾时尽力清理；确认命令在 adapter 入站层处理。
7. 管理动作台账：确认后先持久化 `started`；崩溃恢复为 `unknown`，同 fingerprint 阻断重复调用；管理员通过 operation id 明确 `retry_armed` 或 `discarded`。
8. 收口修复：旧 lease 与后来 pending anchor 在事务中保持各自顺序；多群 backoff 不互相阻塞；raw `self_id` 不匹配拒绝；畸形消息单帧丢弃；completion 后续状态异常不再冒泡为第二个用户错误；reaction/authority 锚定真实触发消息；shared session 摘要优先临时注入。
9. 出站媒体：图片使用受限 `base64://` segment；Hermes 聚合文本/媒体结果，unknown delivery 不自动重试、plain-text fallback 或 cron standalone fallback。

## 关键决策

- 群不自动把旧 per-user 历史合并到 shared session，见 [ADR-0001](../../adr/0001-shared-group-session.md)。
- OneBot 11 非幂等动作无法保证 exactly-once；未知结果不自动重试，见 [ADR-0002](../../adr/0002-onebot-unknown-outcome.md)。
- lease fencing 和持久化前的 WS 可靠性边界见 [ADR-0003](../../adr/0003-lease-fencing-and-ws-reliability-boundary.md)。
- reconnect、管理动作台账和 CI/本地验收边界见 [ADR-0005](../../adr/0005-reconnect-operation-ledger-and-verification.md)。
- TurnAnchor、摘要临时注入和延期边界见 [ADR-0006](../../adr/0006-pending-trigger-and-context-lifecycle.md)。
- TurnAnchor 与 shared session 的 batch/authority 边界见 [ADR-0008](../../adr/0008-turn-anchor-shared-session.md)。
- trusted_user 只读边界见 [ADR-0009](../../adr/0009-trusted-user-read-only.md)。
- 确定性记忆候选、RAG 和运行时自优化延期见 [ADR-0010](../../adr/0010-deterministic-memory-and-deferred-optimization.md)。
- recovery anchor 顺序、authority shrink、task/epoch fencing 和 malformed WS 边界见 [ADR-0013](../../adr/0013-recovery-anchor-order-and-authority-shrink.md)。
- Hermes strict/media 合同与 Arch 验收边界见 [ADR-0014](../../adr/0014-hermes-delivery-contract-and-arch-boundary.md)。

## 验证

- 本地插件门禁：`.venv\\Scripts\\python.exe -m pytest -q` 的纯插件结果见最终交付记录；纯插件环境只跳过需要 Hermes gateway 的 adapter 集成测试。
- Hermes 集成：运行 `scripts\\verify_hermes_integration.ps1` 或跨平台 Python 入口；当前独立 Hermes `feat/i13-onebot11-contract` worktree 组合为 `263 passed`，覆盖真实 adapter import、注册、4 个 hooks、工具 handler、shared session key、TurnAnchor authority、lease fencing、同实例 reconnect、配置合同、operation resolve、触发竞争、上下文分段、媒体孤儿清理、raw self_id、home cron、reaction 生命周期和出站图片/unknown delivery 合同。
- 严格 auxiliary 回归：`3 passed`，覆盖新 API 的 no-fallback/单次尝试合同和旧 Hermes API 缺少参数时的安全禁用。
- 静态检查：`ruff check .` 当前通过；独立临时 venv 中 `uv pip install -e ".[dev]"` 和 `import onebot11` 通过。
- Arch + LLBot 外部联调：2026-08-08 严格使用机器人 QQ `3101482118`、群白名单 `1072992996`、私聊白名单 `2056963663` 和 `home_channel_type=dm`。旧 TurnAnchor 版本的真实 WS/HTTP 连接、真实历史 message ID 的 reaction 添加/移除、TurnAnchor batch 边界、重启后 pending 保留和白名单外群拒绝通过；随后在隔离 queue 上完成了 image-only、文字+图片、多图的真实 OneBot segment/reaction smoke。入站仍为受控 WS payload，图片 smoke 也未经过生产 Agent pipeline，不等同于真人 QQ 发言；双用户并发、部分成功/unknown 出站和 resolve 仍待验收。Arch live queue schema 10 尚未切换到当前 schema 11 分支。

## 计划出入

- 已完成原计划第一、第二阶段的本地实现，并增加了 WS 处理失败主动断开、媒体总大小限制、跨重启孤儿目录清理和非 OneBot hook 隔离。
- 语义摘要仍保持确定性摘要为主；`onebot11_summary` auxiliary、自动语义压缩和运行时自优化不在本轮实现范围内。审计、基础运维命令和持久管理动作台账已纳入当前实现，真实 QQ 上的管理写操作仍待验收。
- 本轮联调中的测试消息只发送到用户指定的群 `1072992996` 和用户 `2056963663`；没有执行禁言、踢人、撤回或全员禁言。远端保留了配置备份，实验 queue 已移出运行配置；未把 schema 10 降级或覆盖。
