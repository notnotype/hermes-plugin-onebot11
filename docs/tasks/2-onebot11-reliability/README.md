# OneBot 11 可靠性与安全完善

- 关联需求：OneBot 11 插件整体完善计划
- 状态：本地实现完成；Task 4 收口补充了访问策略、cooldown/reaction 恢复和媒体边界，既有 Arch 联调尚未覆盖本轮新增代码
- 开始日期：2026-08-05

## 目标

- 群固定一个 shared Hermes session；群消息全部先入持久队列。
- @、关键词、always 和显式旁路 LLM trigger 驱动群 turn。
- 通过不可变 `CallerContext`、`ChatTarget` 和 `(session_id, turn_id)` binding 做权限和出站隔离。
- 队列崩溃可恢复，OneBot 非幂等出站结果未知时停止自动重放并交给管理员 resolve。

## 已实现

1. SQLite WAL 队列：schema 版本迁移、消息和 trigger 去重、条数/字节边界、摘要、lease heartbeat、tombstone、`uncertain` 和人工 retry/discard。
2. 群级 dispatcher：每群一个活动 lease，启动恢复过期租约和持久 trigger request，支持 pause/resume。
3. 触发器：Unicode `casefold` 关键词、@、兼容 `require_mention=false`、冷却和显式 auxiliary LLM trigger；模型失败按不触发。
4. 权限：普通用户默认只读，超级管理员列表和角色工具并集；群管理写工具预览 + 同群同管理员短期单次确认。
5. 协议可靠性：WS 有界接收、同 chat 顺序、inflight 限制、失败关闭连接；HTTP 查询有限重试，非幂等动作永不自动重试；媒体 SSRF/类型/大小限制；支持 LLBot reaction action。
6. 生命周期：部分发送、连接中断、取消或 turn 在发送后失败均进入 `uncertain`；群 turn 默认显示 👀，收尾时尽力清理；确认命令在 adapter 入站层处理。
7. 收口：cron 和恢复复用同一访问策略；cooldown 时间与 reaction 状态持久化；关闭时 fencing 旧 turn；`delegate_task` fail-closed；非 URL 图片通过 `get_image` 和显式媒体根目录处理。

## 关键决策

- 群不自动把旧 per-user 历史合并到 shared session，见 [ADR-0001](../../adr/0001-shared-group-session.md)。
- OneBot 11 非幂等动作无法保证 exactly-once；未知结果不自动重试，见 [ADR-0002](../../adr/0002-onebot-unknown-outcome.md)。
- lease fencing 和持久化前的 WS 可靠性边界见 [ADR-0003](../../adr/0003-lease-fencing-and-ws-reliability-boundary.md)。

## 验证

- 本地协议和状态机：`uv run --extra dev pytest -q`，当前结果 `108 passed, 1 skipped`；adapter 文件在没有 Hermes 依赖的环境按设计跳过。
- Hermes 集成：使用本地 Hermes 源码和依赖环境运行全套测试，当前结果 `163 passed`，覆盖 adapter import、注册、hooks、工具 handler、shared session key、lease 恢复、cooldown 重唤醒、reaction 清理、媒体根目录和非幂等出站。
- 静态检查：`.venv\\Scripts\\python.exe -m ruff check .`，当前通过。
- Arch + LLBot 8.1.5 外部联调已完成：Hermes 实际加载 0.2.0 插件，群 `1072992996` 使用 shared session，私聊白名单只有 `2056963663`；真实 WS/HTTP 连接、允许群 @/私聊 Agent 回复、pending 重启恢复、管理员 flush、非白名单拒绝、审计和群处理 reaction（`set=true` -> 回复 -> `set=false`）均已验证。Agent 入站使用真实 WS 的合成 OneBot payload，真人 QQ 入站、双用户并发、确认写操作和 unknown 出站仍待验收。

## 计划出入

- 已完成原计划第一、第二阶段的本地实现，并增加了 WS 处理失败主动断开、媒体总大小限制、跨重启孤儿目录清理和非 OneBot hook 隔离。
- 语义摘要仍保持确定性摘要为主；`onebot11_summary` auxiliary、审计展示、真实 QQ 联调和后续运维展示不在本轮本地实现完成范围内。
- 本轮联调中的测试消息只发送到用户指定的群 `1072992996` 和用户 `2056963663`；远端保留了配置备份和移出插件扫描目录的旧 worktree 回滚 symlink。
