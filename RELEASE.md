# RELEASE.md

历史版本见 `docs/changelog/`。

- **v0.6.0（2026-08-09）**：[docs/changelog/2026-08-09-v0.6.0.md](docs/changelog/2026-08-09-v0.6.0.md) — 插件自有 pi-ai LLM selector、provider/base URL 和环境变量密钥合同
- **v0.5.0（2026-08-08）**：[docs/changelog/2026-08-08-v0.5.0.md](docs/changelog/2026-08-08-v0.5.0.md) — 出站图片、SQLite 迁移和可靠性边界收口
- **v0.4.0（2026-08-08）**：[docs/changelog/2026-08-08-v0.4.0.md](docs/changelog/2026-08-08-v0.4.0.md) — TurnAnchor、trusted_user 和固定 batch/authority
- **v0.3.3（2026-08-07）**：[docs/changelog/2026-08-07-v0.3.3.md](docs/changelog/2026-08-07-v0.3.3.md) — lease phase 恢复和人工 retry 收口
- **v0.3.2（2026-08-07）**：[docs/changelog/2026-08-07-v0.3.2.md](docs/changelog/2026-08-07-v0.3.2.md) — PR #8 交付修复和 completion/reaction 边界
- **v0.3.1（2026-08-06）**：[docs/changelog/2026-08-06-v0.3.1.md](docs/changelog/2026-08-06-v0.3.1.md) — reconnect、配置合同和管理动作恢复
- **v0.3.0（2026-08-06）**：[docs/changelog/2026-08-06-v0.3.0.md](docs/changelog/2026-08-06-v0.3.0.md) — lease fencing、非幂等出站和分层触发
- **v0.2.0（2026-08-05）**：[docs/changelog/2026-08-05-v0.2.0.md](docs/changelog/2026-08-05-v0.2.0.md) — shared session、持久队列和安全出站合同
- **v0.1.0（2026-08-04）**：[docs/changelog/2026-08-04-v0.1.0.md](docs/changelog/2026-08-04-v0.1.0.md) — OneBot 11 接入最小闭环（私聊+群聊、消息查询工具、权限管理）

## 当前 worktree

Task 7 在当前 `0.6.0` 版本上补充媒体同轮去重、默认纯文本、控制面状态提示、运行时策略 reload、
generic Hermes 工具硬门禁、持久 selector/cooldown、`/context` 旁路、lease/reaction 收口、
活跃窗口短确认词直触发（`engaged_ack`）、turn 收口补触发、自适应 debounce、selector 候选 👀
查看提示和 ⌛ 正在回复提示（`processing_reaction_emoji_id` 默认改为 `8971`，`9203` 不被 QQ
reaction API 支持）。
这些变更仍需通过分支 PR、合并后复审和指定白名单范围内的 Arch 验收，尚未构成新的发布版本。

## 升级与迁移

- 建议升级前备份 `HERMES_HOME/onebot11/queue.sqlite3`、审计 JSONL 和媒体临时目录。
- 当前队列 schema 为 12；已知旧 schema 会在启动时自动迁移，未知更高版本会拒绝启动。
- 不自动迁移旧的 per-user Hermes session 历史；切换到 shared session 前应人工确认历史是否保留。
- `uncertain`/`failed` 消息不会自动重放，升级后仍需超级管理员明确 `resolve retry|discard`。
- 队列消息的 `resolve retry` 只有在原 anchor、authority 和 batch 边界可证明时才生成新的
  request_id；旧 trigger 缺失或 authority 不明的记录保持 hold。
- 管理动作 unknown 的 retry 只解除 fingerprint 阻断，必须重新生成预览并确认；不会直接重放。
- 本轮没有新增必填配置，但升级后必须重新核对 `allowed_groups`、DM allowlist、
  `super_admins`、`trusted_users`、OneBot `self_id` 和 HTTP/WS token。
