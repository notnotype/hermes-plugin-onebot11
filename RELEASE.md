# RELEASE.md

历史版本见 `docs/changelog/`。

- **v0.1.0（2026-08-04）**：[docs/changelog/2026-08-04-v0.1.0.md](docs/changelog/2026-08-04-v0.1.0.md) — OneBot 11 接入最小闭环（私聊+群聊、消息查询工具、权限管理）
- **v0.2.0（2026-08-05）**：[docs/changelog/2026-08-05-v0.2.0.md](docs/changelog/2026-08-05-v0.2.0.md) — shared 群 session、持久队列、多触发器、租约和 unknown 出站合同
- **v0.3.0（2026-08-06）**：[docs/changelog/2026-08-06-v0.3.0.md](docs/changelog/2026-08-06-v0.3.0.md) — 上下文物化、细粒度角色权限、群 slash command 和 reaction
- **v0.3.1（2026-08-07）**：[docs/changelog/2026-08-07-v0.3.1.md](docs/changelog/2026-08-07-v0.3.1.md) — 安全可靠性收口、QueueStore v8、恢复白名单、reaction 有限恢复和 LLM trigger 去重

## v0.3.1 迁移指南

升级前停止 Hermes，并备份 `HERMES_HOME/onebot11/queue.sqlite3` 与 `config.yaml`。

- v7 → v8 会自动迁移，无需手工改表；高于 v8 的未知 schema 会拒绝启动。
- 没有新增必填配置；群 session 仍固定 `shared`，不自动迁移旧 `per_user` Hermes session 历史。
- 已存在的 `uncertain`/`failed` 消息仍需管理员明确 `/onebot resolve retry|discard`。
- unknown 群管理动作不会自动重试；需要重新生成预览和新的 confirmation token，并确认可能重复执行。
- 升级后重新核对 `allowed_groups`、私聊 `allowlist` 和机器人 token；联调白名单只允许群 `1072992996` 与用户 `2056963663`。

完整步骤见：[docs/migrations/2026-08-07-v0.3.1.md](docs/migrations/2026-08-07-v0.3.1.md)。
