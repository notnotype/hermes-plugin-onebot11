# RELEASE.md

历史版本见 `docs/changelog/`。

- **v0.1.0（2026-08-04）**：[docs/changelog/2026-08-04-v0.1.0.md](docs/changelog/2026-08-04-v0.1.0.md) — OneBot 11 接入最小闭环（私聊+群聊、消息查询工具、权限管理）
- **v0.2.0（2026-08-05）**：[docs/changelog/2026-08-05-v0.2.0.md](docs/changelog/2026-08-05-v0.2.0.md) — shared 群 session、持久队列、多触发器、租约和 unknown 出站合同
- **v0.3.0（2026-08-06）**：[docs/changelog/2026-08-06-v0.3.0.md](docs/changelog/2026-08-06-v0.3.0.md) — 上下文物化、细粒度角色权限、群 slash command 和 reaction
- **v0.3.1（2026-08-07）**：[docs/changelog/2026-08-07-v0.3.1.md](docs/changelog/2026-08-07-v0.3.1.md) — 安全可靠性收口、QueueStore v8、恢复白名单、reaction 有限恢复和 LLM trigger 去重
- **v0.4.0（2026-08-07）**：[docs/changelog/2026-08-07-v0.4.0.md](docs/changelog/2026-08-07-v0.4.0.md) — TurnAnchor 独立 followup、不可变 authority、自动锚点选择、双阶段 reaction 和 batch/媒体边界收口

## v0.3.1 迁移指南

升级前停止 Hermes，并备份 `HERMES_HOME/onebot11/queue.sqlite3` 与 `config.yaml`。

- v7 → v8 会自动迁移，无需手工改表；高于 v8 的未知 schema 会拒绝启动。
- 没有新增必填配置；群 session 仍固定 `shared`，不自动迁移旧 `per_user` Hermes session 历史。
- 已存在的 `uncertain`/`failed` 消息仍需管理员明确 `/onebot resolve retry|discard`。
- unknown 群管理动作不会自动重试；v0.3.1 仍需重新生成预览和 confirmation token。升级 v0.4.0 后，只有管理员明确执行 `/onebot resolve retry` 才会创建新的 retry anchor；不会复用旧 request id，也不会猜测 legacy authority。
- 升级后重新核对 `allowed_groups`、私聊 `allowlist` 和机器人 token；联调白名单只允许群 `1072992996` 与用户 `2056963663`。

完整步骤见：[docs/migrations/2026-08-07-v0.3.1.md](docs/migrations/2026-08-07-v0.3.1.md)。

## v0.4.0 迁移指南

v0.4.0 是触发与权限行为升级：QueueStore v8/v9 → v10 自动迁移；`require_mention=false` 不再直接授予发送者 authority；自动 selector 改为返回 `anchor_seq`；确认令牌被移除；当前 turn 使用不可变权限快照。PR #10 继续收口 lease fencing、崩溃退避、retry 新 anchor、reaction 迁移、真实/内部 message ID 分离、私聊作用域和 HTTP redirect 边界。

升级前停止 Hermes 并备份队列和配置。无法证明单一 authority 的旧 batch 会进入 legacy hold，不会自动执行。完整步骤见：[docs/migrations/2026-08-07-v0.4.0.md](docs/migrations/2026-08-07-v0.4.0.md)。

v0.4.0 当前仍未合并/发布；本 worktree 使用 Hermes 源码和 site-packages 的完整测试为 `274 passed`，Ruff、compileall 与 `git diff --check` 已通过。Task 5 提交 `5b657e5` 已部署到 Arch 独立 worktree，QueueStore schema v10、Hermes 进程、反向 WS `18880` 和 LLBot compose 已只读核对；外部输入使用合成 WS payload，真实 QQ reply/reaction/unknown 仍不计为已验收。这不等同于真人 QQ 或 OneBot 11 exactly-once 保证。白名单仍只允许群 `1072992996` 与私聊用户 `2056963663`。
