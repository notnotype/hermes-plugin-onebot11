# PROJECT-STATUS

仓库级现状报告。实现变更通过分支和 PR 收口；本文只记录当前代码合同、验证证据和仍需外部联调的事项。

## 当前状态

- **阶段**：`fix/i13-onebot11-closeout` 基于已合并的 `master`/PR #12，完成 Issue #13 的恢复、权限、连接生命周期和媒体输入收口，已创建插件 PR #14 且 CI 通过。Hermes strict auxiliary 与媒体/unknown 合同在独立 worktree，Arch 仍运行旧插件/旧 Hermes，未切换生产队列。
- **核心合同**：群固定一个共享 session；群消息持久入队；每个真实 TurnAnchor 固定 batch 和 authority，同群按序单 lease follow-up；非幂等出站结果未知时进入 `uncertain`，不自动重放。
- **本地验证**：纯协议/状态机测试和 Hermes 组合测试均通过；独立 Hermes worktree 的 strict/media 合同只作为组合验收依赖，不代表远端 Hermes 已发布。

## 模块状态

| 模块 | 状态 | 当前合同 |
|---|---|---|
| `onebot11/message.py` | 完成 | array/CQ 字符串解析，保留 text、媒体、reply、文件/语音/视频/转发/未知段标记 |
| `onebot11/events.py` | 完成 | message 事件归一化；自身回传过滤；畸形单帧限长审计后丢弃；notice/request/lifecycle 只做限长统计摘要 |
| `onebot11/ws_server.py` | 完成 | token、loopback 默认、有界接收队列、同 chat 顺序、全局 inflight、失败关闭连接促使上游重放 |
| `onebot11/http_api.py` | 完成 | 查询有限重试；发送/管理/reaction 写永不自动重试；有符号 message_id；超时、429/5xx、非 JSON、超大响应分类；媒体 SSRF/类型/大小限制；文本/图片 segment 出站 |
| `onebot11/queue.py` | 完成 | SQLite WAL、schema 11 迁移（真实 v7/v8/v9/v10 表结构）、消息/TurnAnchor 去重、固定 batch lease、heartbeat、摘要、tombstone、uncertain 人工 resolve、reopen 和管理动作 operation ledger |
| `onebot11/dispatch.py` | 完成 | 每群最多一个活动 turn，恢复触发请求，按群隔离 backoff，暂停/恢复、失败状态转换、恢复轮询自恢复和 reconnect reset |
| `onebot11/triggers.py` | 完成 | @、关键词、always、问句/记忆候选、5 秒 debounce、60 秒活跃窗口和显式旁路 LLM 三态判断 |
| `onebot11/permissions.py` | 完成 | `CallerContext`、`ChatTarget`、精确 `(session_id, turn_id)` binding、task/epoch/lease fencing、authority shrink、user/trusted_user/super_admin 角色、只读边界和 fail-closed |
| `onebot11/tools.py` | 完成 | 当前群/私聊范围查询和群管理写工具；写操作必须确认 |
| `adapter.py` | 完成 | Hermes glue、shared session、入站访问策略、hooks、工具 handler、群 turn 👀 指示器、文本/图片出站生命周期、base64 segment、`get_image` 输入解析、媒体回收、统一配置解析、raw self_id、临时摘要注入和 operation resolve |
| 文档/ADR | 进行中 | 已有 README/权限/Task/ADR 合同；本轮补充 Issue #13 的 recovery 顺序、authority 收紧、旧 task fencing 和 Hermes/Arch 验收边界 |

## 验证证据

- `pytest -q`：`171 passed, 1 skipped`；纯插件环境只跳过没有 Hermes gateway 的 adapter 集成测试。
- `ruff check .`：通过。
- `scripts/verify_hermes_integration.py` + Hermes 独立 `feat/i13-onebot11-contract` worktree：`263 passed`，strict auxiliary 回归 `3 passed`，smoke 通过，`tools=9 hooks=4 strict_auxiliary=True reconnect=True`；覆盖 schema/authority、硬触发 cooldown、图片数量和总量预检、图片 base64 segment、media-only completion 和 unknown no-fallback 合同。
- 独立临时 venv 中 `uv pip install -e ".[dev]"` 和 `import onebot11`：通过；纯插件门禁不承诺可直接 `import adapter`，后者需要 Hermes gateway 依赖。
- Arch 旧 Hermes `91937a6` 的 strict auxiliary 仍不支持 `fallback_policy/max_attempts`；本分支不会在旧 API 上偷偷调用主模型，LLM trigger 会安全禁用并保留 pending。
- 真实 Hermes 临时 `HERMES_HOME` 注册 smoke：已确认平台、9 个工具、4 个安全 hooks 和 `onebot11_trigger` auxiliary 均注册，并验证 shared session/TurnAnchor 合同、严格旁路配置、pending anchor 恢复、home cron、同实例 reconnect 和本地图片 base64 segment；旧 Hermes 组合仍安全禁用 strict LLM trigger。媒体/unknown 组合证据来自独立 Hermes worktree 的本地注入，不代表远端 Hermes PR 已合并。

## 外部联调状态

2026-08-08 在 Arch `ssh arch` 上做过受控联调，严格固定机器人 QQ `3101482118`、唯一允许群 `1072992996` 和唯一允许私聊用户 `2056963663`；原始配置、`.env`、SQLite、session 和 LLBot 配置已备份到 `/home/notnotype/.hermes/onebot11-backup-20260808-turn-anchor-contract/`。联调结束后 Hermes 已恢复到原主分支 `91937a6`，没有把本次 PR 的代码留在生产运行路径。

此前部署的 PR #8/TurnAnchor 版本已确认：

- OneBot `get_login_info` 返回 `retcode=0`、机器人 QQ `3101482118`；当前 WS 在 `0.0.0.0:18880` 监听。
- 使用指定群中已存在的真实消息 ID `2076873675` 做一次受控 WS 重放：该 anchor 被 ack，后续 seq `119–127` 仍为 pending，证明 TurnAnchor batch 边界没有偷吃后续消息。
- LLBot 日志确认对真实消息 `2076873675` 先发送 `emoji_id=128064,set=true`，Hermes 回复到指定群后再发送 `set=false`；回复消息 ID 为 `438359985`。这是真实消息 ID 的受控重放，不等同于真人刚发消息。
- 重启 Hermes gateway 前后指定群均为 `schema=9、pending=9、无 trigger`，WS 恢复监听，pending 保留；这是旧生产版本的历史证据，不代表当前 schema 11 分支已部署。
- 白名单外群 `999999999` 的事件被拒绝，SQLite 中该群为 0 行，指定群队列未变化，没有产生出站。
- 2026-08-09 只读检查确认 Arch 当前 `.env` 白名单仍为群 `1072992996`、用户 `2056963663`、机器人 `3101482118`，但 `config.yaml` 的 `roles.super_admin.tools` 仍残留 `image_generate`、`onebot_get_permissions`、`onebot_set_role_tools`、`onebot_set_trusted_users` 这 4 个本插件不存在的工具；部署 `0.5.0` 前必须清理，当前未修改远端配置。

本次 `0.5.0` 图片/unknown 变更尚未在 Arch 生产部署。通过隔离 queue 和真实 OneBot HTTP/WS adapter smoke 验证了 image-only、文字+图片、多图的 `base64://` segment，以及正负 message id 的 `👀` 添加/移除；这些消息没有经过生产 Agent pipeline，因此真实 QQ Agent 的图片-only、文字+图片、多图、部分成功/unknown 和真人并发仍是待验收项。

Arch 当前生产 live queue 仍为 schema 10，来自 detached Task 5 实现；当前分支已支持真实 v7/v8/v9/v10 表结构迁移并升级到 schema 11，但尚未切换生产 queue。此前没有修改 `PRAGMA user_version`，也没有删除或覆盖生产 queue。

## Issue #13 交付状态

- 当前分支代码尚未部署；以上入站仍是历史版本的受控反向 WS payload，不等同于 QQ 客户端真人发言。两名真人群成员同时 @、当前版本重启恢复和当前版本的 unknown/resolve 仍需外部验收。
- 本分支的非幂等出站 `unknown` 和 `resolve action retry|discard` 尚未在真实 QQ 上制造；联调不执行禁言、踢人、撤回或全员禁言。
- LLBot 当前请求日志会打印 Bearer header；插件不会把 token 发送到外部媒体地址，但部署侧应在生产前关闭该日志或轮换 token。未在本轮擅自修改 LLBot 凭据。
- 未在本轮使用 NapCat，也未把 WS 重连重放行为提升为协议保证。
- Hermes 全局 SIGTERM 关闭日志仍偶发出现 `onebot11 disconnect timed out after 5.0s`；空 adapter 直测没有复现，日志同时包含 Discord/Weixin 的关闭期错误，当前作为 Hermes 多平台 shutdown 观察项，不据此修改 OneBot 插件。
- Hermes 独立 `feat/i13-onebot11-contract` worktree 当前包含 strict auxiliary 与 media/unknown 两个提交（`279409979`、`0d580e74f`），通过定向 `22 passed`；尚未 push、创建 PR 或合并。插件在旧 Hermes API 上会安全禁用 LLM trigger；旧 Hermes 媒体合同不可用时会返回 `unsupported`，不访问 OneBot 图片 API。

## 约束与取舍

- `onebot11/` 保持零 Hermes 依赖；只有根目录 `adapter.py` 依赖 Hermes gateway。
- 消息处理是至少一次语义；OneBot 11 非幂等请求无法提供 exactly-once，因此未知结果不自动重试。
- 不自动迁移旧的群 `per_user` session 历史到新的 shared session；需要人工决定是否清理旧历史。
- 本轮不纳入 OneBot 12、语音转写、群级热更新、复杂管理后台、RAG/向量库、运行时自优化、queued `⏳` reaction 和强制语义摘要模型。
- `trusted_user` 只读；权限、白名单和角色变化不由 Agent 运行时修改。
