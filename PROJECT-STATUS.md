# PROJECT-STATUS

仓库级现状报告。实现变更通过分支和 PR 收口；本文只记录当前代码合同、验证证据和仍需外部联调的事项。

## 当前状态

- **阶段**：OneBot 11 可靠性、安全和分层触发已完成代码闭环；closeout commit `f1b0b8b` 已部署到 Arch 并完成受限 smoke，仍等待两名真人并发、群管理写操作和 PR 审查。
- **核心合同**：群固定一个共享 session；群消息持久入队；触发后按 lease 启动单群单 turn；非幂等出站结果未知时进入 `uncertain`，不自动重放。
- **本地验证**：协议/状态机测试通过；使用本地 Hermes 源码与其 site-packages 运行 adapter 测试通过。最终门禁命令和环境见“验证证据”。

## 模块状态

| 模块 | 状态 | 当前合同 |
|---|---|---|
| `onebot11/message.py` | 完成 | array/CQ 字符串解析，保留 text、媒体、reply、文件/语音/视频/转发/未知段标记 |
| `onebot11/events.py` | 完成 | message 事件归一化；自身回传过滤；notice/request/lifecycle 只做限长统计摘要 |
| `onebot11/ws_server.py` | 完成 | token、loopback 默认、有界接收队列、同 chat 顺序、全局 inflight、失败关闭连接促使上游重放 |
| `onebot11/http_api.py` | 完成 | 查询有限重试；发送/管理/reaction 写永不自动重试；有符号 message_id；超时、429/5xx、非 JSON、超大响应分类；媒体 SSRF/类型/大小限制 |
| `onebot11/queue.py` | 完成 | SQLite WAL、schema 迁移、持久去重、批量 lease、heartbeat、摘要、pending trigger 合并、tombstone、uncertain 人工 resolve、reopen 和管理动作 operation ledger |
| `onebot11/dispatch.py` | 完成 | 每群最多一个活动 turn，恢复触发请求，暂停/恢复、失败状态转换和 reconnect reset |
| `onebot11/triggers.py` | 完成 | @、关键词、always、问句/记忆候选、5 秒 debounce、60 秒活跃窗口和显式旁路 LLM 三态判断 |
| `onebot11/permissions.py` | 完成 | `CallerContext`、`ChatTarget`、精确 `(session_id, turn_id)` binding、角色工具并集、fail-closed |
| `onebot11/tools.py` | 完成 | 当前群/私聊范围查询和群管理写工具；写操作必须确认 |
| `adapter.py` | 完成 | Hermes glue、shared session、入站访问策略、hooks、工具 handler、群 turn 👀 指示器、出站生命周期、媒体回收、统一配置解析、raw self_id、临时摘要注入和 operation resolve |
| 文档/ADR | 完成 | README、权限、状态、任务 walkthrough、strict auxiliary、reconnect、operation ledger、pending trigger 合并和验收边界同步到当前合同 |

## 验证证据

- `.venv\\Scripts\\python.exe -m pytest -q`：`128 passed, 1 skipped`；纯插件环境只跳过需要 Hermes gateway 的 adapter 集成测试。
- 使用 `scripts\\verify_hermes_integration.ps1` 或 `scripts/verify_hermes_integration.py` 和本地 Hermes 源码/strict auxiliary worktree：全套测试 `194 passed`；覆盖 adapter hooks、工具注册、共享队列、身份绑定、shared session key、同实例 reconnect、配置合同、pending trigger 合并、completion recovery、上下文分段、raw self_id、operation resolve、媒体清理、出站 unknown、严格 auxiliary 参数、触发竞争和 reaction 生命周期。
- 严格 auxiliary 回归测试：`3 passed`；确认 `fallback_policy="none"`、`max_attempts=1` 和旧 API 安全降级。
- `.venv\\Scripts\\python.exe -m ruff check .`：通过。
- `pip install -e ".[dev]"`：在本地干净安装路径验证通过；PR #8 的 Linux CI 初次因 GitHub Actions `Service Unavailable` 失败，重新运行后已通过。
- 真实 Hermes 临时 `HERMES_HOME` 注册 smoke：已确认平台、9 个工具、4 个安全 hooks 和 `onebot11_trigger` auxiliary 均注册，并验证 shared session 合同、严格旁路配置和同实例 reconnect；旧 Hermes 组合也验证为安全禁用 strict LLM trigger。

## 外部联调状态

2026-08-07 在 Arch `192.168.1.18` 将 Hermes gateway 切换到插件 commit `f1b0b8b`（版本 `0.3.2`），systemd 用户服务 `hermes-gateway.service` active，LLBot direct compose 的反向 WS 已重新连接。机器人 QQ 为 `3101482118`，唯一允许群为 `1072992996`，唯一允许私聊用户为 `2056963663`；Hermes/LLBot WS 与 HTTP token 已配置一致且未记录在文档中。

已确认：

- OneBot `get_login_info` 返回 `retcode=0`、机器人 QQ `3101482118`；群管理员 `/onebot status` 回执发到指定群，私聊管理员命令回执只发到 `2056963663`。
- 2026-08-07 通过真实反向 WS 连接注入普通群消息，确认 SQLite 为 `pending=1、pending_trigger=0`；重启 `hermes-gateway.service` 后仍为 pending，随后仅向指定群注入白名单管理员 `/onebot clear`，队列恢复为空。
- 指定群的真实 LLBot 消息 ID `1474877096` 被用作 reaction smoke 锚点：LLBot 日志记录 `set=true`，Hermes 回复后记录同一 ID 的 `set=false`；带 `emoji_id=128064` 查询返回 `emojiLikesList=[]`，确认 👀 已清理。该次入站 payload 是合成 WS 事件，不冒充真人发言。
- 本次 closeout 运行期间日志继续拒绝白名单外群 `786830134`、`970204698`；没有向白名单外目标发送消息。
- 在 Arch 临时 Hermes home 中直测同一 adapter 的 `connect → disconnect → reconnect → disconnect`，四个阶段均在 3 秒超时内完成（实际约 1–20ms）；重连后 queue/dispatcher 可以继续工作。

仍需单独完成：

- 以上本轮 Agent 入站事件使用了真实反向 WS 的合成 OneBot payload，不等同于 QQ 客户端真人发言；还未完成两名真人群成员同时触发时的外部观察。
- 指定群已用本轮已删除的 smoke 消息验证群管理写工具预览/确认：同一超级管理员生成预览并确认，operation `7cb1d08e3b2b4e6a` 记录为 `succeeded`；非幂等出站 `unknown` 和 `resolve action retry|discard` 仍未在真实 QQ 上制造。
- 未在本轮使用 NapCat，也未把 WS 重连重放行为提升为协议保证。
- Hermes 全局 SIGTERM 关闭日志仍偶发出现 `onebot11 disconnect timed out after 5.0s`；空 adapter 直测没有复现，日志同时包含 Discord/Weixin 的关闭期错误，当前作为 Hermes 多平台 shutdown 观察项，不据此修改 OneBot 插件。
- Hermes strict auxiliary 修改已在独立干净 worktree 提交并通过 3 个测试，但当前远端 `NousResearch/hermes-agent` 对本账号无写权限，尚未能推送或创建独立 PR。

## 约束与取舍

- `onebot11/` 保持零 Hermes 依赖；只有根目录 `adapter.py` 依赖 Hermes gateway。
- 消息处理是至少一次语义；OneBot 11 非幂等请求无法提供 exactly-once，因此未知结果不自动重试。
- 不自动迁移旧的群 `per_user` session 历史到新的 shared session；需要人工决定是否清理旧历史。
- 本轮不纳入 OneBot 12、语音转写、群级热更新、复杂管理后台、RAG/向量库、运行时自优化和强制语义摘要模型。
