# Task 6：OneBot 11 群级命令与 Arch 验收收口

- 关联 Issue：[Issue #16：OneBot 11 Arch 验收收口](https://github.com/notnotype/hermes-plugin-onebot11/issues/16)
- 分支：`feat/i16-onebot11-command-acceptance`
- 状态：本地实现和 Hermes 组合 smoke 已完成；尚未合并 PR，也尚未部署 Arch

## 目标

让当前插件具备可直接验收的用户闭环：

1. @、关键词、问句 selector 和活跃窗口触发路径各自有清晰边界；
2. 成功回复后的 60 秒内可以不重复 @ 继续对话；
3. `/new`、`/reset`、`/clear` 可以在群内重置当前 shared session；
4. 普通用户和超级管理员的工具集合保持可配置、可审计、fail-closed；
5. 本机分支经过 PR 合并后，Arch 只需 `git pull --ff-only` 和安装 Node 依赖即可部署。

## 已实现

- 群消息仍先经过访问策略，再进入持久 SQLite 队列；命令在入队前识别。
- `/new [title]`、`/reset`、`/clear` 仅允许超级管理员，未知命令和 `/onebot ...`
  不会被误识别为会话命令。
- `/new` 和 `/reset` 使用 Hermes 公共命令入口；`/clear` 翻译为公共 `/new`。
  OneBot 不调用 Hermes 私有 reset 实现，而是通过 `on_session_reset` hook 在 Hermes
  reset 成功后清理当前群队列、摘要和内存触发状态。
- reset 使用消息序号边界：命令开始后抵达的新消息不会被旧 reset 删除；reset
  期间的普通群消息返回明确提示并不进入旧队列。
- reset 会先停止 selector、debounce 和 engaged timer，并 fence 当前群活动 lease。
  未开始出站的旧 turn 可以回收；已开始或结果未知的操作保留 unknown，不自动重放。
- reset 如果无法安全收口，不会推进 generation；文本已部分发送、图片 lease 失效和
  命令回执分别保持 unknown-safe 或普通命令直发，不混入 Agent turn 门禁。
- 旧 adapter epoch、旧 reset generation 的 late completion 可以完成资源收口，但不能
  回写新连接的 engaged/debounce 状态或创建 recovery trigger。
- hard trigger（@、关键词、always、管理员 flush）不消耗 selector；问句、记忆候选和
  engaged 内普通 follow-up 才进入插件自有 pi-ai selector。selector 失败按 ignore，
  pending 消息保留。

## 权限与 Arch 验收配置

本轮验收只允许：

- 群：`1072992996`
- 私聊用户：`2056963663`
- 机器人：`3101482118`

建议的角色合同：

- `user`：`qq_get_message`、`qq_get_group_msg_history`、`qq_get_friend_msg_history`、
  `qq_get_group_info`、`qq_get_group_member_info`
- `trusted_user`：不配置实际用户；如配置只能使用显式只读工具
- `super_admin`：9 个有效 OneBot 工具，写工具仍需预览和同群同管理员确认

角色和白名单只写入 Hermes `config.yaml`/环境变量，不硬编码进插件。Arch 不执行
真实禁言、踢人、撤回或全员禁言；写操作只验收预览和确认提示。

## 验证证据

- 插件纯环境：`174 passed, 1 skipped`（只跳过没有 Hermes gateway 的 adapter 集成文件）
- `ruff check .`：通过
- Hermes `v0.20.0` 组合：`269 passed`
- 集成 smoke：`tools=9 hooks=5 pi_ai_trigger=True reconnect=True slash_commands=True`
- 真实 Arch 验收仍需在 PR 合并后进行；本地通过不等同于真人 QQ 双用户并发已验收。

## 计划出入与边界

- 不修改 Hermes 源码，不创建 strict auxiliary 或媒体结果合同 PR。
- LLM trigger 由插件自有 Node/pi-ai helper 调用；Hermes auxiliary 不参与。
- Arch 默认只启用问句 selector；关键词保留代码和本地测试能力，是否启用由部署配置决定。
- 不纳入 RAG、向量库、运行时自优化、OneBot 12、原始 WS spool 或 exactly-once
  非幂等出站。
