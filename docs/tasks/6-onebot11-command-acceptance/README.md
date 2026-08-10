# Task 6：OneBot 11 Arch 验收

- 关联 Issue：[#16](https://github.com/notnotype/hermes-plugin-onebot11/issues/16)
- 前置修复：[#13](https://github.com/notnotype/hermes-plugin-onebot11/issues/13)
- 状态：验收未完成；当前分支先修复出站 Binding，PR 合并后再部署和发送真实验收消息。
- 允许范围：群 `1072992996`、私聊用户 `2056963663`、机器人 `3101482118`。

## 验收目标

只在指定群和指定私聊用户验证：

1. @/mention、关键词、问句 selector 和活跃窗口触发；
2. 不带 @ 的中文问句经过插件自有 pi-ai selector；
3. 成功回复后 60 秒内的普通 follow-up 可以连续对话；
4. `/new`、`/reset`、`/clear` 不进入普通消息队列，只重置当前群 shared session；
5. 普通用户只能使用只读工具，超级管理员的写工具停在预览和确认提示；
6. worker thread 建立 binding 后，async final delivery 的文本/图片不会因为 ContextVar 缺失而被错误标记为 `fenced`；
7. 两名群成员并发 @ 时仍只有一个 shared session 和一个活动 lease。

## 当前证据边界

- PR #17 已合并，Arch 当前基线是 `master`、commit `fd246b7`、插件 `0.6.0`、queue schema `11`，服务 active。
- 本分支的 Binding 修复尚未合并或部署；本地纯插件门禁、Hermes 组合 smoke 和 fake OneBot 出站测试不能替代真实 QQ Agent pipeline。
- 关键词当前只保留代码能力，Arch 配置未启用关键词时不能报告为真实关键词验收通过。
- 不执行真实禁言、踢人、撤回或全员禁言；unknown/resolve 使用本地 mock 或无副作用路径。

## 部署前保护

1. 通过 `ssh arch` 只读记录插件 commit、Hermes 服务、queue schema 和队列统计。
2. 由超级管理员在指定群暂停自动 dispatch。
3. 停止服务并备份 config、`.env`、session、SQLite 主文件/WAL/SHM。
4. 确认 checkout 无未提交修改，随后在插件目录 `git pull --ff-only` 和 `npm ci --omit=dev`。
5. 用 Hermes 实际 Python 环境执行 `validate_config()` dry-run，确认白名单、`self_id`、角色工具和 pi-ai 环境变量。
6. 只有确认旧消息是验收残留时，才由管理员 `/new` 清理指定群；不直接编辑 SQLite。

## 验收矩阵

| 场景 | 预期 |
|---|---|
| `/new`、`/reset`、`/clear` | 不入普通队列，只影响当前群 |
| @ 机器人 | 直接 hard trigger，`👀` 添加后在 turn 收尾移除 |
| 不带 @ 的中文问句 | 记录 pi-ai selector，按 `trigger/wait/ignore` 处理 |
| 成功回复后普通 follow-up | 5 秒 trailing debounce 后继续同一 shared session |
| 活跃窗口过期后的普通闲聊 | 不直接触发，消息保留 pending |
| 普通用户调用写工具 | 权限错误，不产生 OneBot 管理写请求 |
| 超级管理员调用写工具 | 只生成预览和确认提示，不执行真实动作 |
| 两名成员同时 @ | 一个 shared session、一个活动 lease、anchor 严格串行 |
| 重启 | pending/durable trigger 保留，active/debounce/judging 回到 idle |
| 白名单外目标 | 入站拒绝，无出站 |

## 已执行的本地验证

- 纯插件：`166 passed, 1 skipped`，Ruff 通过，editable build、`import onebot11`、`npm ci --omit=dev` 和 Node helper 语法检查通过。
- Hermes 组合：`scripts/verify_hermes_integration.py` 输出 `255 passed`，并通过 9 工具、4 hooks、shared session、reconnect、queue recovery、worker-thread binding、图片 segment 和 pi-ai helper smoke。
- 以上证据不代表 Arch 真人验收完成；PR 合并和真实 LLBot/QQ 验收仍是后续步骤。

## 不纳入本任务

RAG/向量库、Hermes runtime 自优化、Docker Hermes、OneBot 12、exactly-once 非幂等出站，以及真实群管理写操作。
