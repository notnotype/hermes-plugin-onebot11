# OneBot 11 权限与目标范围

本插件把“能进入 Hermes”与“当前 turn 能调用哪些工具”分开处理。OneBot 入站策略在入队前执行，工具权限在 `pre_tool_call` 和 handler 两层执行；提示词只用于让模型了解规则，不能代替硬校验。

## 入站访问策略

| 场景 | 合同 | 配置 |
|---|---|---|
| 群消息 | `allowed_groups` 非空时只接受列出的群；为空时接受群消息并按 trigger 决定是否启动 turn | `ONEBOT11_ALLOWED_GROUPS` |
| 私聊白名单 | 只接受列出的 QQ | `ONEBOT11_DM_POLICY=allowlist` + `ONEBOT11_ALLOWED_USERS` |
| 私聊关闭 | 直接丢弃，不进入队列或 session | `ONEBOT11_DM_POLICY=disabled` |
| 私聊开放 | 只有显式 allow-all 才接受；没有 allow-all 时 fail-closed | `ONEBOT11_DM_POLICY=open` + `ONEBOT11_ALLOW_ALL_USERS=true`，或 `GATEWAY_ALLOW_ALL_USERS=true` |

未知 `dm_policy`、`group_sessions_per_user=true` 或非 `shared` session 配置都会拒绝启动。插件声明 `enforces_own_access_policy=True`，通过 adapter 访问策略的消息会以 `role_authorized=True` 进入 Hermes，避免被网关默认 allowlist 再次误拒绝。

群消息即使没有 @、关键词或 `always` trigger，也会先写入 SQLite 队列；这保证触发时能拿到上次触发以来的上下文。没有 trigger 不会启动 Agent turn。

群 turn 认领后，插件默认使用 LLBot 的 `set_msg_emoji_like` 扩展给触发消息添加 `emoji_id=128064`（👀），Hermes turn 收尾时发送 `set=false` 移除。该指示器只作用于当前群的真实消息 ID；内部 hash、私聊消息或 lease 已失效时跳过。reaction 是 best-effort UI 提示，失败或结果未知不会阻断 Agent 回复、队列 ack，也不会自动重试。

## 角色与工具

超级管理员由 QQ 号列表定义：

```yaml
platforms:
  onebot11:
    extra:
      super_admins: ["10001"]
      roles:
        user:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info]
        super_admin:
          tools: [qq_get_message, qq_get_group_msg_history, qq_get_friend_msg_history,
                  qq_get_group_info, qq_get_group_member_info, qq_delete_message,
                  qq_set_group_ban, qq_set_group_kick, qq_set_group_whole_ban]
```

`ONEBOT11_SUPER_ADMINS` 优先，`ONEBOT11_ADMINS` 仅作为兼容旧名。超级管理员为空时没有任何写权限；普通用户默认只有只读工具。可配置的角色工具集合会取所有角色许可工具的并集注册到 Hermes，再由当前 turn 的角色门禁限制实际执行。

默认只读工具：

- `qq_get_message`：返回的消息必须属于当前群或当前私聊。
- `qq_get_group_msg_history`：只能在当前群查询，群号不从模型参数读取。
- `qq_get_friend_msg_history`：只能在当前私聊查询当前用户。
- `qq_get_group_info`、`qq_get_group_member_info`：只能作用于当前群。

写工具（撤回、禁言、踢人、全员禁言）只能作用于当前群，普通用户永远拒绝。首次调用只产生预览和短期 `/onebot confirm TOKEN`，不会立即执行；确认命令必须由同一超级管理员在同一群发送，令牌单次消费且不写入审计日志。确认命令在 adapter 入站层直接处理，不会进入 Hermes session 或消息队列。

确认令牌仍只保存在当前 adapter 进程内存中，进程重启会让旧令牌失效；但管理动作台账持久化在同一个队列 SQLite 中。进程恢复会把遗留的 `started` 标记为 `unknown`，同一 fingerprint 在 `unknown` 状态下禁止重复调用。`/onebot resolve action retry OPERATION_ID` 只把动作置为 `retry_armed`，随后必须重新生成预览并再次确认；`discard` 只记录放弃，不访问 OneBot。审计只保留 operation id、fingerprint 摘要、工具、目标和结果，不记录 token、完整参数或媒体 URL。

## 身份传递

每次入站 turn 都创建不可变 `CallerContext`。Hermes 的 `session_key` 只用于 session 路由，不能当作身份；工具身份按 `(session_id, turn_id)` 绑定，并在 Hermes registry 没传 `turn_id` 的兼容路径使用当前 task 的 binding，同时校验 `session_id`。找不到精确 binding 时 fail-closed。

出站目标使用明确的 `ChatTarget(group|dm, chat_id)`。当前 turn 只能向它绑定的目标发送；同一个数字同时被识别为群号和 QQ 号时，未带明确类型的发送会被拒绝。

## 队列与不确定结果

群队列是持久 SQLite 状态机；同一群始终只保留一个 pending trigger。旧 lease 在失败、恢复或断开结算时如果遇到后来创建的 pending trigger，会合并旧请求，不因唯一索引冲突而卡住；曾经被认领但仍 pending 的消息保留恢复入口。

```text
pending -> leased(agent_running) -> acked/deleted
leased --明确失败且未开始出站--> pending（2、4、8 秒退避，最多 3 次）
leased --出站已开始、lease 过期或阶段未知--> uncertain
leased --达到失败上限--> failed
uncertain --管理员 retry--> pending
uncertain --管理员 discard--> deleted
failed --管理员 retry--> pending
failed --管理员 discard--> deleted
```

消息入队允许至少一次；OneBot 非幂等 HTTP 请求（发送、撤回、禁言、踢人、全员禁言）不自动重试。连接断开、非 JSON 响应、超时、5xx 或部分分块成功时，结果可能是 `unknown`，插件不会重新执行整轮 Agent，必须由管理员 `/onebot resolve retry|discard` 明确处理。lease 一旦写入出站 marker，任何明确错误也不会自动 release；队列消息的 `retry` 仍可能再次执行动作，因此只应在确认目标端没有执行后使用。管理动作台账的 `retry` 只解除该动作 fingerprint 的阻断，不会直接调用 API。完成 ack/release 只有在 SQLite 原子状态转换成功后才会推进下一轮。

## 运维命令

超级管理员可在目标群发送：

`/onebot status`、`/onebot queue`、`/onebot flush`、`/onebot clear`、`/onebot pause`、`/onebot resume`、`/onebot resolve retry`、`/onebot resolve discard`、`/onebot resolve action retry OPERATION_ID`、`/onebot resolve action discard OPERATION_ID`、`/onebot confirm TOKEN`。

`pause` 只停止自动 dispatch，消息继续入队；`clear` 清理 pending 消息和滚动摘要但不删除 Hermes session 历史，活动 lease、`uncertain` 或 `failed` 必须先显式处理。

## 分层触发和旁路模型

群消息的触发顺序是“硬触发优先，候选消息再仲裁”：

- @、关键词、`always` 和管理员命令直接创建持久 trigger，不调用 LLM。
- 空闲状态只把问句或带有“之前/上次/刚才/继续”等回指词、且当前群已有摘要或最近原文的消息送入候选。
- 候选消息使用 5 秒 trailing debounce；每群最多一个判断任务，冷却期间不创建判断。
- 旁路模型必须显式配置 provider、model 和群 allowlist，并且 Hermes auxiliary API 必须支持 `fallback_policy`、`max_attempts`。插件固定使用 `fallback_policy=none`、`max_attempts=1`；旧 API 会安全跳过，绝不调用主 Agent 作为隐式 fallback。
- 模型只能返回 `{"decision":"trigger|wait|ignore","wait_seconds":0}`。`wait` 的 `wait_seconds` 只能为 `5/10/30/60`，`trigger` 和 `ignore` 必须为 `0`；非法 JSON、超时、模型错误均按 `ignore`，消息留在 pending，不创建 lease。
- 成功 turn 后进入最多 60 秒 idle 活跃窗口，最长连续活跃时间 300 秒，最多 3 次 LLM 仲裁；重启后 active/engaged 状态回到 idle，只恢复 SQLite 消息和显式 durable trigger。

群历史摘要通过 Hermes 支持的 `channel_prompt` 临时注入，当前批次才写入普通 user transcript；摘要被标记为“不可信群消息数据”，其中的指令不能覆盖系统规则。旧 Hermes 没有该字段时退回有界单文本模式，并写入审计。

这些规则只决定“是否启动一轮 Agent”，不改变角色权限。实际工具调用仍必须通过当前 `(session_id, turn_id)` binding、访问策略和 lease fencing。
