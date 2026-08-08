# OneBot 11 authority、权限与上下文

本插件把入站授权、任务锚点、模型提示和工具硬门禁分开处理。提示词只解释 authority；执行权限只认当前 turn 的 binding、lease、目标和不可变工具快照。

## 入站访问策略

| 场景 | 合同 |
|---|---|
| 群消息 | `allowed_groups` 非空时只接受列出的群；授权后先入 SQLite |
| 私聊 allowlist | 只接受 `allowed_users` 中的 QQ |
| 私聊 disabled | 不进入队列或 session |
| 私聊 open | 需要显式 `ONEBOT11_ALLOW_ALL_USERS` 或 `GATEWAY_ALLOW_ALL_USERS` |

未知策略、`group_sessions_per_user=true` 或非 shared session 配置都 fail-closed。同一策略函数用于实时入站、恢复、reaction cleanup 和 cron。

## TurnAnchor 与 authority

群仍只有一个 shared Hermes session，但每个任务独立成 turn：

```text
锚点消息 -> TurnAnchor -> authority 快照 -> Hermes followup
```

- @、mention、关键词和显式自定义规则是精确触发，消息入队时原子创建 anchor。
- 自动 selector 只看未锚定消息的有限投影，只能返回一个现存 `anchor_seq` 或 null；它看不到角色/工具配置。
- message anchor 完全继承锚点发送者；`/onebot flush` 创建 operator anchor，继承命令管理员。
- 同群多个 anchor 串行进入同一个 session，不合并权限，不使用 steer。
- 首次 claim、Agent 启动前把角色与精确工具名持久化到 QueueStore schema v10；同一 anchor 不能换绑。配置变化从下一 turn 生效；白名单、目标、lease 和关闭状态仍立即 fencing。v9 pending anchor 可以首次 claim 时绑定，旧 active/uncertain/failed 且无快照的记录进入 hold。

## Agent 输入与缓存

一次 provider 输入由以下部分组成：

1. Hermes shared session 历史：之前成功完成的 user/assistant/tool turn，构成稳定缓存前缀。
2. 当前 TurnAnchor batch：只包含上一个锚点边界之后至当前 `anchor_seq` 的消息。
3. authority reminder：由 `pre_llm_call` 动态生成，追加到当前 user request，不修改 system prompt。
4. 其他动态信息：时间等易变字段只适合 request-only 注入，不应写入稳定 system prompt。

当前 batch 用有界 JSONL 表示，每条包含 `seq`、真实 `message_id`、稳定 `message_key`、`user_id`、`user_name`、turn-start `role`、`reply_to`、segment/media markers、正文和 anchor 标记。没有真实 OneBot message ID 时 `message_id` 保持为空；`message_key` 只用于去重/上下文定位，不能传给 `get_msg`。这样群管理工具能拿到可用的真实消息 ID，同时 Agent 能区分上下文发送者与本轮 authority。

Hermes 将当前 user turn 和 wire sidecar 保存到 session；SQLite ack 后删除已消费消息，不重复维护跨轮摘要。极端崩溃窗口仍是至少一次语义，不承诺跨 SQLite/Hermes exactly-once。

## 角色与工具

角色优先级：`super_admin > trusted_user > user`。工具按精确名称配置，不支持 wildcard 或 toolset 名。

```yaml
platforms:
  onebot11:
    extra:
      super_admins: ["10001"]
      roles:
        user:
          tools: [qq_get_message, qq_get_group_msg_history,
                  qq_get_group_info, qq_get_group_member_info]
        trusted_user:
          users: ["2056963663"]
          tools: [web_search, web_extract, browser_navigate]
        super_admin:
          tools: [onebot_get_permissions, onebot_set_role_tools,
                  onebot_set_trusted_users, qq_get_message,
                  qq_get_group_msg_history, qq_get_group_info,
                  qq_get_group_member_info, qq_delete_message,
                  qq_set_group_ban, qq_set_group_kick,
                  qq_set_group_whole_ban]
```

`user` 默认只有作用域受限的 OneBot 只读工具；`trusted_user` 默认无工具；`super_admin` 默认只有本插件工具。网页、浏览器、终端、文件、MCP 和 `execute_code` 必须显式授予；`execute_code` 等价于高风险本机代码执行。

共享 session 的 schema 是所有角色工具并集，`pre_tool_call` 与 handler 按当前 authority 硬拦截。`delegate_task`、`tool_search`、`tool_describe`、`tool_call` 在 OneBot 角色中禁止配置和调用；Hermes tool-search bridge 的目录可见不等于工具获得授权，缺少精确 turn 身份的实际调用必须拒绝。Docker 子代理另行设计。

模型会看到一个有界 role catalog，列出三个角色当前配置的工具集合。catalog 只是解释性提示；当前 turn authority 只来自锚点，其他消息的 role 不能授予权限。`tool_search` 不是权限绕过入口，但在 Hermes 上游完整传递 `turn_id` 前，缺少精确 turn 身份的工具搜索请求必须拒绝。

## 权限配置工具

- `onebot_get_permissions`：读取角色配置。
- `onebot_set_role_tools`：替换角色精确工具列表。
- `onebot_set_trusted_users`：替换受信 QQ 列表。

配置写工具仅 super admin 可用，只修改 `platforms.onebot11.extra.roles`。写入前和 YAML 原子写入前都会再次检查 adapter、当前目标白名单、authority 和 lease；这不是跨 SQLite/YAML 的事务。写入成功后更新 adapter 的下一-turn 配置；当前 turn 继续使用创建时快照。

## 写操作与 unknown

撤回、禁言、踢人、全员禁言在角色、目标和 lease 校验通过后直接执行，不再生成 confirmation token。真实 HTTP 写请求前原子写入 outbound marker。

OneBot 11 非幂等请求无法保证 exactly-once。超时、断线、非 JSON、5xx、缺少合法数字 message ID 或部分成功会使 anchor 进入 `uncertain`，阻塞后续 anchor；同一 turn 的相同 unknown 动作禁止重复调用。管理员核对 QQ 端状态后使用 `/onebot resolve retry|discard`。`resolve retry` 为可验证的 `message`/`operator` anchor 创建新的 request id，保留原 authority、消息范围和 reaction 状态；legacy 或缺少 authority 快照的记录不能 retry。`resolve discard` 不删除 reaction cleanup record，直到 unset 成功、目标失权或管理员明确清理。

## 目标与命令

工具和出站目标绑定精确 `(session_id, turn_id)` 与 `ChatTarget(group|dm, id)`。群号和 QQ 号相同时不猜目标。私聊历史和单条消息响应逐条要求参与者集合正好等于“当前用户 + 当前机器人”；群成员响应还必须匹配当前群和请求的成员 QQ。

群 `/context`、`/status`、`/whoami`、`/help`、`/commands` 在入队前旁路；`/new`、`/reset`、`/restart`、`/model`、`/compress` 拒绝。`/onebot status|queue|flush|clear|pause|resume|reaction clear <message_id>|resolve` 仅 super admin 可用。`reaction clear` 只删除本地 cleanup 责任，不调用 OneBot；活动 turn 期间拒绝。
