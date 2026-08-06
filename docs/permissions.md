# OneBot 11 权限、上下文与目标范围

本插件把“能否进入 Hermes”“当前 batch 怎样进入 session”和“当前 turn 能调用哪些工具”分开处理。OneBot 入站策略在入队前执行，权限在 `pre_tool_call`、插件 handler 两层执行，提示词只用于说明规则，不能代替硬校验。

## 入站访问策略

| 场景 | 合同 | 配置 |
|---|---|---|
| 群消息 | `allowed_groups` 非空时只接受列出的群；为空时接受消息并按 trigger 决定是否启动 turn | `ONEBOT11_ALLOWED_GROUPS` |
| 私聊白名单 | 只接受列出的 QQ | `ONEBOT11_DM_POLICY=allowlist` + `ONEBOT11_ALLOWED_USERS` |
| 私聊关闭 | 直接丢弃，不进入队列或 session | `ONEBOT11_DM_POLICY=disabled` |
| 私聊开放 | 只有显式 allow-all 才接受；没有 allow-all 时 fail-closed | `ONEBOT11_DM_POLICY=open` + `ONEBOT11_ALLOW_ALL_USERS=true` |

未知策略、`group_sessions_per_user=true` 或非 `shared` session 配置都会拒绝启动。插件声明 `enforces_own_access_policy=True`，adapter 访问策略通过后才会以 `role_authorized=True` 进入 Hermes。

## 上下文装配

群消息进入 SQLite 后，触发器认领一个 batch。真实 Agent 输入的逻辑结构是：

1. Hermes session 历史消息：之前成功完成的 user/assistant/tool turn，由 Hermes 自己保存并参与 provider 缓存。
2. 当前 batch 的确定性摘要：只包含本次 lease 中较早的消息，按 UTF-8 字节预算裁剪。
3. 当前 batch 最近消息原文：默认保留最后几条，保留规范化文本、CQ/media/reply 标记和必要 raw segment。
4. 动态 request-only 上下文：时间、当前目标等只允许在宿主提供 `pre_provider_request` 后追加到 provider request copy，不能用 `pre_llm_call` 伪装，否则会写入 Hermes 的 `api_content` sidecar。

第 2、3 项拼成一个 synthetic user turn，因此会进入 session 历史，下一轮不需要从 SQLite 滚动摘要再次注入。`QueueStore.ack()` 只删除已确认消息和触发请求，不再更新跨轮摘要。进程崩溃允许至少一次处理；跨 SQLite 与 Hermes session 没有 exactly-once 事务，极端崩溃窗口可能重复一个 user turn，但正常成功路径不会重复。

## 角色与精确工具权限

角色固定为 `user`、`trusted_user`、`super_admin`，优先级为 `super_admin > trusted_user > user`。配置示例：

```yaml
platforms:
  onebot11:
    extra:
      super_admins: ["10001"]
      roles:
        user:
          tools:
            - qq_get_message
            - qq_get_group_msg_history
            - qq_get_group_info
            - qq_get_group_member_info
        trusted_user:
          users: ["2056963663"]
          tools:
            - web_search
            - web_extract
            - browser_navigate
        super_admin:
          tools:
            - onebot_get_permissions
            - onebot_set_role_tools
            - onebot_set_trusted_users
            - qq_get_message
            - qq_get_group_msg_history
            - qq_get_group_info
            - qq_get_group_member_info
            - qq_delete_message
            - qq_set_group_ban
            - qq_set_group_kick
            - qq_set_group_whole_ban
```

工具名逐个精确匹配，不支持 `*`、`?`、toolset 名或模糊前缀。没有显式配置时，`user` 默认只有当前范围内的只读 OneBot 工具，`trusted_user` 默认为空，`super_admin` 默认只有本插件的 OneBot 工具。网页搜索、网页提取、浏览器、终端、文件/MCP 工具必须显式列入受信角色或超级管理员角色；插件不把这些高风险工具偷偷加入普通用户。

Hermes schema 继续提供所有角色允许工具的并集，以保持共享 session 的 schema 稳定；实际执行时当前 turn 使用不可变权限快照，并在权限收紧后于下一次工具调用重新检查。当前插件的 `pre_tool_call` 会检查所有工具名，包括 `tool_search`、`execute_code`、`delegate_task`；宿主未来还需要把同一集合传入 tool search 和子 Agent 的 tool registry，才能让“不可见”和“不可执行”同时成立。
在 Hermes 上游提供 per-turn 工具策略前，OneBot 角色不能配置或调用 `delegate_task`；它不能把 QQ caller 的权限传递给子 Agent。`tool_search` 没有完整 turn 身份时也会 fail-closed。`execute_code` 若显式授予，代表完整的高风险本机代码执行能力，不应放入普通 `user` 角色。

权限配置工具为：

- `onebot_get_permissions`：读取当前权限快照，仅超级管理员。
- `onebot_set_role_tools`：替换一个角色的精确工具名列表，仅超级管理员。
- `onebot_set_trusted_users`：替换受信 QQ 列表，仅超级管理员。

两个写工具只返回预览和 `/onebot confirm TOKEN`。确认命令在 adapter 入站层执行，要求同一超级管理员、同一群和短期单次令牌；只修改 `platforms.onebot11.extra.roles`，不能修改白名单、token、provider 或全局 toolset。写配置的生效规则是：新增权限下一 turn 生效，权限收紧在下一次工具调用立即阻断。

## 会话与目标

群固定一个 shared Hermes session；私聊按用户独立 session。工具身份按精确 `(session_id, turn_id)` 绑定，`session_key` 只用于 Hermes 路由。出站目标使用显式 `ChatTarget(group|dm, chat_id)`，当前 turn 只能发送到当前目标；同一个数字同时作为群号和 QQ 号时，缺少类型的发送拒绝猜测。

群里的 `/context`、`/status`、`/whoami`、`/help`、`/commands` 在入队前旁路处理，不进入队列或 session。`/new`、`/reset`、`/restart`、`/model`、`/compress` 直接拒绝。`/onebot ...` 管理命令继续只允许超级管理员。

## 写操作与未知结果

撤回、禁言、踢人、全员禁言必须确认，且只能作用于当前群。OneBot 11 非幂等 HTTP 请求无法保证 exactly-once；连接中断、超时、非 JSON、缺少 message ID 或部分分块成功时进入 `unknown`，不会自动重放 Agent turn，必须管理员 `/onebot resolve retry|discard`。

## 当前边界

Docker 子代理不在本 Task 实现，后续任务只记录容器隔离、共享目录、资源/网络限制、凭据隔离和结果大小限制。动态 request-only 上下文和 provider 级精确 tool policy 需要 Hermes 上游公共接口；插件不修改本机 Hermes 安装目录，也不把 `pre_llm_call` 当作 request-only 兼容层。
