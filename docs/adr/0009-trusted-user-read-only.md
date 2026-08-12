# ADR-0009：trusted_user 的 OneBot 写边界与 generic 工具显式授权

- 状态：已接受
- 日期：2026-08-08

## 决策

`roles.trusted_user.users` 可以把指定 QQ 号标记为 `trusted_user`，角色优先级为：

```text
super_admin > trusted_user > user
```

`trusted_user` 可以使用明确配置的工具。OneBot 群管理写工具仍只允许
`super_admin`，配置解析器拒绝把这些写工具加入 `user` 或 `trusted_user`；
Hermes generic 工具（例如网页、浏览器、终端或文件能力）可以按工具名显式授予
`trusted_user`，并由 adapter、hooks 和 handler 运行时双重 fail-closed 校验。
`tool_search` 永久禁止，避免动态发现工具绕过本轮 authority 快照；`delegate_task`
不再全局禁止。`super_admin` 默认拥有 Hermes 通用工具，因此可以直接委派；
`user`/`trusted_user` 必须显式把它加入角色工具集合。启用 `main_agent_read_only`
时主 agent 只能直接使用只读工具，项目执行能力交给 Hermes delegated child。子代理
继承父 turn 的 binding/lease/epoch 和访问范围，但不能调用 QQ 工具、send_message、
cronjob 或再次委派；缺少 delegated-child context 或父 binding 时按 fail-closed 处理。

trusted_user 不能通过 Agent 或工具修改权限、白名单、角色配置或运行时 Python。权限变化只通过 YAML/环境配置和人工部署生效。

## 原因

trusted_user 适合给可信成员提供明确、可审计的 generic 能力，但不应成为绕过
超级管理员确认流程的 OneBot 群管理入口。把 OneBot 写能力从配置阶段就拒绝，
同时允许 generic 工具按名授权，比给 trusted_user 一个模糊的“全能”角色更容易审计。

## 影响

- trusted_user 的工具集合仍参与 Hermes 注册表并集；generic 工具同样必须经过
  OneBot `pre_tool_call` 的精确 binding/lease 门禁。
- 若用户同时出现在 `super_admins` 和 `trusted_user.users`，按 super_admin 处理。
- 旧配置没有 trusted_user 时行为不变：普通用户默认只读，超级管理员由 `super_admins` 决定。
