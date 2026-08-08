# ADR-0009：trusted_user 只读边界

- 状态：已接受
- 日期：2026-08-08

## 决策

`roles.trusted_user.users` 可以把指定 QQ 号标记为 `trusted_user`，角色优先级为：

```text
super_admin > trusted_user > user
```

`trusted_user` 只能使用明确配置的只读工具。配置解析器拒绝把群管理写工具加入 `user` 或 `trusted_user`；adapter、hooks 和 handler 仍做运行时双重 fail-closed 校验。

trusted_user 不能通过 Agent 或工具修改权限、白名单、角色配置或运行时 Python。权限变化只通过 YAML/环境配置和人工部署生效。

## 原因

trusted_user 适合给可信成员提供有限查询能力，但不应成为绕过超级管理员确认流程的第二条管理入口。把写能力从配置阶段就拒绝，比只依赖提示词更容易审计。

## 影响

- trusted_user 的工具集合仍参与 Hermes 注册表并集，但当前 turn 只能执行只读集合。
- 若用户同时出现在 `super_admins` 和 `trusted_user.users`，按 super_admin 处理。
- 旧配置没有 trusted_user 时行为不变：普通用户默认只读，超级管理员由 `super_admins` 决定。
