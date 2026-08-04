# 权限模型（群聊为主场景）

OneBot 11 主要跑在群聊里,权限按三层设计。核心原则：**群聊里能 @ 机器人聊天的人很多,但能用工具、能查数据的人必须被约束住**。

## 三层结构

### 1. 网关用户授权层（谁能让机器人响应）

| 场景 | 默认 | 配置 |
|---|---|---|
| 群聊 | 群里任何人都能 @ 机器人 | `ONEBOT11_ALLOW_ALL_USERS=true`（配合 require_mention） |
| 群聊触发 | 必须 @ 机器人才响应 | `ONEBOT11_REQUIRE_MENTION=true`（默认开;`false` = 群里每条消息都响应） |
| 群白名单 | 所有群可用 | `ONEBOT11_ALLOWED_GROUPS=群号,群号`（空 = 不限制） |
| 私聊 | 全部放开 | `ONEBOT11_DM_POLICY=open` |
| 私聊白名单 | — | `ONEBOT11_DM_POLICY=allowlist` + `ONEBOT11_ALLOWED_USERS=QQ,QQ` |
| 私聊关闭 | — | `ONEBOT11_DM_POLICY=disabled` |

### 2. 工具权限层（谁能调用平台工具）

- `ONEBOT11_ADMINS`：管理员 QQ 列表（逗号分隔）。
- 工具分两类：
  - **普通工具**：所有已授权用户可用。
  - **admin 工具**：仅管理员可用（如查私聊历史——涉及隐私,默认 admin-only）。
- `ONEBOT11_ADMINS` 为空时：所有已授权用户同权（普通工具全部可用）。

### 3. 会话范围校验（安全底线,强制生效）

工具只能作用于**发起会话自身**：

- 群里调用 `qq_get_group_msg_history` → 只能查**本群**,传别的群号直接拒绝。
- 群里调用 `qq_get_friend_msg_history` → 拒绝（群会话无权查私聊）+ admin 门禁。
- 私聊调用查询工具 → 只能查**自己**的消息。

这是群聊场景的安全底线：陌生人进群能聊天,但查不到群外数据。

## 群角色感知（v1.1 预留,不在 v1 实现）

v1.1 计划通过 `get_group_member_info` 获取发起者在目标群的 role（owner/admin/member）,对**破坏性写工具**（踢人、禁言等,v1 不做）按群角色降级。v1 用 `ONEBOT11_ADMINS` 列表即可满足,`permissions.py` 已留接口。

## 决策记录

- 查询类工具为什么默认不是 admin-only？—— 群聊主场景下,群内查本群消息是常用诉求;隐私边界由「会话范围校验」兜住（查不到群外）。
- 为什么私聊历史默认 admin-only？—— 私聊历史跨会话、涉及他人隐私面,默认收紧,需要时由管理员放开。
