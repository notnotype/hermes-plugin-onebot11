# ADR-0001：群固定 shared session

- 状态：已接受
- 日期：2026-08-05

## 决策

OneBot 11 群聊固定使用一个 shared Hermes session，插件启动时强制写入 `group_sessions_per_user=false`。发现旧的 `per_user` 配置时拒绝启动，不自动合并或迁移旧 session 历史。

## 原因

产品需求是“一个群一个 session”，且消息要在触发前持续积累。自动合并旧 per-user transcript 会改变历史身份和权限语义，无法可靠判断合并顺序，也可能把不同用户的旧上下文错误地带入当前群。

## 影响

升级后新消息进入群级 session；旧 per-user 文件仍由 Hermes 管理，但不会被插件自动搬运。需要迁移时由运营者备份后人工决定清理或保留。
