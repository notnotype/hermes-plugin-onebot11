# ADR-0012：队列 schema 迁移与 authority 快照 fail-closed

- 状态：已接受
- 日期：2026-08-08

## 决策

OneBot 队列当前 schema 固定为 12，启动迁移按“删除旧索引 → 补列/重建旧表
→ 补齐 reaction 与 cooldown/selector 列 → 创建新索引 → 更新版本并提交”的顺序执行。迁移只接受
已知的 v7、v8、v9、v10 表结构；更高版本或无法证明结构安全时拒绝启动，不通过修改
`PRAGMA user_version` 假装兼容。迁移失败会 rollback、关闭连接并保持 `closed`。

每个可执行 TurnAnchor 必须持久化触发时的角色、工具集合和机器人 `self_id`。
启动恢复时允许当前配置收紧权限，但不能扩大旧 anchor 的权限；缺少、损坏或属于
其他机器人的 authority 一律进入 `uncertain`，需要管理员处理。通用 QueueStore
仍保留无 Hermes 调用方的最小兼容接口，但 OneBot adapter 入口不执行缺 authority
的 durable anchor。

## 原因

SQLite 旧索引可能引用尚不存在的 anchor 列，先建索引会让真实 v7/v8 文件无法启动。
同时，恢复路径若重新按当前用户或当前环境推导角色，旧消息可能在权限变化后被意外
升级；持久 authority 快照和 adapter 入口 fencing 能把这两类风险分别收口。

## 影响

- reaction 清理记录尽量在迁移中保留；远端 reaction 是否仍存在只能通过受限
  `unset` 恢复确认，不承诺迁移时 exactly-once 清理。
- 旧 anchor 缺少 authority 时会停在 `uncertain`，不会为了自动恢复而猜测身份。
- schema 12 迁移支持现有已知版本，但真实生产 queue 仍需在备份后单独联调，不能把本地
  fixture 通过当成生产迁移已经完成。
