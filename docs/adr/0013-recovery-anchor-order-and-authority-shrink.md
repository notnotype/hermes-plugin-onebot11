# ADR-0013：恢复保持 anchor 顺序，authority 只能收紧

- 状态：已接受
- 日期：2026-08-09

## 背景

一个群可以同时拥有多个待处理 TurnAnchor，但 Hermes/OneBot 出站仍必须按真实消息顺序执行。恢复路径如果按全局 `next_attempt_at` 排序并提前停止，会让一个群的退避阻塞其他群；如果找不到旧 trigger 就复用后续 anchor，又会让旧消息跨越 batch 边界。

同时，队列里的 authority 是触发时的快照，而超级管理员、trusted_user 和白名单可能在进程运行期间被收紧。旧 task 还可能在 reconnect 后延迟完成，不能只凭 `(session_id, turn_id)` 清理或重新获得权限。

## 决策

- 恢复按群维护 blocked 集合：某群最早 anchor 的 backoff、`uncertain` 或 `failed` 只阻塞同群后续 anchor，其他群继续恢复。
- 缺失 trigger 时，为该群最早仍阻塞的消息创建独立 recovery anchor；不吸收后续 pending anchor，不引入硬触发抢占。
- 每个 anchor 保留触发时的 role、工具集合和机器人 `self_id`。启动 turn 时按当前实时角色取交集并只允许权限收紧：
  - `super_admin -> trusted_user/user` 只能减少能力；
  - `trusted_user -> user` 只能减少能力；
  - 角色或 `self_id` 快照损坏时进入 `uncertain`。
- `TurnBinding` 绑定完整的 session、turn、task、adapter epoch 和 lease。completion 清理必须匹配完整 snapshot；旧 task 不能删除同键的新 binding，也不能通过 fencing 后的新工具或出站请求。
- 畸形 OneBot message 单帧限长审计后丢弃，不关闭整个 WS；队列、SQLite 或系统级错误仍通过连接失败/重放路径暴露。
- 入站图片只有 `file` 标识时必须调用 OneBot `get_image` 解析；不猜测宿主机或 Docker 路径。

## 原因

这些规则把“顺序”“身份”和“连接生命周期”分别绑定到可验证的持久或短生命周期对象上，避免用最近来源缓存、全局排序或宽松兼容路径解决问题。复杂度主要集中在已有 QueueStore、binding store 和 adapter hook，不新增数据库或后台服务。

## 影响

- 硬触发不会抢占更早的 blocked anchor；管理员需要先 resolve，或等待前一个 anchor 完成。
- 权限收紧立即影响恢复中的旧 anchor，但权限放宽不会升级旧快照，必须由新的入站消息触发。
- OneBot 输入保持至少一次语义；持久化前的 WS 事件不承诺可靠不丢。
- 真实 QQ/LLBot 验收必须分别验证多群恢复、reconnect fencing、authority revoke 和 `file` 图片解析。
