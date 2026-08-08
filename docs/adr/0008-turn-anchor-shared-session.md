# ADR-0008：TurnAnchor 与 shared session 的边界

- 状态：已接受
- 日期：2026-08-08

## 决策

群仍然只使用一个 Hermes shared session，但每次触发创建一个独立 TurnAnchor。

TurnAnchor 绑定：

- 真实 OneBot 消息的 `message_key`、`seq` 和有限消息 ID；
- 当前触发类型：hard、selector、operator 或 recovery；
- 上一个 anchor 之后到当前 anchor 的固定 batch；
- 当前 turn 的 authority、角色、reaction 和 reply 目标。

同群可以有多个 pending anchor，但 `GroupDispatcher` 保证同一时间只有一个活动 lease，并按 anchor 序号串行执行。新消息不会通过动态读取加入已经认领的旧 batch。

## 原因

“每群只有一个 pending trigger”会把多用户同时 @、selector 结果和失败恢复压成一个权限主体，也容易在恢复时发生错误 retarget。TurnAnchor 把身份和批次边界落到持久数据，同时保留群级共享 session。

## 影响

- SQLite 中可能同时存在多个 pending anchor，但不会产生同群并发 Agent turn。
- 最早 anchor 的 backoff、`uncertain` 或 `failed` 会阻塞后续 anchor，保证回复顺序。
- selector 指向的消息消失时直接放弃旧结果，不静默选择另一条消息。
- shared session 的历史仍由 Hermes 管理；本 ADR 不引入 session 合并、RAG 或向量库。
