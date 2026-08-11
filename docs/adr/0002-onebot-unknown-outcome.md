# ADR-0002：OneBot 非幂等出站的 unknown 结果

- 状态：已接受
- 日期：2026-08-05

## 决策

发送、图片发送、撤回、禁言、踢人和全员禁言等非幂等 OneBot HTTP 请求永不由本插件自动重试。连接断开、超时、非 JSON、5xx、缺少可靠响应或分块部分成功时，把当前群 lease 标记为 `uncertain`，暂停自动重放，要求超级管理员明确 `/onebot resolve retry|discard`。Hermes 全局 delivery 的 fallback 行为不属于本插件合同，本插件不依赖它来保证图片结果。

## 原因

请求返回前网络断开时，客户端无法区分“请求尚未到达”“OneBot 已执行但响应丢失”和“框架正在执行”。再次执行整轮 Agent 可能重复回复或重复管理动作。OneBot 11 没有可供本插件依赖的全局幂等键，因此不能承诺 exactly-once。

## 人工 retry 的 anchor 合同

队列消息的 `/onebot resolve retry` 不是把旧 request_id 原样复活，而是在同一事务中创建新的
request_id，并保留原 anchor 的真实消息范围、发送者和 authority 快照。这样后续明确触发的
新 anchor 不会继承旧 turn 的状态，也不会让管理员身份接管原消息权限。旧 trigger 缺失、
`legacy` anchor、authority 快照损坏或 batch 边界无法证明时，retry 保持 hold，只能 discard
或等待新的明确消息。该人工操作仍可能重复执行原业务动作，不能把它理解为 exactly-once。

管理动作台账的 `resolve action retry` 仍只解除 fingerprint 阻断；它不会直接访问 OneBot，
必须重新生成预览并确认。

## 影响

输入消息仍是至少一次语义；只有在非幂等出站尚未开始时，明确失败才可以按退避 release。出站 marker 一旦写入，即使随后收到明确业务错误，也不自动重放整轮。管理员选择 retry 前应先确认目标端没有执行，discard 则写入去重 tombstone，避免上游重放重新入队。
