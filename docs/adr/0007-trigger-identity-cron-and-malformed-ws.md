# ADR-0007：触发身份、Home Channel 与 malformed WS 边界

- 状态：已接受
- 日期：2026-08-07

## 决策

### 混合批次的权限主体

一个群的 durable trigger 只有一个权限主体：创建或被硬触发覆盖时记录的用户。
同一批里其他用户的消息只是非可信上下文，不会改变角色、允许工具或出站目标。
这样保持“群一个 shared session”不变，同时避免把普通用户消息混入后意外升级管理权限。

### Home Channel cron

第一阶段的 cron 只保证显式 `home_channel`。如果配置目标 ID，必须同时配置
`home_channel_type=group|dm`；群目标必须经过群白名单，私聊目标必须经过 DM policy。
插件不根据 QQ 号长度或数字形状猜测目标类型。发送成功响应缺少可靠 `message_id` 时返回
`unknown`，不把可能已经执行的发送伪装成成功。

### malformed WS 输入

无法归一化为合法 OneBot message 的 payload 只记录限长诊断并丢弃，不因为单条坏消息关闭整条
WS；只有进入有界接收队列后发生的持久化异常、队列背压或处理异常才允许触发上游重连/重放。
这样避免恶意或偶发 malformed payload 造成无限重连风暴，同时保留队列边界内的恢复机会。

## 原因

这三个合同分别约束权限升级、cron 误投递和 WS 连接稳定性；它们都能在当前 adapter/SQLite/
WS 结构内实现，不需要第二套身份数据库、delivery metadata 或原始 WS spool。

## 影响

- 混合消息批次不会按用户拆分，因此后续普通消息不能获得更高权限；
- cron 配置更严格，但避免把群号当 QQ 号或反之；
- malformed payload 的丢弃不提供可靠恢复承诺，真实框架重连行为仍按 OneBot 11 实际观察记录。
