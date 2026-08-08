# ADR-0002：OneBot 非幂等出站的 unknown 结果

- 状态：已接受
- 日期：2026-08-05

## 决策

发送、撤回、禁言、踢人和全员禁言等非幂等 OneBot HTTP 请求永不自动重试。连接断开、超时、非 JSON、5xx、缺少可靠响应或分块部分成功时，把当前 TurnAnchor 标记为 `uncertain`，暂停自动重放，要求超级管理员明确 `/onebot resolve retry|discard`。写工具由当前 anchor authority 直接硬校验；同一 turn 的同一 unknown 动作禁止重复调用。

`resolve retry` 不复用旧 request id，也不让后续用户接管旧消息。对能证明 authority 的 `message`/`operator` anchor，管理员操作会创建新的 retry anchor，保留原 authority、原消息范围和已存在的 reaction 清理状态，再显式 dispatch；这表示“管理员重新确认了一次”，不表示插件判断了旧请求安全。无法证明 authority 的 `legacy` hold 不能 retry，只能 discard 或等待新的明确触发。

## 原因

请求返回前网络断开时，客户端无法区分“请求尚未到达”“OneBot 已执行但响应丢失”和“框架正在执行”。再次执行整轮 Agent 可能重复回复或重复管理动作。OneBot 11 没有可供本插件依赖的全局幂等键，因此不能承诺 exactly-once。

## 影响

输入消息仍是至少一次语义；只有在非幂等出站尚未开始时，明确失败才可以按退避 release。出站 marker 一旦写入，即使随后收到明确业务错误，也不自动重放整轮。管理员选择 retry 前应先确认目标端没有执行；retry 可能重复执行，discard 则写入去重 tombstone，避免上游重放重新入队。插件只维护当前 turn 的 unknown 指纹，不建立永久动作 ledger。

如果未来 Hermes 为系统错误通知提供 `hermes_system_error_notice=true` metadata，插件不会把这类错误提示计入业务 outbound marker；在该上游接口可用前，插件保持保守行为，可能把失败通知误记为 `uncertain`，但不会把未知业务出站误判为安全成功。
