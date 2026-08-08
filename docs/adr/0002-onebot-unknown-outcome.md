# ADR-0002：OneBot 非幂等出站的 unknown 结果

- 状态：已接受
- 日期：2026-08-05

## 决策

发送、图片发送、撤回、禁言、踢人和全员禁言等非幂等 OneBot HTTP 请求永不自动重试。连接断开、超时、非 JSON、5xx、缺少可靠响应或分块部分成功时，把当前群 lease 标记为 `uncertain`，暂停自动重放，要求超级管理员明确 `/onebot resolve retry|discard`。Hermes 通用 delivery 层遇到 `error_kind="unknown"` 时也不重试、不发 plain-text fallback、不从 live cron 改走 standalone。

## 原因

请求返回前网络断开时，客户端无法区分“请求尚未到达”“OneBot 已执行但响应丢失”和“框架正在执行”。再次执行整轮 Agent 可能重复回复或重复管理动作。OneBot 11 没有可供本插件依赖的全局幂等键，因此不能承诺 exactly-once。

## 影响

输入消息仍是至少一次语义；只有在非幂等出站尚未开始时，明确失败才可以按退避 release。出站 marker 一旦写入，即使随后收到明确业务错误，也不自动重放整轮。管理员选择 retry 前应先确认目标端没有执行，discard 则写入去重 tombstone，避免上游重放重新入队。
