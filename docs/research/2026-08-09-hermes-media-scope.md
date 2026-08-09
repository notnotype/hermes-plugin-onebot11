# Hermes 媒体链路与严格旁路范围收口

> 调研日期：2026-08-09。本文记录本地 Hermes worktree、OneBot adapter 和其他 provider 的只读审查结果。

## 结论

本插件不需要等待 Hermes 全局媒体重构才能使用。OneBot adapter
已经能够把 Agent 最终回复中的图片转换为受限的 `base64://` image
segment；本次调研发现的 Hermes 缺口主要影响较窄的
`send_message`/cron plugin media 路径，以及媒体失败结果的统一统计。

这些问题记录为已知限制，不在本轮新增 Hermes PR，也不把微信已有的
兼容行为升级为全平台可靠性改造。主需求的优先级仍是群共享 session、
持久队列、触发、权限和 Agent 最终回复。

## 已验证的调用链

### Agent 最终回复中的图片

```text
Hermes 提取图片
→ BasePlatformAdapter.send_multiple_images()
→ OneBot11Adapter.send_image_file()
→ 每张图片的 SendResult
→ Hermes processing completion
```

本地 Hermes 媒体 worktree 已验证 image-only、文字+图片、多图、partial
和 unknown 的结果聚合。插件在旧 Hermes 缺少该能力时 fail-closed，继续
支持文本，不把 URL 或路径伪装成普通文本。

### 通用 send_message 图片

通用 `send_message` 的 live plugin 路径当前只调用
`adapter.send(content=...)`，没有把 `media_files` 交给图片方法。
standalone 路径虽然接收 `media_files`，但 OneBot 的独立投递函数当前只
保证文本，未承诺 cron/CLI 图片。

这个限制不影响 Agent 最终回复的主链路，后续如有真实需求再单独收口。

### unknown

`unknown` 只表示“请求可能已经到达 OneBot，但可靠响应没有回来”。
OneBot 的非幂等发送不能安全自动重试；插件当前把它交给队列
`uncertain`/operation ledger。Hermes 的其他 delivery 路径是否消费该
结果，属于通用框架兼容性边界，不由 OneBot adapter 猜测。

## 上游问题对应

以下 Hermes issue/PR 与本地调研直接相关：

- [#19002](https://github.com/NousResearch/hermes-agent/issues/19002)：
  希望能够统一关闭 provider fallback。
- [#40565](https://github.com/NousResearch/hermes-agent/issues/40565) /
  [#40664](https://github.com/NousResearch/hermes-agent/pull/40664)：
  只讨论禁止回退主模型，范围小于 OneBot 所需的“无 provider fallback、
  单次请求”合同。
- [#18422](https://github.com/NousResearch/hermes-agent/issues/18422) /
  [#18686](https://github.com/NousResearch/hermes-agent/pull/18686)：
  直接讨论 plugin platform 的 `send_message` 媒体路由缺口。
- [#36817](https://github.com/NousResearch/hermes-agent/pull/36817)：
  提出用媒体能力声明避免基类 stub 被误调用。这个方向值得参考，
  但完整覆盖所有 provider 超出 OneBot 本轮范围。
- [#37315](https://github.com/NousResearch/hermes-agent/issues/37315)、
  [#78932](https://github.com/NousResearch/hermes-agent/issues/78932)、
  [#55806](https://github.com/NousResearch/hermes-agent/issues/55806)：
  分别反映 plugin/QQ 媒体被丢弃、媒体拒绝后状态误报和 delivery
  receipt 不一致等相邻问题。

## 其他 provider 的参考与取舍

Telegram、Signal、BlueBubbles、WhatsApp、Weixin 和 Yuanbao 都存在某种
图片发送路径，但批量结果、失败聚合和 fallback 行为并不统一：

- Telegram/Signal 更偏向失败后继续尝试其它图片或分批发送；
- BlueBubbles/WhatsApp 更偏向 adapter 自己下载并上传；
- Weixin 能完成 native 图片发送，但媒体失败不一定形成严格的整轮失败；
- Yuanbao 更接近先传 bytes，再传平台引用。

可复用的共同点是“平台 adapter 负责上传”。不应照搬的是失败后继续
发送整批或把图片 URL 当普通文本的行为，因为它们与 OneBot 的
`uncertain` 语义不一致。

## 本轮折中

- 不创建新的 Hermes strict auxiliary PR。
- 插件不把 strict auxiliary 当作生产启动依赖；旧 Hermes 不支持严格
  参数时，LLM trigger 直接跳过，消息保留在 pending，不回退主 Agent。
- 如果未来确实需要在旧 Hermes 上启用旁路判断，优先考虑由插件直接调用
  明确配置的 provider，而不是修改 Hermes 主 Agent fallback 链；这属于
  后续独立任务，本次只记录边界，不实现直连 provider。
- 出站图片以 Agent 最终回复为主要支持场景；`send_message`/cron 图片
  保持已知的窄限制，不为了理论上的全平台一致性引入新的媒体状态机、
  volume 或临时媒体服务器。
