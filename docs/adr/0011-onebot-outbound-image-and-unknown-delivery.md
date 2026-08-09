# ADR-0011：OneBot 出站图片与 unknown delivery 合同

- 状态：已接受
- 日期：2026-08-08

## 决策

OneBot 11 出站图片由插件负责协议适配，使用受限的 `base64://` image
segment，不使用宿主机路径直传、临时 HTTP 媒体服务或 Docker volume。原因是
Arch 上 Hermes 运行在宿主机、LLBot 运行在容器内，宿主机路径不一定对 QQ
框架可见。

插件只接受 Hermes 允许的图片目录中的 PNG、JPEG、GIF、WebP 文件，并在
发送前校验文件大小和魔数。远程图片先由 Hermes/插件下载并经过现有
host、port、重定向、Content-Type、魔数和大小检查；下载外部媒体时不携带
OneBot Bearer token。图片发送失败不会把 URL 或本地路径回退成普通文本。

Agent 最终回复图片采用有意的 best-effort 合同。插件会预检图片大小、
类型和数量，并发送受限 `base64://` segment，但不改造 Hermes 全局 delivery
来获得更强的 image-only 完成状态。部分成功或 `unknown` 不会让插件重新执行
整轮 Agent。

任何非幂等 OneBot 出站结果为 `unknown` 时，本插件不自动重试、不把图片
回退为 plain-text，也不重新执行整轮 Agent。这样不提供 exactly-once，
但避免插件自身造成“第一次可能已发送、第二次又发送”的确定性重复。
未知结果由 OneBot 队列/operation ledger 和管理员人工流程处理。

## 原因

OneBot `send_group_msg`/`send_private_msg` 是非幂等请求。连接在响应返回前
断开时，客户端无法知道服务端是否已经执行。对文本和图片使用同一
unknown-safe 合同，才能让 lease fencing、队列 ack 和 Hermes completion
保持一致。

## 影响

- 图片消息可跨宿主机/容器边界传输，但 base64 会增加内存和请求体大小；
  现有单图、单消息图片数量和总量边界继续生效。
- 插件启动和图片发送不依赖 Hermes 媒体结果能力探测。
- 真实部署仍需把允许的远程媒体 host 配置到 allowlist；本 ADR 不引入
  DNS rebinding 服务端或新的媒体数据库。

## 范围与折中

本 ADR 的主要支持场景是 Hermes Agent 最终回复中的图片。通用
`send_message`、cron 或跨进程 standalone sender 的 plugin media
不是本轮的交付合同；这些窄路径保持文本能力，不宣称完整图片支持。

这是有意的妥协：现有微信等 adapter 也存在媒体失败聚合不完全、批量
结果不统一或 fallback 行为不同的历史兼容方式。为了 OneBot 一个较窄
的使用场景改造 Hermes 全部 provider，会扩大变更面、增加升级冲突，
但不会提升原始需求的核心价值。

因此本轮不新增 Hermes 媒体 PR、不引入新的媒体数据库、Docker volume、
临时 HTTP 媒体服务器或全平台媒体能力矩阵。若未来 `send_message`
图片成为明确产品需求，再单独设计通用的 fail-closed 媒体能力声明。
