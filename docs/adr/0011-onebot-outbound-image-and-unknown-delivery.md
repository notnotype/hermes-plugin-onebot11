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

Hermes 的媒体发送接口可以返回每个图片的 `SendResult`。文本块和媒体块
全部明确成功时，image-only turn 才算成功；部分成功、缺少结果或
`error_kind="unknown"` 都算失败。旧 adapter 返回 `None` 仍保持调用兼容，
但不把无法确认的 image-only turn 伪装成成功。

任何非幂等 OneBot 出站结果为 `unknown` 时，Hermes 不自动重试、不发送
plain-text fallback，也不从 cron live delivery 改走 standalone sender。
这样不提供 exactly-once，但避免“第一次可能已发送、第二次又发送”的确定性
重复。未知结果由 OneBot 队列/operation ledger 和管理员人工流程处理。

## 原因

OneBot `send_group_msg`/`send_private_msg` 是非幂等请求。连接在响应返回前
断开时，客户端无法知道服务端是否已经执行。对文本和图片使用同一
unknown-safe 合同，才能让 lease fencing、队列 ack 和 Hermes completion
保持一致。

## 影响

- 图片消息可跨宿主机/容器边界传输，但 base64 会增加内存和请求体大小；
  现有单图、单消息图片数量和总量边界继续生效。
- 旧 Hermes 没有媒体结果聚合能力时，插件继续支持文本；图片能力必须在
  Hermes 媒体合同可用时启用，不能静默伪造成功。
- 真实部署仍需把允许的远程媒体 host 配置到 allowlist；本 ADR 不引入
  DNS rebinding 服务端或新的媒体数据库。
