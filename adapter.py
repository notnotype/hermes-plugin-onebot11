"""OneBot 11 (QQ) Platform Adapter for Hermes Agent.

架构：
- 反向 WebSocket 收事件（LLBot/NapCat 的 ob11 ws-reverse 拨入本插件的 WS 服务）
- HTTP API 发送（POST /{action} 调 ob11 HTTP 服务）

本文件是唯一的 Hermes 胶水层；协议逻辑全部在 onebot11/ 包内（零 Hermes 依赖）。
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from typing import Any

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

# 内部子包导入桥: 网关把插件目录当包加载(相对导入), 测试/独立脚本按顶层模块导入
try:
    from . import onebot11 as _proto
except ImportError:
    import onebot11 as _proto

InboundEvent = _proto.events.InboundEvent
build_inbound_event = _proto.events.build_inbound_event
OneBotApiError = _proto.http_api.OneBotApiError
OneBotHttpApi = _proto.http_api.OneBotHttpApi
chunk_text = _proto.http_api.chunk_text
ToolContext = _proto.permissions.ToolContext
parse_admin_list = _proto.permissions.parse_admin_list
validate_tool_call = _proto.permissions.validate_tool_call
TOOL_SCHEMAS = _proto.tools.TOOL_SCHEMAS
handle_get_friend_msg_history = _proto.tools.handle_get_friend_msg_history
handle_get_group_msg_history = _proto.tools.handle_get_group_msg_history
handle_get_message = _proto.tools.handle_get_message
ReverseWsServer = _proto.ws_server.ReverseWsServer

logger = logging.getLogger(__name__)

_PLATFORM_NAME = "onebot11"

# 工具 handler 表：名称 → (onebot11/tools 里的 handler)
_TOOL_HANDLERS: dict[str, Any] = {
    "qq_get_message": handle_get_message,
    "qq_get_group_msg_history": handle_get_group_msg_history,
    "qq_get_friend_msg_history": handle_get_friend_msg_history,
}


def _platform() -> Platform:
    """惰性解析平台枚举。

    插件模块在 register() 之前就会被 import,而 Platform._missing_ 只接受
    已注册的平台名——所以不能在模块级调用 Platform("onebot11")（irc 插件
    同样是在 __init__ 里才解析）。
    """
    return Platform(_PLATFORM_NAME)


class OneBot11Adapter(BasePlatformAdapter):
    """OneBot 11 平台适配器。"""

    def __init__(self, config: PlatformConfig) -> None:
        super().__init__(config=config, platform=_platform())
        extra = config.extra or {}

        # 连接配置（env 优先于 config.yaml）
        self.ws_port = int(os.getenv("ONEBOT11_WS_PORT") or extra.get("ws_port", 18880))
        self.access_token = os.getenv("ONEBOT11_ACCESS_TOKEN") or extra.get("access_token", "")
        http_api = os.getenv("ONEBOT11_HTTP_API") or extra.get("http_api", "")
        self.self_id = os.getenv("ONEBOT11_SELF_ID") or extra.get("self_id", "")

        # 私聊策略
        self.dm_policy = (os.getenv("ONEBOT11_DM_POLICY") or extra.get("dm_policy", "open")).lower()
        raw_allowed = os.getenv("ONEBOT11_ALLOWED_USERS") or extra.get("allowed_users", "")
        self.allowed_users = {u.strip() for u in str(raw_allowed).split(",") if u.strip()}

        # 工具权限（管理员列表）
        raw_admins = os.getenv("ONEBOT11_ADMINS") or extra.get("admins", "")
        self._admins = parse_admin_list(str(raw_admins))

        self._api = OneBotHttpApi(base_url=http_api, token=self.access_token)
        self._ws: ReverseWsServer | None = None
        # 会话类型登记：入站事件记录 chat_id → chat_type,发送时据此选择私聊/群聊
        self._chat_types: dict[str, str] = {}
        self._media_dir: str | None = None

    # ── 连接生命周期 ──────────────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """启动反向 WS 服务,等待 QQ 框架拨入。"""
        if not self._api_base():
            self._set_fatal_error(
                "config_missing", "ONEBOT11_HTTP_API 未配置", retryable=False
            )
            return False
        self._ws = ReverseWsServer(
            port=self.ws_port, token=self.access_token, on_event=self._on_ws_event
        )
        await self._ws.start()
        logger.info("OneBot11: 反向 WS 已监听 0.0.0.0:%s,等待 QQ 框架拨入", self.ws_port)
        self._mark_connected()
        return True

    def _api_base(self) -> str:
        """读取 HTTP API 地址（config 可能通过 extra 传入）。"""
        extra = getattr(self.config, "extra", {}) or {}
        return os.getenv("ONEBOT11_HTTP_API") or extra.get("http_api", "")

    async def disconnect(self) -> None:
        """停止 WS 服务并关闭 HTTP 会话。"""
        if self._ws is not None:
            await self._ws.stop()
            self._ws = None
        await self._api.close()
        self._mark_disconnected()

    # ── 入站事件处理 ──────────────────────────────────────────────────────

    async def _on_ws_event(self, raw: dict) -> None:
        """收到 OneBot 11 事件：归一化 → 私聊策略 → 转 MessageEvent。"""
        event = build_inbound_event(raw, self.self_id)
        if event is None:
            return
        # 私聊策略
        if event.chat_type == "dm":
            if self.dm_policy == "disabled":
                return
            if self.dm_policy == "allowlist" and event.user_id not in self.allowed_users:
                return
        self._chat_types[event.chat_id] = event.chat_type
        await self.handle_message(await self._build_message_event(event))

    async def _build_message_event(self, ev: InboundEvent) -> MessageEvent:
        """InboundEvent → Hermes MessageEvent。

        群聊消息加 `[昵称] ` 前缀（整群共享会话时区分发言者）。
        """
        text = ev.text
        if ev.chat_type == "group" and ev.user_name and ev.user_name != ev.user_id:
            text = f"[{ev.user_name}] {text}"

        media_urls: list[str] = []
        media_types: list[str] = []
        for image in ev.images:
            path = await self._download_image(image)
            if path:
                media_urls.append(path)
                media_types.append("photo")

        source = self.build_source(
            chat_id=ev.chat_id,
            chat_name=ev.chat_id,
            chat_type=ev.chat_type,
            user_id=ev.user_id,
            user_name=ev.user_name,
        )
        return MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=ev.message_id,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=ev.reply_to_message_id,
            metadata={"mentioned_self": ev.mentioned_self, "onebot11_raw": raw_meta(ev)},
        )

    async def _download_image(self, image: str) -> str | None:
        """把 http(s) 图片下载到临时目录,返回本地路径；失败返回 None。"""
        if not image.startswith(("http://", "https://")):
            # file id 需要框架侧解析,v1 跳过
            return None
        try:
            if self._media_dir is None:
                self._media_dir = tempfile.mkdtemp(prefix="hermes-onebot11-")
            return await self._api.download_to_temp(image, self._media_dir)
        except Exception:
            logger.debug("图片下载失败: %s", image, exc_info=True)
            return None

    # ── 发送 ──────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        """发送消息（长文本自动分块）。"""
        if self._ws is None:
            return SendResult(success=False, error="Not connected")
        chat_type = self._chat_types.get(chat_id, "group")
        limit = self.max_message_length_for_chat(chat_id)
        pieces = chunk_text(content, limit) if limit > 0 else [content]
        last_id = ""
        for piece in pieces:
            try:
                last_id = await self._api.send_message(
                    chat_id, piece, chat_type=chat_type, reply_to=reply_to
                )
            except OneBotApiError as exc:
                return SendResult(success=False, error=str(exc))
        return SendResult(success=True, message_id=last_id or str(uuid.uuid4()))

    async def send_typing(self, chat_id: str, metadata: dict | None = None) -> None:
        """QQ 无 typing 指示器,no-op。"""

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """返回会话基本信息。"""
        chat_type = self._chat_types.get(chat_id, "group")
        return {"name": chat_id, "type": "group" if chat_type == "group" else "dm"}

    # ── 工具权限桥接 ──────────────────────────────────────────────────────

    def _resolve_tool_context(self, session_id: str | None) -> ToolContext | None:
        """按 session_id 从 gateway runner 找回会话来源,构造工具上下文。

        这是权限校验的可信通道：user/chat 来自入站事件的 SessionSource,
        不由 LLM 传入,杜绝群里提示词注入伪造身份。
        """
        if not session_id:
            return None
        runner = getattr(self, "gateway_runner", None)
        if runner is None:
            return None
        try:
            source = runner._get_cached_session_source(session_id)  # type: ignore[attr-defined]
        except Exception:
            source = None
        if source is None or getattr(source, "platform", None) != self.platform:
            return None
        return ToolContext(
            user_id=str(source.user_id or ""),
            chat_type=source.chat_type if source.chat_type in ("group", "dm") else "dm",
            chat_id=str(source.chat_id),
        )

    def _make_tool_handler(self, tool_name: str):
        """包装 onebot11/tools 的 handler：权限校验 + 会话注入 + JSON 序列化。"""

        async def wrapped(args: dict, **kwargs: Any) -> str:
            ctx = self._resolve_tool_context(kwargs.get("session_id"))
            if ctx is None:
                return "拒绝调用: 无法解析当前会话上下文(仅支持在 QQ 会话中调用)"
            error = validate_tool_call(tool_name, args, ctx, self._admins)
            if error:
                return f"拒绝调用: {error}"
            handler = _TOOL_HANDLERS[tool_name]
            try:
                result = await handler(self._api, args, ctx)
                return json.dumps(result, ensure_ascii=False, default=str)
            except OneBotApiError as exc:
                return f"调用失败: {exc}"

        return wrapped


def raw_meta(ev: InboundEvent) -> dict:
    """把入站事件的可展示信息放进 metadata（供调试/审计）。"""
    return {
        "chat_type": ev.chat_type,
        "user_id": ev.user_id,
        "user_name": ev.user_name,
        "mentioned_self": ev.mentioned_self,
        "images": ev.images,
    }


# ---------------------------------------------------------------------------
# 插件注册
# ---------------------------------------------------------------------------


def check_requirements() -> bool:
    """环境变量是否满足最低配置。"""
    return bool(os.getenv("ONEBOT11_HTTP_API", "").strip() and os.getenv("ONEBOT11_SELF_ID", "").strip())


def validate_config(config) -> bool:
    """config.yaml 是否满足最低配置。"""
    extra = getattr(config, "extra", {}) or {}
    return bool((os.getenv("ONEBOT11_HTTP_API") or extra.get("http_api")) and (os.getenv("ONEBOT11_SELF_ID") or extra.get("self_id")))


def _env_enablement() -> dict | None:
    """从环境变量种子 PlatformConfig.extra,支持纯 env 部署。"""
    http_api = os.getenv("ONEBOT11_HTTP_API", "").strip()
    self_id = os.getenv("ONEBOT11_SELF_ID", "").strip()
    if not (http_api and self_id):
        return None
    seed: dict[str, Any] = {"http_api": http_api, "self_id": self_id}
    for key in ("ONEBOT11_ACCESS_TOKEN", "ONEBOT11_WS_PORT", "ONEBOT11_DM_POLICY",
                "ONEBOT11_ALLOWED_USERS", "ONEBOT11_ADMINS"):
        value = os.getenv(key, "").strip()
        if value:
            seed[key.removeprefix("ONEBOT11_").lower()] = value
    home = os.getenv("ONEBOT11_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": "OneBot 11 Home"}
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: str | None = None,
    media_files: list[str] | None = None,
    force_document: bool = False,
) -> dict:
    """cron 独立进程投递（gateway 不在本进程时）。"""
    extra = getattr(pconfig, "extra", {}) or {}
    http_api = os.getenv("ONEBOT11_HTTP_API") or extra.get("http_api", "")
    token = os.getenv("ONEBOT11_ACCESS_TOKEN") or extra.get("access_token", "")
    if not http_api:
        return {"error": "ONEBOT11_HTTP_API 未配置"}
    chat_type = extra.get("home_channel_type", "group")
    api = OneBotHttpApi(base_url=http_api, token=token)
    try:
        await api.send_message(str(chat_id), message, chat_type=str(chat_type))
        return {"success": True, "message_id": ""}
    except OneBotApiError as exc:
        return {"error": str(exc)}
    finally:
        await api.close()


def register(ctx) -> None:
    """插件入口：注册平台 + 三个查询工具。"""
    ctx.register_platform(
        name="onebot11",
        label="OneBot 11 (QQ)",
        adapter_factory=lambda cfg: OneBot11Adapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["ONEBOT11_HTTP_API", "ONEBOT11_SELF_ID"],
        install_hint="已随 hermes plugins install 安装;运行时依赖 aiohttp",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ONEBOT11_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ONEBOT11_ALLOWED_USERS",
        allow_all_env="ONEBOT11_ALLOW_ALL_USERS",
        max_message_length=4000,
        emoji="🐧",
        platform_hint=(
            "You are chatting via OneBot 11 (QQ). Plain text is preferred; "
            "markdown renders poorly in QQ. In groups, address you with @; "
            "a shared group conversation carries [nickname] sender prefixes. "
            "You can query group message history with qq_get_group_msg_history "
            "(limited to the current group)."
        ),
    )
    for name, handler in _TOOL_HANDLERS.items():
        ctx.register_tool(
            name=name,
            toolset="onebot11",
            schema=TOOL_SCHEMAS[name],
            handler=_tool_dispatch(handler, name),
            is_async=True,
            description=_TOOL_DESCRIPTIONS[name],
            emoji="🔍",
        )


def _tool_dispatch(handler, name: str):
    """注册到全局 registry 的 handler：调用时按会话找到活着的 adapter。"""

    async def wrapped(args: dict, **kwargs: Any) -> str:
        adapter = _get_live_adapter()
        if adapter is None:
            return "拒绝调用: OneBot 11 网关适配器未运行"
        return await adapter._make_tool_handler(name)(args, **kwargs)

    return wrapped


def _get_live_adapter() -> OneBot11Adapter | None:
    """从 gateway runner 找当前平台的 live adapter。"""
    try:
        from gateway.run import _gateway_runner_ref

        runner = _gateway_runner_ref()
    except Exception:
        return None
    if runner is None:
        return None
    try:
        adapter = runner.adapters.get(_platform())
    except Exception:
        return None
    return adapter if isinstance(adapter, OneBot11Adapter) else None


_TOOL_DESCRIPTIONS: dict[str, str] = {
    "qq_get_message": "按消息 ID 查询 QQ 单条消息内容",
    "qq_get_group_msg_history": "查询当前 QQ 群最近消息（只能查发起会话所在群）",
    "qq_get_friend_msg_history": "查询与某人的 QQ 私聊最近消息（仅管理员,且只能查自己）",
}
