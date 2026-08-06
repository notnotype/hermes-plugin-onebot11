"""Hermes 与 OneBot 11 的唯一胶水层。

群消息先进入 ``onebot11.QueueStore``，由确定性触发器创建 durable request，
``GroupDispatcher`` 再以共享 session 启动一个 Hermes turn。协议和状态机本身
保持零 Hermes 依赖，方便独立测试。
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import json
import logging
import os
import shutil
import tempfile
import time
import uuid
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
)

try:
    from . import onebot11 as _proto
except ImportError:
    import onebot11 as _proto

CallerContext = _proto.CallerContext
ChatTarget = _proto.ChatTarget
GroupDispatcher = _proto.GroupDispatcher
QueueFull = _proto.queue.QueueFull
QueueLease = _proto.QueueLease
QueueMessage = _proto.QueueMessage
QueueStore = _proto.QueueStore
TriggerRequest = _proto.TriggerRequest
TurnBinding = _proto.TurnBinding
TurnBindingStore = _proto.TurnBindingStore
WRITE_TOOLS = _proto.permissions.WRITE_TOOLS
READ_ONLY_TOOLS = _proto.permissions.READ_ONLY_TOOLS
ALL_TOOLS = _proto.permissions.ALL_TOOLS
build_inbound_event = _proto.events.build_inbound_event
normalize_auxiliary_event = _proto.events.normalize_auxiliary_event
OneBotApiError = _proto.http_api.OneBotApiError
OneBotHttpApi = _proto.http_api.OneBotHttpApi
is_loopback_http_url = _proto.http_api.is_loopback_http_url
parse_http_base_url = _proto.http_api.parse_http_base_url
chunk_text = _proto.http_api.chunk_text
is_numeric_message_id = _proto.http_api.is_numeric_message_id
AuditLog = _proto.audit.AuditLog
ConfirmationStore = _proto.confirm.ConfirmationStore
ToolContext = _proto.permissions.ToolContext
build_role_tools = _proto.permissions.build_role_tools
build_access_policy = _proto.permissions.build_access_policy
parse_admin_list = _proto.permissions.parse_admin_list
parse_bool = _proto.permissions.parse_bool
parse_id_list = _proto.permissions.parse_id_list
role_for_user = _proto.permissions.role_for_user
access_allowed = _proto.permissions.access_allowed
role_prompt = _proto.permissions.role_prompt
validate_tool_call = _proto.permissions.validate_tool_call
handle_get_friend_msg_history = _proto.tools.handle_get_friend_msg_history
handle_get_group_info = _proto.tools.handle_get_group_info
handle_get_group_member_info = _proto.tools.handle_get_group_member_info
handle_get_group_msg_history = _proto.tools.handle_get_group_msg_history
handle_get_message = _proto.tools.handle_get_message
handle_write_action = _proto.tools.handle_write_action
READ_TOOL_NAMES = _proto.tools.READ_TOOL_NAMES
TOOL_SCHEMAS = _proto.tools.TOOL_SCHEMAS
WRITE_TOOL_NAMES = _proto.tools.WRITE_TOOL_NAMES
build_trigger_config = _proto.triggers.build_trigger_config
build_llm_trigger_input = _proto.triggers.build_llm_trigger_input
LayeredTriggerState = _proto.triggers.LayeredTriggerState
TriggerAction = _proto.triggers.TriggerAction
parse_llm_decision = _proto.triggers.parse_llm_decision
build_agent_context = _proto.context.build_agent_context
should_trigger = _proto.triggers.should_trigger
ReverseWsServer = _proto.ws_server.ReverseWsServer

logger = logging.getLogger(__name__)
_PLATFORM_NAME = "onebot11"
_PROCESSING_REACTION_EMOJI_ID = "128064"  # LLBot 的 QQ Emoji「👀」ID
_CURRENT_CALLER: contextvars.ContextVar[CallerContext | None] = contextvars.ContextVar(
    "onebot11_current_caller", default=None
)
_CURRENT_BINDING: contextvars.ContextVar[TurnBinding | None] = contextvars.ContextVar(
    "onebot11_current_turn_binding", default=None
)
_CURRENT_EVENT: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "onebot11_current_event", default=None
)

_TOOL_HANDLERS: dict[str, Any] = {
    "qq_get_message": handle_get_message,
    "qq_get_group_msg_history": handle_get_group_msg_history,
    "qq_get_friend_msg_history": handle_get_friend_msg_history,
    "qq_get_group_info": handle_get_group_info,
    "qq_get_group_member_info": handle_get_group_member_info,
}


def _platform() -> Platform:
    """惰性解析平台枚举，避免 register 前导入时 registry 尚未注册。"""
    return Platform(_PLATFORM_NAME)


def _platform_value(value: Any) -> str:
    """读取 Hermes Platform 或字符串的稳定值。"""
    return str(getattr(value, "value", value) or "").casefold()


def _is_loopback_url(url: str) -> bool:
    """判断 HTTP API 是否只连接本机回环。"""
    return is_loopback_http_url(url)


def _resolve_hermes_home() -> Path:
    """读取 Hermes 的真实状态根目录，避免可靠队列退化为临时文件。"""
    try:
        from hermes_cli.config import get_hermes_home

        return Path(get_hermes_home()).expanduser().resolve()
    except (ImportError, AttributeError, OSError, TypeError, ValueError):
        configured = os.getenv("HERMES_HOME", "").strip()
        if configured:
            return Path(configured).expanduser().resolve()
        if os.name == "nt":
            local_appdata = os.getenv("LOCALAPPDATA", "").strip()
            base = Path(local_appdata) if local_appdata else Path.home() / "AppData" / "Local"
            return (base / "hermes").resolve()
        return (Path.home() / ".hermes").resolve()


def _effective_extra(extra: Mapping[str, Any]) -> dict[str, Any]:
    """合并 OneBot 部署环境覆盖，保留显式空值的 fail-closed 语义。"""
    effective = dict(extra)
    env_to_extra = {
        "ONEBOT11_HTTP_API": "http_api",
        "ONEBOT11_SELF_ID": "self_id",
        "ONEBOT11_ACCESS_TOKEN": "access_token",
        "ONEBOT11_WS_PORT": "ws_port",
        "ONEBOT11_WS_HOST": "ws_host",
        "ONEBOT11_DM_POLICY": "dm_policy",
        "ONEBOT11_ALLOWED_USERS": "allowed_users",
        "ONEBOT11_ALLOWED_GROUPS": "allowed_groups",
        "ONEBOT11_REQUIRE_MENTION": "require_mention",
        "ONEBOT11_SUPER_ADMINS": "super_admins",
        "ONEBOT11_ADMINS": "admins",
        "ONEBOT11_QUEUE_DB": "queue_db_path",
        "ONEBOT11_HOME_CHANNEL_TYPE": "home_channel_type",
    }
    for env_name, extra_name in env_to_extra.items():
        if env_name in os.environ:
            effective[extra_name] = os.environ[env_name]
    return effective


def _serializable_caller(context: CallerContext) -> dict[str, Any]:
    """把不可变身份放进当前 synthetic event 的有限 metadata。"""
    return {
        "user_id": context.user_id,
        "chat_type": context.chat_type,
        "chat_id": context.chat_id,
        "role": context.role,
        "allowed_tools": sorted(context.allowed_tools),
        "lease_id": context.lease_id,
        "self_id": context.self_id,
    }


def _caller_from_metadata(value: Any) -> CallerContext | None:
    """只从 adapter metadata 读取身份坐标，再由 live adapter 重算角色。"""
    if not isinstance(value, Mapping):
        return None
    adapter = _get_live_adapter()
    if adapter is None:
        return None
    try:
        user_id = str(value["user_id"])
        chat_type = str(value["chat_type"])
        chat_id = str(value["chat_id"])
        lease_id = str(value["lease_id"]) if value.get("lease_id") else None
        metadata_self_id = str(value.get("self_id") or "").strip()
        if metadata_self_id != adapter.self_id:
            return None
        self_id = adapter.self_id
    except (KeyError, TypeError, ValueError):
        return None
    if chat_type not in {"group", "dm"} or not user_id or not chat_id:
        return None
    if not adapter._chat_access_allowed(chat_type, chat_id, user_id):
        return None
    if lease_id and not adapter._lease_matches_target(lease_id, chat_type, chat_id):
        return None
    role = role_for_user(user_id, adapter.super_admins)
    return CallerContext(
        user_id=user_id,
        chat_type=chat_type,
        chat_id=chat_id,
        role=role,
        allowed_tools=adapter.role_tools.get(role, frozenset()),
        lease_id=lease_id,
        self_id=self_id,
    )


class OneBot11Adapter(BasePlatformAdapter):
    """OneBot 11 适配器：私聊直接 turn，群聊持久队列 + 共享 session。"""

    def __init__(self, config: PlatformConfig) -> None:
        """读取并校验配置，初始化协议客户端和群级状态机。"""
        extra = _effective_extra(config.extra if isinstance(config.extra, Mapping) else {})
        configured_mode = str(extra.get("session_mode", "shared")).casefold()
        if configured_mode != "shared" or parse_bool(
            extra.get("group_sessions_per_user"), default=False, name="group_sessions_per_user"
        ):
            raise ValueError("OneBot11 群 session 只允许 session_mode=shared")
        extra["session_mode"] = "shared"
        extra["group_sessions_per_user"] = False
        config.extra = extra
        super().__init__(config=config, platform=_platform())

        try:
            self.ws_port = int(os.getenv("ONEBOT11_WS_PORT") or extra.get("ws_port", 18880))
        except (TypeError, ValueError) as exc:
            raise ValueError("ONEBOT11_WS_PORT 必须是 1-65535 范围内的整数") from exc
        if not 0 <= self.ws_port <= 65535:
            raise ValueError("ONEBOT11_WS_PORT 必须是 0-65535 范围内的整数")
        self.ws_host = str(os.getenv("ONEBOT11_WS_HOST") or extra.get("ws_host", "127.0.0.1")).strip()
        self.access_token = str(
            os.getenv("ONEBOT11_ACCESS_TOKEN") or extra.get("access_token", "")
        ).strip()
        http_api = str(os.getenv("ONEBOT11_HTTP_API") or extra.get("http_api") or "").strip()
        self.self_id = str(os.getenv("ONEBOT11_SELF_ID") or extra.get("self_id") or "").strip()
        if not self.self_id:
            raise ValueError("ONEBOT11_SELF_ID 未配置")
        if http_api:
            parse_http_base_url(http_api)
        if self.ws_host not in {"127.0.0.1", "::1", "localhost"} and not self.access_token:
            raise ValueError("WS 非 loopback 地址必须配置 ONEBOT11_ACCESS_TOKEN")
        if http_api and not _is_loopback_url(http_api) and not self.access_token:
            raise ValueError("HTTP API 非本机地址必须配置 ONEBOT11_ACCESS_TOKEN")

        self._access_policy = build_access_policy(extra, os.environ)
        self.dm_policy = self._access_policy.dm_policy
        self.allowed_users = set(self._access_policy.allowed_users)
        self.allowed_groups = set(self._access_policy.allowed_groups)
        self.require_mention = parse_bool(
            os.getenv("ONEBOT11_REQUIRE_MENTION") if os.getenv("ONEBOT11_REQUIRE_MENTION") is not None else extra.get("require_mention"),
            default=True,
            name="require_mention",
        )
        raw_super_admins = os.getenv("ONEBOT11_SUPER_ADMINS")
        if raw_super_admins is None:
            raw_super_admins = os.getenv("ONEBOT11_ADMINS")
        if raw_super_admins is None:
            raw_super_admins = extra.get("super_admins")
        if raw_super_admins is None:
            raw_super_admins = extra.get("admins")
        self.super_admins = parse_id_list(raw_super_admins)
        self.role_tools = build_role_tools(extra)
        self._processing_reaction_enabled = parse_bool(
            extra.get("processing_reaction_enabled"),
            default=True,
            name="processing_reaction_enabled",
        )
        self._processing_reaction_emoji_id = str(
            extra.get("processing_reaction_emoji_id", _PROCESSING_REACTION_EMOJI_ID)
        ).strip()
        if not self._processing_reaction_emoji_id:
            raise ValueError("processing_reaction_emoji_id 不能为空")
        parsed_trigger_config = build_trigger_config(extra)
        self.trigger_config = replace(parsed_trigger_config, require_mention=self.require_mention)
        self._last_trigger_at: dict[str, float] = {}
        self._llm_trigger_tasks: dict[str, asyncio.Task[None]] = {}
        self._trigger_timer_tasks: dict[str, asyncio.Task[None]] = {}
        self._trigger_state_locks: dict[str, asyncio.Lock] = {}
        self._trigger_states: dict[str, LayeredTriggerState] = {}
        self._llm_trigger_api_supported: bool | None = None
        self._llm_trigger_api_audited = False
        self._llm_trigger_semaphore: asyncio.Semaphore | None = None
        self._llm_trigger_loop: asyncio.AbstractEventLoop | None = None
        self._llm_trigger_route_logged = False

        media_hosts = parse_id_list(extra.get("media_allowed_hosts"))
        media_ports = {int(item) for item in parse_id_list(extra.get("media_allowed_ports"))}
        self._api = OneBotHttpApi(
            base_url=http_api,
            token=self.access_token,
            timeout=float(extra.get("http_timeout_seconds", 10)),
            max_retries=int(extra.get("query_max_retries", 1)),
            max_response_bytes=int(extra.get("http_max_response_bytes", 1_000_000)),
            allowed_media_hosts=media_hosts,
            allowed_media_ports=media_ports,
            max_media_bytes=int(extra.get("max_image_bytes", 8_000_000)),
            max_redirects=int(extra.get("max_image_redirects", 3)),
        )
        self._max_media_total_bytes = max(1024, int(extra.get("max_image_total_bytes", 16_000_000)))
        self._max_images_per_message = max(
            0, min(32, int(extra.get("max_images_per_message", 4)))
        )

        self._hermes_home = _resolve_hermes_home()
        queue_path = os.getenv("ONEBOT11_QUEUE_DB") or extra.get("queue_db_path")
        if not queue_path:
            queue_path = str(self._hermes_home / "onebot11" / "queue.sqlite3")
        self._queue = QueueStore(
            queue_path,
            max_messages=int(extra.get("queue_max_messages", 1000)),
            max_queue_bytes=int(extra.get("queue_max_bytes", 2_000_000)),
            max_message_bytes=int(extra.get("queue_max_message_bytes", 32_000)),
            max_original_bytes=int(extra.get("queue_max_original_bytes", 8_000)),
            max_summary_bytes=int(extra.get("queue_max_summary_bytes", 16_000)),
            recent_originals=int(extra.get("queue_recent_originals", 3)),
            dedupe_ttl_seconds=float(extra.get("queue_dedupe_ttl_seconds", 7 * 24 * 3600)),
            max_attempts=int(extra.get("queue_max_attempts", 3)),
        )
        self._agent_input_bytes = max(4_096, min(256 * 1024, int(extra.get("agent_input_bytes", 64 * 1024))))
        self._agent_recent_originals = max(0, int(extra.get("agent_recent_originals", 3)))
        self._dispatcher = GroupDispatcher(
            self._queue,
            self._start_queue_turn,
            lease_seconds=float(extra.get("queue_lease_seconds", 120)),
            recovery_poll_seconds=float(extra.get("queue_recovery_poll_seconds", 5)),
            can_dispatch=self._can_dispatch_chat,
            on_lease_lost=self._on_lease_lost,
        )
        self._bindings = TurnBindingStore()
        self._confirmations = ConfirmationStore(float(extra.get("confirm_ttl_seconds", 60)))
        audit_path = extra.get("audit_path") or (
            str(self._hermes_home / "onebot11" / "audit.jsonl")
        )
        self._audit = AuditLog(audit_path, max_bytes=int(extra.get("audit_max_bytes", 2_000_000)))
        self._ws: ReverseWsServer | None = None
        self._chat_types: dict[str, str] = {}
        self._targets: dict[str, ChatTarget | None] = {}
        self._ambiguous_targets: set[str] = set()
        media_root = self._hermes_home / "onebot11" / "media"
        media_prefix = "turn-"
        media_root.mkdir(parents=True, exist_ok=True)
        self._media_root = media_root.resolve()
        self._media_prefix = media_prefix
        self._media_orphan_ttl = max(
            60.0, float(extra.get("media_orphan_ttl_seconds", 24 * 3600))
        )
        self._media_dir = tempfile.mkdtemp(prefix=media_prefix, dir=str(self._media_root))
        self._unknown_leases: set[str] = set()
        self._outbound_started: set[str] = set()
        self._outbound_successful: set[str] = set()
        self._outbound_known_failure: set[str] = set()
        self._unknown_operations: set[str] = set()
        self._processing_reaction_message_ids: dict[str, str] = {}
        self._fenced_leases: set[str] = set()
        self._lease_session_keys: dict[str, str] = {}
        self._pending_completions: dict[str, tuple[ProcessingOutcome, bool, bool, str | None]] = {}
        self._aux_event_count = 0
        self._closed = False
        self._cleanup_orphan_media_dirs()
        logger.info(
            "OneBot11: groups=%s dm_policy=%s super_admins=%s mention=%s session=shared",
            sorted(self.allowed_groups) or "全部群",
            self.dm_policy,
            sorted(self.super_admins) or "无",
            self.require_mention,
        )

    @property
    def enforces_own_access_policy(self) -> bool:
        """声明 adapter 在进入 Hermes 前执行自己的 allow/deny。"""
        return True

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """启动反向 WS，并恢复 SQLite 中未完成的群 turn。"""
        del is_reconnect
        if not self._api_base():
            self._set_fatal_error("config_missing", "ONEBOT11_HTTP_API 未配置", retryable=False)
            return False
        self._ws = ReverseWsServer(
            port=self.ws_port,
            token=self.access_token,
            on_event=self._on_ws_event,
            host=self.ws_host,
            max_queue=int(self.config.extra.get("ws_max_queue", 256)),
            max_inflight=int(self.config.extra.get("ws_max_inflight", 32)),
        )
        await self._ws.start()
        await self._dispatcher.recover()
        if self.trigger_config.llm_enabled:
            pending_chats = await asyncio.to_thread(self._queue.pending_chat_ids)
            for chat_id in pending_chats:
                await self._restore_trigger_state(chat_id)
        self._mark_connected()
        logger.info("OneBot11: 反向 WS 已监听 %s:%s", self.ws_host, self._ws.port)
        return True

    def _api_base(self) -> str:
        """读取 HTTP API 地址。"""
        return os.getenv("ONEBOT11_HTTP_API") or str((self.config.extra or {}).get("http_api", ""))

    async def disconnect(self) -> None:
        """停止 WS、heartbeat、HTTP 会话并回收本插件创建的媒体文件。"""
        self._closed = True
        trigger_tasks = list(self._llm_trigger_tasks.values())
        self._llm_trigger_tasks.clear()
        timer_tasks = list(self._trigger_timer_tasks.values())
        self._trigger_timer_tasks.clear()
        for task in trigger_tasks:
            task.cancel()
        for task in timer_tasks:
            task.cancel()
        if trigger_tasks or timer_tasks:
            await asyncio.gather(*trigger_tasks, *timer_tasks, return_exceptions=True)
        cancel_background = getattr(self, "cancel_background_tasks", None)
        if callable(cancel_background):
            await cancel_background()
        await self._dispatcher.close()
        if self._ws is not None:
            await self._ws.stop()
            self._ws = None
        await self._api.close()
        self._cleanup_media()
        self._queue.close()
        self._mark_disconnected()

    async def _on_ws_event(self, raw: dict) -> None:
        """归一化事件、执行入队前授权并路由到 DM/群 dispatch。"""
        auxiliary = normalize_auxiliary_event(raw)
        if auxiliary is not None:
            self._aux_event_count += 1
            logger.debug("OneBot11 auxiliary event: %s", auxiliary.summary)
            self._audit.record(
                "auxiliary_event",
                {
                    "post_type": auxiliary.post_type,
                    "event_type": auxiliary.event_type,
                    "chat_id": auxiliary.chat_id,
                    "user_id": auxiliary.user_id,
                    "summary": auxiliary.summary[:512],
                },
            )
            return
        event = build_inbound_event(raw, self.self_id)
        if event is None:
            return
        if not self._access_allowed(event):
            self._audit.record(
                "access_denied",
                {
                    "chat_type": event.chat_type,
                    "chat_id": event.chat_id,
                    "user_id": event.user_id,
                    "reason": "adapter access policy",
                },
            )
            return
        target = ChatTarget(event.chat_type, event.chat_id)
        previous = self._targets.get(event.chat_id)
        if previous is not None and previous != target:
            self._targets[event.chat_id] = None
            self._ambiguous_targets.add(event.chat_id)
        elif event.chat_id not in self._targets:
            self._targets[event.chat_id] = target
        elif event.chat_id not in self._ambiguous_targets:
            self._targets[event.chat_id] = target
        self._chat_types[event.chat_id] = event.chat_type

        normalized_text = event.text.strip()
        if normalized_text == "/onebot" or normalized_text.startswith("/onebot "):
            await self._handle_admin_command(event)
            return
        if event.chat_type == "group":
            await self._enqueue_group_event(event)
            return
        message_event = await self._build_message_event(event)
        try:
            await self.handle_message(message_event)
        except BaseException:
            self._cleanup_media(
                (message_event.metadata or {}).get("onebot11_media_paths"),
                media_dir=(message_event.metadata or {}).get("onebot11_media_dir"),
            )
            raise

    def _access_allowed(self, event: _proto.events.InboundEvent) -> bool:
        """在图片下载和入队前应用严格访问策略。"""
        allowed = self._chat_access_allowed(event.chat_type, event.chat_id, event.user_id)
        if not allowed and event.chat_type == "group":
            logger.info("OneBot11: 群 %s 不在当前访问策略，拒绝入队", event.chat_id)
        return allowed

    def _chat_access_allowed(
        self, chat_type: str, chat_id: str, user_id: str | None = None
    ) -> bool:
        """用同一纯策略判断实时入站和恢复 dispatch 是否可以继续。"""
        try:
            explicit_all = parse_bool(
                os.getenv("ONEBOT11_ALLOW_ALL_USERS"),
                default=False,
                name="ONEBOT11_ALLOW_ALL_USERS",
            ) or parse_bool(
                os.getenv("GATEWAY_ALLOW_ALL_USERS"),
                default=False,
                name="GATEWAY_ALLOW_ALL_USERS",
            )
        except ValueError:
            explicit_all = False
        return access_allowed(
            chat_type,
            chat_id,
            user_id,
            allowed_groups=self.allowed_groups,
            dm_policy=self.dm_policy,
            allowed_users=self.allowed_users,
            allow_all_users=explicit_all,
        )

    def _can_dispatch_chat(self, chat_id: str) -> bool:
        """恢复群 lease 前重新应用当前 adapter 的群访问策略。"""
        chat_type = self._queue.chat_type(str(chat_id)) or self._chat_types.get(str(chat_id))
        return chat_type == "group" and self._chat_access_allowed("group", str(chat_id))

    async def _build_message_event(self, ev: _proto.events.InboundEvent) -> MessageEvent:
        """把内部消息转换为 Hermes MessageEvent，并下载受限图片。"""
        text = ev.text
        if ev.chat_type == "group" and ev.user_name and ev.user_name != ev.user_id:
            text = f"[{ev.user_name}] {text}"
        media_urls: list[str] = []
        media_total_bytes = 0
        media_limited = False
        media_dir = self._new_media_dir() if ev.images else self._media_dir
        bounded_images = ev.images[: self._max_images_per_message]
        for index, image in enumerate(bounded_images):
            path = await self._download_image(image, media_dir)
            if path:
                try:
                    image_bytes = Path(path).stat().st_size
                except OSError:
                    image_bytes = 0
                if media_total_bytes + image_bytes > self._max_media_total_bytes:
                    try:
                        Path(path).unlink(missing_ok=True)
                    except OSError:
                        logger.debug("超过本条消息媒体总大小限制，临时文件清理失败", exc_info=True)
                    media_limited = True
                    break
                media_urls.append(path)
                media_total_bytes += image_bytes
                if media_total_bytes >= self._max_media_total_bytes and index + 1 < len(bounded_images):
                    media_limited = True
                    break
        source = self.build_source(
            chat_id=ev.chat_id,
            chat_name=ev.chat_id,
            chat_type=ev.chat_type,
            user_id=ev.user_id,
            user_name=ev.user_name,
            message_id=ev.message_id,
            role_authorized=self._chat_access_allowed(ev.chat_type, ev.chat_id, ev.user_id),
        )
        metadata = {
            "onebot11_raw": ev.raw_metadata,
            "onebot11_markers": ev.markers[:32],
            "mentioned_self": ev.mentioned_self,
            "onebot11_media_paths": media_urls,
            "onebot11_media_dir": media_dir if ev.images else None,
            "onebot11_media_limited": media_limited,
            "onebot11_target": {"chat_type": ev.chat_type, "chat_id": ev.chat_id},
            "onebot11_managed_context": True,
        }
        return MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=ev.message_id,
            media_urls=media_urls,
            media_types=["photo"] * len(media_urls),
            reply_to_message_id=ev.reply_to_message_id,
            metadata=metadata,
        )

    async def _download_image(self, image: str, dest_dir: str | None = None) -> str | None:
        """下载单张图片；URL/响应安全边界由 OneBotHttpApi 执行。"""
        if not str(image).startswith(("http://", "https://")):
            return None
        return await self._api.download_to_temp(str(image), dest_dir or self._media_dir)

    def _new_media_dir(self) -> str:
        """为一个 turn 创建受控媒体目录，便于完成后精确回收。"""
        return tempfile.mkdtemp(prefix=self._media_prefix, dir=str(self._media_root))

    def _trigger_state_for(self, chat_id: str) -> LayeredTriggerState:
        """获取一个群的内存触发状态；重启后首次访问从 idle 开始。"""
        normalized = str(chat_id)
        state = self._trigger_states.get(normalized)
        if state is None:
            state = LayeredTriggerState(self.trigger_config)
            self._trigger_states[normalized] = state
        return state

    def _trigger_lock_for(self, chat_id: str) -> asyncio.Lock:
        """串行化同群入队和触发状态变化，保持 revision 与状态对应。"""
        normalized = str(chat_id)
        lock = self._trigger_state_locks.get(normalized)
        if lock is None:
            lock = asyncio.Lock()
            self._trigger_state_locks[normalized] = lock
        return lock

    def _stable_message_id(
        self,
        message_id: Any,
        *,
        chat_id: str,
        text: str,
        metadata: Mapping[str, Any],
    ) -> str:
        """没有 OneBot message_id 时生成可重放的稳定去重 ID。"""
        normalized = str(message_id or "").strip()
        if normalized:
            return normalized
        payload = json.dumps(
            {"chat_id": str(chat_id), "text": str(text), "metadata": dict(metadata)},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")
        return "hash:" + hashlib.sha256(payload).hexdigest()

    async def _enqueue_group_event(self, ev: _proto.events.InboundEvent) -> None:
        """先完成规范化、授权和持久入队，媒体在 lease turn 中按需下载。"""
        metadata = {
            "onebot11_markers": ev.markers[:32],
            "onebot11_images": ev.images[: self._max_images_per_message],
            "onebot11_reply_to": ev.reply_to_message_id,
            "onebot11_segments": ev.segments[:32],
            "onebot11_raw_metadata": ev.raw_metadata,
            "onebot11_mentioned_self": ev.mentioned_self,
        }
        message_id = self._stable_message_id(
            ev.message_id,
            chat_id=ev.chat_id,
            text=ev.text,
            metadata=metadata,
        )
        message = QueueMessage(
            chat_id=ev.chat_id,
            chat_type="group",
            message_id=message_id,
            user_id=ev.user_id,
            user_name=ev.user_name or ev.user_id or "unknown",
            text=ev.text,
            raw_text=ev.raw_text,
            metadata=metadata,
            message_key=f"group:{message_id}",
        )
        caller = self._caller_for_event(
            SimpleNamespace(user_id=ev.user_id, chat_type=ev.chat_type, chat_id=ev.chat_id)
        )
        await self._enqueue_group_message(
            message,
            mentioned_self=ev.mentioned_self,
            caller=caller,
            user_name=ev.user_name or caller.user_id,
        )

    async def _enqueue_group_message(
        self,
        message: QueueMessage,
        *,
        mentioned_self: bool,
        caller: CallerContext,
        user_name: str,
    ) -> None:
        """把一个规范消息入队，并按硬触发或候选触发推进状态机。"""
        chat_id = str(message.chat_id)
        should_notify = False
        cancel_judgement = False
        async with self._trigger_lock_for(chat_id):
            before = await asyncio.to_thread(self._queue.status, chat_id)
            now = time.monotonic()
            previous_trigger_at = self._last_trigger_at.get(chat_id)
            decision = should_trigger(
                chat_type="group",
                text=message.text,
                mentioned_self=mentioned_self,
                config=self.trigger_config,
                last_trigger_at=previous_trigger_at,
                now=now,
            )
            trigger = (
                TriggerRequest.create(
                    chat_id,
                    str(message.message_key),
                    decision.reason,
                    caller.user_id,
                    user_name or caller.user_id,
                )
                if decision.triggered
                else None
            )
            try:
                result = await asyncio.to_thread(self._queue.enqueue, message, trigger)
            except QueueFull:
                self._audit.record(
                    "queue_full",
                    {"chat_type": "group", "chat_id": chat_id, "user_id": caller.user_id},
                )
                raise
            if result.duplicate:
                # 只在释放群触发锁后通知 dispatcher。通知可能认领 lease 并
                # 启动完整 Hermes turn，不能和入队状态机嵌套在同一把锁里。
                should_notify = bool(result.trigger_request_id)
                if decision.triggered and result.trigger_request_id:
                    state = self._trigger_states.get(chat_id)
                    if state is not None:
                        state.invalidate_judgement()
                        cancel_judgement = True
            else:
                action = TriggerAction("none", reason=decision.reason)
                status = await asyncio.to_thread(self._queue.status, chat_id)
                paused = bool(status.get("paused"))
                if (
                    self.trigger_config.llm_enabled
                    and chat_id in self.trigger_config.llm_allowed_groups
                    and not paused
                ):
                    state = self._trigger_state_for(chat_id)
                    action = state.observe_message(
                        chat_type="group",
                        text=message.text,
                        mentioned_self=mentioned_self,
                        has_context=bool(before.get("summary") or int(before.get("pending", 0)) > 0),
                        revision=int(status.get("revision", 0)),
                        now=now,
                        last_trigger_at=previous_trigger_at,
                    )
                    if action.kind == "schedule":
                        await self._apply_trigger_action_locked(chat_id, action)
                if decision.triggered or action.kind == "direct":
                    if decision.triggered:
                        self._last_trigger_at[chat_id] = now
                        state = self._trigger_states.get(chat_id)
                        if state is not None:
                            state.invalidate_judgement()
                            cancel_judgement = True
                    should_notify = True

        if cancel_judgement:
            self._cancel_llm_judgement(chat_id)
        if should_notify:
            await self._dispatcher.notify(chat_id)

    async def handle_message(self, event: MessageEvent) -> None:
        """群消息入队并按触发结果 dispatch；私聊沿用 Hermes 直接 turn。"""
        source = event.source
        if source is None or source.platform != self.platform:
            return
        if not self._chat_access_allowed(
            str(source.chat_type), str(source.chat_id), str(source.user_id or "")
        ):
            self._audit.record(
                "access_denied",
                {
                    "chat_type": str(source.chat_type),
                    "chat_id": str(source.chat_id),
                    "user_id": str(source.user_id or ""),
                    "reason": "adapter access policy",
                },
            )
            return
        caller = self._caller_for_event(source)
        event.metadata = dict(event.metadata or {})
        event.metadata["onebot11_caller_context"] = _serializable_caller(caller)
        if source.chat_type == "dm":
            event_token = _CURRENT_EVENT.set(event)
            token = _CURRENT_CALLER.set(caller)
            try:
                await super().handle_message(event)
            finally:
                _CURRENT_CALLER.reset(token)
                _CURRENT_EVENT.reset(event_token)
            return

        metadata = dict(event.metadata or {})
        event.metadata = metadata
        mentioned_self = bool(
            metadata.get("mentioned_self") or metadata.get("onebot11_mentioned_self")
        )
        message_id = self._stable_message_id(
            event.message_id,
            chat_id=str(source.chat_id),
            text=event.text,
            metadata=metadata,
        )
        images = metadata.get("onebot11_images") or []
        if not isinstance(images, list):
            images = []
        if not images and getattr(event, "media_urls", None):
            images = list(event.media_urls)
        metadata["onebot11_images"] = images[: self._max_images_per_message]
        metadata["onebot11_mentioned_self"] = mentioned_self
        message = QueueMessage(
            chat_id=str(source.chat_id),
            chat_type="group",
            message_id=message_id,
            user_id=str(source.user_id or ""),
            user_name=str(source.user_name or source.user_id or "unknown"),
            text=event.text,
            raw_text=str((event.metadata or {}).get("onebot11_raw_text") or event.text),
            metadata=metadata,
            message_key=f"group:{message_id}",
        )
        await self._enqueue_group_message(
            message,
            mentioned_self=mentioned_self,
            caller=caller,
            user_name=str(source.user_name or caller.user_id),
        )

    def _llm_trigger_route(self) -> tuple[str, str] | None:
        """读取显式旁路 provider/model；缺失时绝不回退主模型。"""
        provider = self.trigger_config.llm_provider.strip()
        model = self.trigger_config.llm_model.strip()
        if not provider or provider.casefold() == "auto" or not model:
            try:
                from hermes_cli.config import load_config

                config = load_config() or {}
                auxiliary = config.get("auxiliary") if isinstance(config, dict) else None
                route = auxiliary.get("onebot11_trigger") if isinstance(auxiliary, Mapping) else None
                if isinstance(route, Mapping):
                    provider = str(route.get("provider") or provider).strip()
                    model = str(route.get("model") or model).strip()
            except Exception:
                pass
        if not provider or provider.casefold() == "auto" or not model:
            if not self._llm_trigger_route_logged:
                logger.info("OneBot11 LLM trigger 已启用但未配置明确 provider/model，旁路已跳过")
                self._llm_trigger_route_logged = True
            return None
        return provider, model

    def _schedule_llm_trigger(self, chat_id: str) -> None:
        """兼容旧调用点：只调度现有状态机的 due timer。"""
        self._schedule_trigger_timer(str(chat_id))

    def _cancel_llm_judgement(self, chat_id: str) -> None:
        """硬触发优先时取消同群旁路判断，旧结果由状态机再次 fencing。"""
        normalized = str(chat_id)
        current = asyncio.current_task()
        task = self._llm_trigger_tasks.get(normalized)
        if task is not None and not task.done() and task is not current:
            task.cancel()
        timer = self._trigger_timer_tasks.get(normalized)
        if timer is not None and not timer.done() and timer is not current:
            timer.cancel()

    def _llm_trigger_semaphore_for_loop(self) -> asyncio.Semaphore:
        """为当前事件循环创建插件级 LLM 判断并发限制。"""
        loop = asyncio.get_running_loop()
        if self._llm_trigger_semaphore is None or self._llm_trigger_loop is not loop:
            self._llm_trigger_semaphore = asyncio.Semaphore(self.trigger_config.llm_concurrency)
            self._llm_trigger_loop = loop
        return self._llm_trigger_semaphore

    def _llm_trigger_api_ready(self) -> bool:
        """检查 Hermes 是否支持旁路严格参数；旧 API 直接安全降级。"""
        if self._llm_trigger_api_supported is not None:
            return self._llm_trigger_api_supported
        try:
            from agent.auxiliary_client import async_call_llm

            parameters = inspect.signature(async_call_llm).parameters
            self._llm_trigger_api_supported = {
                "fallback_policy",
                "max_attempts",
            }.issubset(parameters)
        except (ImportError, TypeError, ValueError):
            self._llm_trigger_api_supported = False
        if not self._llm_trigger_api_supported and not self._llm_trigger_api_audited:
            self._audit.record(
                "llm_trigger_disabled",
                {"reason": "Hermes auxiliary API 不支持 fallback_policy/max_attempts"},
            )
            self._llm_trigger_api_audited = True
        return self._llm_trigger_api_supported

    def _schedule_trigger_timer(self, chat_id: str) -> None:
        """为一个群保留唯一 timer，负责 debounce、wait 和 engaged 到期。"""
        if self._closed or not self.trigger_config.llm_enabled:
            return
        normalized = str(chat_id)
        state = self._trigger_states.get(normalized)
        if state is None:
            return
        due_at = state.debounce_due or state.wait_until or state.engaged_until
        if due_at is None:
            return
        previous = self._trigger_timer_tasks.get(normalized)
        current = asyncio.current_task()
        if previous is not None and not previous.done() and previous is not current:
            previous.cancel()
        task = asyncio.create_task(self._run_trigger_timer(normalized))
        self._trigger_timer_tasks[normalized] = task

    async def _run_trigger_timer(self, chat_id: str) -> None:
        """等待当前 due 时间并把无副作用动作交给 adapter 生命周期。"""
        try:
            while not self._closed:
                state = self._trigger_states.get(chat_id)
                if state is None:
                    return
                due_at = state.debounce_due or state.wait_until or state.engaged_until
                if due_at is None:
                    return
                await asyncio.sleep(max(0.0, due_at - time.monotonic()))
                async with self._trigger_lock_for(chat_id):
                    if self._closed or not self._chat_access_allowed("group", chat_id):
                        return
                    status = await asyncio.to_thread(self._queue.status, chat_id)
                    if self._dispatcher.active(chat_id) is not None:
                        # Agent turn 尚未完全收口时不启动旁路仲裁；completion
                        # 会在释放活动 lease 后重新安排现有 due timer。
                        return
                    if status.get("paused") or int(status.get("pending_trigger_requests", 0)) > 0:
                        state.pause() if status.get("paused") else state.invalidate_judgement()
                        return
                    action = state.on_timer(now=time.monotonic())
                if action.kind == "judge":
                    await self._start_llm_judgement(chat_id, action)
                elif action.kind in {"schedule", "wait"}:
                    await self._apply_trigger_action(chat_id, action)
                elif state.engaged_until is not None:
                    # wait 从 engaged 状态到期后，状态机会恢复 engaged；
                    # 当前 timer 随即结束，必须为剩余活跃窗口重新挂载计时器。
                    self._schedule_trigger_timer(chat_id)
                if action.kind == "judge":
                    return
        except asyncio.CancelledError:
            return
        finally:
            if self._trigger_timer_tasks.get(chat_id) is asyncio.current_task():
                self._trigger_timer_tasks.pop(chat_id, None)

    async def _apply_trigger_action(self, chat_id: str, action: TriggerAction) -> None:
        """执行状态机动作；候选阶段绝不 claim queue lease。"""
        normalized = str(chat_id)
        if action.kind not in {"schedule", "wait"}:
            return
        async with self._trigger_lock_for(normalized):
            await self._apply_trigger_action_locked(normalized, action)

    async def _apply_trigger_action_locked(self, chat_id: str, action: TriggerAction) -> None:
        """在群锁内应用候选动作；调用方不得在此执行网络请求。"""
        normalized = str(chat_id)
        if action.kind not in {"schedule", "wait"}:
            return
        state = self._trigger_states.get(normalized)
        if state is None or not self._chat_access_allowed("group", normalized):
            return
        status = await asyncio.to_thread(self._queue.status, normalized)
        if status.get("paused") or int(status.get("pending_trigger_requests", 0)) > 0:
            if status.get("paused"):
                state.pause()
            else:
                state.invalidate_judgement()
            return
        # wait 是已经完成的旁路判断结果，不应因为 provider 此刻不可用
        # 丢掉等待状态；下一条候选消息再重新检查 route。
        if action.kind == "wait":
            self._schedule_trigger_timer(normalized)
            return
        route = self._llm_trigger_route()
        api_ready = self._llm_trigger_api_ready() if route else False
        if not route or not api_ready:
            state.on_llm_failure(
                now=time.monotonic(),
                current_revision=int(status.get("revision", 0)),
                generation=action.generation,
            )
            if state.engaged_until is not None:
                self._schedule_trigger_timer(normalized)
            self._audit.record(
                "llm_trigger_skip",
                {
                    "chat_id": normalized,
                    "reason": "provider_missing" if not route else "hermes_api_unsupported",
                    "pending": int(status.get("pending", 0)),
                    "rate_limited": False,
                },
            )
            return
        self._schedule_trigger_timer(normalized)

    def _llm_trigger_ready(self) -> bool:
        """判断旁路 provider/model 与 Hermes 严格 API 是否同时可用。"""
        return bool(
            self._llm_trigger_route()
            and self._llm_trigger_api_ready()
            and self.trigger_config.llm_enabled
        )

    async def _start_llm_judgement(self, chat_id: str, action: TriggerAction) -> None:
        """为每群启动至多一个旁路判断 task。"""
        normalized = str(chat_id)
        async with self._trigger_lock_for(normalized):
            current = self._llm_trigger_tasks.get(normalized)
            if current is not None and not current.done():
                return
            state = self._trigger_states.get(normalized)
            status = await asyncio.to_thread(self._queue.status, normalized)
            if (
                state is None
                or not state.judgement_is_current(action.generation)
                or status.get("paused")
                or int(status.get("pending_trigger_requests", 0)) > 0
                or not self._chat_access_allowed("group", normalized)
            ):
                if state is not None and status.get("paused"):
                    state.pause()
                return
            if not self._llm_trigger_ready():
                state.on_llm_failure(
                    now=time.monotonic(),
                    current_revision=int(status.get("revision", 0)),
                    generation=action.generation,
                )
                if state.engaged_until is not None:
                    self._schedule_trigger_timer(normalized)
                return
            task = asyncio.create_task(self._judge_llm_trigger(normalized, action))
            self._llm_trigger_tasks[normalized] = task

    async def _create_llm_trigger(
        self,
        chat_id: str,
        *,
        expected_generation: int | None = None,
        expected_revision: int | None = None,
    ) -> bool:
        """在群锁内安全创建旁路 trigger，释放锁后再启动 dispatcher。"""
        normalized = str(chat_id)
        async with self._trigger_lock_for(normalized):
            request_id = await self._create_llm_trigger_locked(
                normalized,
                expected_generation=expected_generation,
                expected_revision=expected_revision,
            )
        if request_id:
            await self._dispatcher.notify(normalized)
            return True
        return False

    async def _create_llm_trigger_locked(
        self,
        chat_id: str,
        *,
        expected_generation: int | None = None,
        expected_revision: int | None = None,
    ) -> str | None:
        """在已持有群触发锁时创建旁路 trigger；调用方不得在此发网络请求。"""
        normalized = str(chat_id)
        if not self._chat_access_allowed("group", normalized):
            return None
        state = self._trigger_states.get(normalized)
        if expected_generation is not None and (
            state is None or not state.generation_matches(expected_generation)
        ):
            return None
        status = await asyncio.to_thread(self._queue.status, normalized)
        if (
            status.get("paused")
            or int(status.get("pending_trigger_requests", 0)) > 0
            or int(status.get("pending", 0)) <= 0
        ):
            return None
        current_revision = int(status.get("revision", 0))
        if expected_revision is not None and current_revision != expected_revision:
            return None
        messages = await asyncio.to_thread(self._queue.peek, normalized)
        if not messages:
            return None
        latest = messages[-1]
        request_id = await asyncio.to_thread(
            self._queue.create_trigger,
            normalized,
            "llm",
            latest.user_id,
            latest.user_name,
        )
        if request_id:
            self._last_trigger_at[normalized] = time.monotonic()
        return request_id

    async def _flush_group(
        self,
        chat_id: str,
        *,
        caller_user_id: str,
        caller_user_name: str,
    ) -> tuple[bool, bool, bool]:
        """在群锁内准备管理员 flush，再在锁外启动 dispatcher。"""
        normalized = str(chat_id)
        cancel_judgement = False
        has_request = False
        paused = False
        async with self._trigger_lock_for(normalized):
            if not self._chat_access_allowed("group", normalized):
                return False, False, False
            state = self._trigger_states.get(normalized)
            if state is not None:
                state.invalidate_judgement()
                cancel_judgement = True
            status = await asyncio.to_thread(self._queue.status, normalized)
            paused = bool(status.get("paused"))
            if int(status.get("pending_trigger_requests", 0)) > 0:
                has_request = True
            else:
                request_id = await asyncio.to_thread(
                    self._queue.create_trigger,
                    normalized,
                    "admin_flush",
                    caller_user_id,
                    caller_user_name,
                )
                has_request = request_id is not None
        if cancel_judgement:
            self._cancel_llm_judgement(normalized)
        if not has_request:
            return False, False, paused
        started = await self._dispatcher.notify(normalized)
        return True, started, paused

    async def _clear_group(self, chat_id: str) -> int:
        """清理队列时同时失效该群旧的旁路状态和定时任务。"""
        normalized = str(chat_id)
        cancel_judgement = False
        async with self._trigger_lock_for(normalized):
            count = await asyncio.to_thread(self._queue.clear, normalized)
            state = self._trigger_states.get(normalized)
            if state is not None:
                state.invalidate_judgement()
            cancel_judgement = (
                state is not None
                or normalized in self._llm_trigger_tasks
                or normalized in self._trigger_timer_tasks
            )
        if cancel_judgement:
            self._cancel_llm_judgement(normalized)
        return count

    async def _set_group_paused(self, chat_id: str, paused: bool) -> bool:
        """切换群级自动 dispatch，并在暂停时失效旁路判断。"""
        normalized = str(chat_id)
        cancel_judgement = False
        async with self._trigger_lock_for(normalized):
            if not self._chat_access_allowed("group", normalized):
                return False
            state = self._trigger_states.get(normalized)
            if paused:
                if state is not None:
                    state.pause()
                cancel_judgement = (
                    state is not None
                    or normalized in self._llm_trigger_tasks
                    or normalized in self._trigger_timer_tasks
                )
            await self._dispatcher.set_paused(
                normalized,
                paused,
                notify_on_resume=False,
            )
        if cancel_judgement:
            self._cancel_llm_judgement(normalized)
        if not paused:
            await self._restore_trigger_state(normalized)
        return True

    async def _restore_trigger_state(self, chat_id: str) -> bool:
        """从 pending 消息重建候选 timer；不恢复重启前的 active/engaged 状态。"""
        normalized = str(chat_id)
        should_notify = False
        async with self._trigger_lock_for(normalized):
            if not self._chat_access_allowed("group", normalized):
                return False
            status = await asyncio.to_thread(self._queue.status, normalized)
            if status.get("paused"):
                return False
            if int(status.get("pending_trigger_requests", 0)) > 0:
                should_notify = True
            elif (
                not self.trigger_config.llm_enabled
                or normalized not in self.trigger_config.llm_allowed_groups
            ):
                return False
            else:
                state = self._trigger_state_for(normalized)
                messages = await asyncio.to_thread(self._queue.peek, normalized)
                if not messages:
                    return False
                revision = int(status.get("revision", 0))
                action = TriggerAction("none", reason="restore")
                for index, message in enumerate(messages):
                    mentioned = bool(message.metadata.get("onebot11_mentioned_self"))
                    action = state.observe_message(
                        chat_type="group",
                        text=message.text,
                        mentioned_self=mentioned,
                        has_context=bool(status.get("summary") or index > 0),
                        revision=revision,
                        now=time.monotonic(),
                        last_trigger_at=self._last_trigger_at.get(normalized),
                    )
                    if action.kind == "direct":
                        request_id = await asyncio.to_thread(
                            self._queue.create_trigger,
                            normalized,
                            "restore",
                            message.user_id,
                            message.user_name,
                        )
                        should_notify = bool(request_id)
                        if should_notify:
                            self._last_trigger_at[normalized] = time.monotonic()
                        break
                if action.kind == "schedule":
                    await self._apply_trigger_action_locked(normalized, action)
        if should_notify:
            await self._dispatcher.notify(normalized)
        return should_notify

    async def _apply_llm_result_locked(
        self,
        chat_id: str,
        action: TriggerAction,
        *,
        decision: str,
        wait_seconds: int,
        observed_revision: int,
    ) -> tuple[TriggerAction | None, bool, str | None]:
        """在群锁内 fence 旁路结果；返回动作、是否需要 notify 和失败原因。"""
        normalized = str(chat_id)
        state = self._trigger_states.get(normalized)
        if state is None or not state.judgement_is_current(action.generation):
            return None, False, "stale_judgement"
        if not self._chat_access_allowed("group", normalized):
            state.invalidate_judgement()
            return None, False, "access_denied"
        status = await asyncio.to_thread(self._queue.status, normalized)
        if status.get("paused"):
            state.pause()
            return None, False, "paused"
        if int(status.get("pending_trigger_requests", 0)) > 0:
            state.invalidate_judgement()
            return None, True, "hard_trigger_already_pending"
        current_revision = int(status.get("revision", 0))
        result_action = state.on_llm_result(
            decision=decision,
            wait_seconds=wait_seconds,
            observed_revision=observed_revision,
            current_revision=current_revision,
            now=time.monotonic(),
            generation=action.generation,
        )
        if result_action.kind == "direct":
            request_id = await self._create_llm_trigger_locked(
                normalized,
                expected_generation=action.generation,
                expected_revision=current_revision,
            )
            return result_action, bool(request_id), None if request_id else "trigger_not_created"
        if result_action.kind in {"schedule", "wait"}:
            await self._apply_trigger_action_locked(normalized, result_action)
        elif state.engaged_until is not None:
            self._schedule_trigger_timer(normalized)
        return result_action, False, None

    async def _apply_llm_failure(
        self,
        chat_id: str,
        action: TriggerAction,
        *,
        failure: str,
    ) -> None:
        """在群锁内处理取消/失败结果，避免旧 task 污染新状态。"""
        normalized = str(chat_id)
        retry_action: TriggerAction | None = None
        stale = False
        async with self._trigger_lock_for(normalized):
            state = self._trigger_states.get(normalized)
            if state is None or not state.judgement_is_current(action.generation):
                stale = True
            elif not self._chat_access_allowed("group", normalized):
                state.invalidate_judgement()
            else:
                status = await asyncio.to_thread(self._queue.status, normalized)
                if status.get("paused"):
                    state.pause()
                elif int(status.get("pending_trigger_requests", 0)) > 0:
                    state.invalidate_judgement()
                else:
                    retry_action = state.on_llm_failure(
                        now=time.monotonic(),
                        current_revision=int(status.get("revision", 0)),
                        generation=action.generation,
                    )
                    if retry_action.kind == "schedule":
                        await self._apply_trigger_action_locked(normalized, retry_action)
                    elif state.engaged_until is not None:
                        self._schedule_trigger_timer(normalized)
        self._audit.record(
            "llm_trigger",
            {
                "chat_id": normalized,
                "candidate_type": action.candidate_type or "candidate",
                "decision": "ignore",
                "wait_seconds": 0,
                "duration_ms": 0,
                "failure": "stale_judgement" if stale else failure,
                "rate_limited": False,
            },
        )

    async def _judge_llm_trigger(self, chat_id: str, action: TriggerAction) -> None:
        """只 peek 判断；失败、超时和非法 JSON 都保留 pending 消息。"""
        normalized = str(chat_id)
        started_at = time.monotonic()
        observed_revision = int(action.revision or 0)
        candidate_type = action.candidate_type or "candidate"
        decision_name = "ignore"
        wait_seconds = 0
        failure = ""
        rate_limited = False
        messages_count = 0
        input_bytes = 0
        notify = False
        try:
            async with self._trigger_lock_for(normalized):
                state = self._trigger_states.get(normalized)
                if state is None or not state.judgement_is_current(action.generation):
                    failure = "stale_judgement"
                elif not self._chat_access_allowed("group", normalized):
                    failure = "access_denied"
                else:
                    status = await asyncio.to_thread(self._queue.status, normalized)
                    if status.get("paused"):
                        failure = "paused"
                    elif int(status.get("pending_trigger_requests", 0)) > 0:
                        failure = "hard_trigger_already_pending"
                    elif self.trigger_config.cooldown_seconds > 0:
                        last_trigger = self._last_trigger_at.get(normalized)
                        if (
                            last_trigger is not None
                            and time.monotonic() - last_trigger
                            < self.trigger_config.cooldown_seconds
                        ):
                            failure = "cooldown"
                    else:
                        messages = await asyncio.to_thread(self._queue.peek, normalized)
                        messages_count = len(messages)
                        if not messages:
                            failure = "no_pending_messages"
                        else:
                            if not observed_revision:
                                observed_revision = int(status.get("revision", 0))
                            prompt = build_llm_trigger_input(
                                str(status.get("summary") or ""),
                                messages,
                                self.trigger_config.llm_input_bytes,
                                candidate_type=candidate_type,
                            )
                            input_bytes = len(prompt.encode("utf-8"))
                            provider_model = self._llm_trigger_route()
                            if not provider_model or not self._llm_trigger_api_ready():
                                failure = (
                                    "provider_missing"
                                    if not provider_model
                                    else "hermes_api_unsupported"
                                )
                            else:
                                provider, model = provider_model
                                from agent.auxiliary_client import async_call_llm

                                semaphore = self._llm_trigger_semaphore_for_loop()
                                rate_limited = semaphore.locked()
                                # 释放群锁后再等待模型，允许新消息入队并推进
                                # dirty_revision；这里仅把调用参数复制出来。
                                request = (
                                    async_call_llm,
                                    provider,
                                    model,
                                    prompt,
                                    semaphore,
                                )
            if failure:
                return
            async_call_llm, provider, model, prompt, semaphore = request
            async with semaphore:
                response = await asyncio.wait_for(
                    async_call_llm(
                        task="onebot11_trigger",
                        provider=provider,
                        model=model,
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "你是严格的 OneBot11 消息触发判断器。"
                                    "只能返回 JSON，不要输出 Markdown。"
                                ),
                            },
                            {"role": "user", "content": prompt},
                        ],
                        temperature=0,
                        max_tokens=32,
                        timeout=self.trigger_config.llm_timeout_seconds,
                        fallback_policy="none",
                        max_attempts=1,
                    ),
                    timeout=self.trigger_config.llm_timeout_seconds + 1.0,
                )
            content = response.choices[0].message.content
            if isinstance(content, Mapping):
                parsed_value: Any = content
            elif isinstance(content, str):
                parsed_value = json.loads(content.strip())
            else:
                raise ValueError("旁路模型没有返回 JSON 对象")
            parsed = parse_llm_decision(parsed_value)
            if parsed is None:
                raise ValueError("旁路模型返回非法 decision JSON")
            decision_name, wait_seconds = parsed
            async with self._trigger_lock_for(normalized):
                result_action, notify, result_failure = await self._apply_llm_result_locked(
                    normalized,
                    action,
                    decision=decision_name,
                    wait_seconds=wait_seconds,
                    observed_revision=observed_revision,
                )
                if result_failure:
                    failure = result_failure
            if not failure:
                self._audit.record(
                    "llm_trigger",
                    {
                        "chat_id": normalized,
                        "candidate_type": candidate_type,
                        "pending": messages_count,
                        "input_bytes": input_bytes,
                        "decision": decision_name,
                        "wait_seconds": wait_seconds,
                        "duration_ms": int((time.monotonic() - started_at) * 1000),
                        "rate_limited": rate_limited,
                    },
                )
            if notify:
                await self._dispatcher.notify(normalized)
        except asyncio.CancelledError:
            if not self._closed:
                failure = "cancelled"
            raise
        except ImportError:
            failure = "provider_missing"
        except TimeoutError:
            failure = "timeout"
        except json.JSONDecodeError:
            failure = "invalid_json"
        except (TypeError, ValueError, AttributeError):
            failure = "invalid_result"
        except Exception:
            failure = "model_error"
            logger.info("OneBot11 LLM trigger 失败，按不触发处理", exc_info=True)
        finally:
            if failure and not self._closed:
                await self._apply_llm_failure(
                    normalized,
                    action,
                    failure=failure,
                )
            current = self._llm_trigger_tasks.get(normalized)
            if current is asyncio.current_task():
                self._llm_trigger_tasks.pop(normalized, None)

    def _caller_for_event(self, source: Any, *, lease_id: str | None = None) -> CallerContext:
        """按当前入站消息解析角色和允许工具集合。"""
        user_id = str(source.user_id or "")
        role = role_for_user(user_id, self.super_admins)
        return CallerContext(
            user_id=user_id,
            chat_type=str(source.chat_type),
            chat_id=str(source.chat_id),
            role=role,
            allowed_tools=self.role_tools.get(role, frozenset()),
            lease_id=lease_id,
            self_id=self.self_id,
        )

    async def _start_queue_turn(self, lease: QueueLease) -> None:
        """将 lease 批量编排为一个 synthetic user turn，保持 caller/target 绑定。"""
        if not self._chat_access_allowed("group", lease.chat_id):
            raise PermissionError("当前群已不再满足 OneBot11 allowed_groups 策略")
        if not await asyncio.to_thread(self._queue.mark_agent_started, lease):
            raise PermissionError("OneBot11 queue lease 已失效")
        trigger = lease.trigger
        role = role_for_user(trigger.caller_user_id, self.super_admins)
        caller = CallerContext(
            user_id=trigger.caller_user_id,
            chat_type="group",
            chat_id=lease.chat_id,
            role=role,
            allowed_tools=self.role_tools.get(role, frozenset()),
            lease_id=lease.lease_id,
            self_id=self.self_id,
        )
        media_paths: list[str] = []
        has_images = any(
            isinstance(message.metadata.get("onebot11_images"), list)
            and message.metadata.get("onebot11_images")
            for message in lease.messages
        )
        media_dir = self._new_media_dir() if has_images else self._media_dir
        handed_off = False
        try:
            reaction_message_id = await self._set_processing_reaction(lease, enabled=True)
            if reaction_message_id is not None:
                self._processing_reaction_message_ids[lease.lease_id] = reaction_message_id
            media_total_bytes = 0
            media_limited = False
            reply_id: str | None = None
            for message in lease.messages:
                images = message.metadata.get("onebot11_images") or []
                if isinstance(images, list):
                    bounded_images = images[: self._max_images_per_message]
                    for image in bounded_images:
                        path = await self._download_image(str(image), media_dir)
                        if not path:
                            continue
                        try:
                            image_bytes = Path(path).stat().st_size
                        except OSError:
                            image_bytes = 0
                        if media_total_bytes + image_bytes > self._max_media_total_bytes:
                            Path(path).unlink(missing_ok=True)
                            media_limited = True
                            break
                        media_paths.append(path)
                        media_total_bytes += image_bytes
                        if media_total_bytes >= self._max_media_total_bytes:
                            media_limited = True
                            break
                    if media_limited:
                        break
            if not await asyncio.to_thread(self._queue.is_lease_current, lease):
                raise PermissionError("OneBot11 queue lease 在媒体处理期间失效")
            if lease.messages:
                reply_id = lease.messages[-1].message_id
            lines_text = build_agent_context(
                lease.summary,
                lease.messages,
                self._agent_input_bytes,
                self._agent_recent_originals,
            )
            source = self.build_source(
                chat_id=lease.chat_id,
                chat_name=lease.chat_id,
                chat_type="group",
                user_id=caller.user_id,
                user_name=trigger.caller_user_name,
                message_id=reply_id,
                role_authorized=True,
            )
            event = MessageEvent(
                text=lines_text,
                message_type=MessageType.TEXT,
                source=source,
                message_id=reply_id,
                media_urls=media_paths,
                media_types=["photo"] * len(media_paths),
                reply_to_message_id=reply_id,
                metadata={
                    "onebot11_queue_turn": True,
                    "onebot11_lease_id": lease.lease_id,
                    "onebot11_lease_revision": lease.revision,
                    "onebot11_caller_context": _serializable_caller(caller),
                    "onebot11_target": {"chat_type": "group", "chat_id": lease.chat_id},
                    "onebot11_media_dir": media_dir if has_images else None,
                    "onebot11_media_paths": list(media_paths),
                    "onebot11_media_limited": media_limited,
                    "onebot11_defer_completion": True,
                    "onebot11_managed_context": True,
                },
            )
            try:
                from gateway.session import build_session_key

                self._lease_session_keys[lease.lease_id] = build_session_key(
                    source,
                    group_sessions_per_user=False,
                )
            except Exception:
                logger.debug("OneBot11 无法解析 lease 对应 session key", exc_info=True)
            event_token = _CURRENT_EVENT.set(event)
            caller_token = _CURRENT_CALLER.set(caller)
            binding_token = _CURRENT_BINDING.set(None)
            try:
                await super().handle_message(event)
                handed_off = True
            finally:
                _CURRENT_BINDING.reset(binding_token)
                _CURRENT_CALLER.reset(caller_token)
                _CURRENT_EVENT.reset(event_token)
        finally:
            if not handed_off:
                self._lease_session_keys.pop(lease.lease_id, None)
                await self._clear_processing_reaction(lease.lease_id)
                self._cleanup_media(
                    media_paths,
                    media_dir=media_dir if has_images else None,
                )

    def _reaction_message_id(self, lease: QueueLease) -> str | None:
        """从持久化触发请求定位真实消息 ID；内部 hash 不能用于 OneBot reaction。"""
        trigger_key = str(lease.trigger.message_key)
        for message in lease.messages:
            if str(message.message_key) == trigger_key:
                message_id = str(message.message_id or "").strip()
                return message_id if is_numeric_message_id(message_id) else None
        return None

    async def _set_processing_reaction(self, lease: QueueLease, *, enabled: bool) -> str | None:
        """按当前群 lease 设置处理指示器；reaction 失败不阻断 Agent turn。"""
        if not self._processing_reaction_enabled:
            return None
        if not self._chat_access_allowed("group", lease.chat_id):
            return None
        message_id = self._reaction_message_id(lease)
        if message_id is None:
            logger.debug("OneBot11 reaction 跳过无真实 message_id 的触发消息: %s", lease.lease_id)
            return None
        if not await asyncio.to_thread(self._queue.is_lease_current, lease):
            logger.info("OneBot11 reaction 跳过已失效 lease: %s", lease.lease_id)
            return None
        try:
            await self._api.set_message_emoji_like(
                message_id,
                self._processing_reaction_emoji_id,
                enabled=enabled,
            )
            return message_id
        except OneBotApiError as exc:
            logger.warning(
                "OneBot11 reaction %s 失败: lease=%s message=%s status=%s",
                "添加" if enabled else "移除",
                lease.lease_id,
                message_id,
                exc.status,
            )
            return message_id if enabled and exc.unknown_outcome else None
        except (OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 reaction %s 失败: lease=%s message=%s error=%s",
                "添加" if enabled else "移除",
                lease.lease_id,
                message_id,
                exc,
            )
            return None

    async def _clear_processing_reaction(self, lease_id: str) -> None:
        """在当前 turn 收尾时尽力移除处理指示器，不改变队列完成结果。"""
        message_id = self._processing_reaction_message_ids.pop(str(lease_id), None)
        if message_id is None:
            return
        # completion 成功后 dispatcher 会先删除内存中的 active lease；清理
        # reaction 不能再依赖 active_by_lease，否则 👀 会残留在触发消息上。
        try:
            await self._api.set_message_emoji_like(
                message_id,
                self._processing_reaction_emoji_id,
                enabled=False,
            )
        except OneBotApiError as exc:
            logger.warning(
                "OneBot11 reaction 移除失败: lease=%s message=%s status=%s",
                lease_id,
                message_id,
                exc.status,
            )
        except (OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 reaction 移除失败: lease=%s message=%s error=%s",
                lease_id,
                message_id,
                exc,
            )

    def _lease_is_current(self, lease_id: str | None) -> bool:
        """检查当前 turn 是否仍持有 queue lease。"""
        if not lease_id or lease_id in self._fenced_leases:
            return False
        try:
            return self._queue.is_lease_current(str(lease_id))
        except Exception:
            logger.warning("OneBot11 lease 检查失败，按失效处理: %s", lease_id, exc_info=True)
            return False

    def _lease_matches_target(self, lease_id: str, chat_type: str, chat_id: str) -> bool:
        """确认 metadata 中的 lease 属于当前目标，阻止跨群换绑。"""
        if str(chat_type) != "group":
            return False
        try:
            status = self._queue.status_for_lease(str(lease_id))
        except Exception:
            logger.warning("OneBot11 lease 目标检查失败，按失效处理: %s", lease_id, exc_info=True)
            return False
        return (
            status.get("chat_type") == "group"
            and status.get("chat_id") == str(chat_id)
            and self._lease_is_current(str(lease_id))
        )

    async def _on_lease_lost(self, lease: QueueLease) -> None:
        """heartbeat fencing 后取消旧 Hermes task，避免它继续调用工具。"""
        self._fenced_leases.add(lease.lease_id)
        session_key = self._lease_session_keys.get(lease.lease_id)
        if session_key:
            try:
                await self.cancel_session_processing(session_key)
            except Exception:
                logger.warning("OneBot11 无法取消失效 lease 的 Hermes task", exc_info=True)

    def _queue_completion_decision(
        self,
        lease_id: str,
        outcome: ProcessingOutcome,
    ) -> tuple[bool, bool, bool, str | None]:
        """根据内存观察和持久阶段决定 ack、release 或 uncertain。"""
        status = self._queue.status_for_lease(lease_id)
        if not status and lease_id not in self._outbound_started:
            return False, False, True, "queue lease 不存在，拒绝确认"
        started = bool(status.get("outbound_started")) or lease_id in self._outbound_started
        unknown = lease_id in self._unknown_leases
        known_failure = lease_id in self._outbound_known_failure
        # 只有每个出站块都拿到明确成功结果，才允许删除队列消息。
        # Hermes SUCCESS 只代表 turn 结束，不代表 OneBot 已收到回复。
        if outcome == ProcessingOutcome.SUCCESS and started and not unknown:
            if lease_id in self._outbound_successful:
                return True, False, False, None
            return False, False, True, "Hermes turn 成功但没有完整的 OneBot 出站成功记录"
        if outcome != ProcessingOutcome.SUCCESS and started:
            unknown = True
        if unknown:
            return False, True, False, "OneBot 出站结果未知或 lease 已发生部分成功"
        if outcome == ProcessingOutcome.SUCCESS and not started:
            return False, False, False, "Hermes turn 成功但没有成功出站，消息保留待重试"
        return False, False, known_failure, "Hermes turn 未成功完成"

    async def _finish_queue_turn(
        self,
        event: Any,
        outcome: ProcessingOutcome,
    ) -> None:
        """在 Hermes background task 真正退出后完成 queue lease。"""
        metadata = getattr(event, "metadata", None) or {}
        lease_id = str(metadata.get("onebot11_lease_id") or "")
        if not lease_id:
            return
        ack = unknown = known_failure = False
        reason: str | None = None
        completed = False
        completion_error: BaseException | None = None
        should_schedule_timer = False
        should_notify = False
        chat_id = ""
        lease_revision = int(metadata.get("onebot11_lease_revision") or 0)
        try:
            ack, unknown, known_failure, reason = self._queue_completion_decision(lease_id, outcome)
            completion_kwargs: dict[str, Any] = {
                "outcome": "success" if ack else "failure",
                "unknown": unknown,
            }
            try:
                parameters = inspect.signature(self._dispatcher.complete).parameters
            except (TypeError, ValueError):
                parameters = {}
            if "known_failure" in parameters:
                completion_kwargs["known_failure"] = known_failure
            if "reason" in parameters:
                completion_kwargs["reason"] = reason
            completed = await self._dispatcher.complete(lease_id, **completion_kwargs)
        except BaseException as exc:
            completion_error = exc
            logger.warning("OneBot11 queue turn 收口失败，等待 lease 恢复: %s", lease_id, exc_info=True)
        finally:
            # reaction 是 best-effort，不能阻断 binding、媒体和内存状态释放。
            try:
                await self._clear_processing_reaction(lease_id)
            except Exception:
                logger.warning("OneBot11 reaction 收尾失败: %s", lease_id, exc_info=True)
        post_completion_error: BaseException | None = None
        try:
            raw_target = metadata.get("onebot11_target")
            chat_id = str(raw_target.get("chat_id") or "") if isinstance(raw_target, Mapping) else ""
            if not chat_id:
                chat_id = str(getattr(getattr(event, "source", None), "chat_id", "") or "")
            if completion_error is None and chat_id in self.trigger_config.llm_allowed_groups and self.trigger_config.llm_enabled:
                async with self._trigger_lock_for(chat_id):
                    status = await asyncio.to_thread(self._queue.status, chat_id)
                    state = self._trigger_state_for(chat_id)
                    previous_mode = state.mode
                    revision_changed = (
                        lease_revision > 0
                        and int(status.get("revision", 0)) != lease_revision
                    )
                    preserve_pending = revision_changed or int(status.get("pending", 0)) > 0
                    has_hard_trigger = int(status.get("pending_trigger_requests", 0)) > 0
                    successful_turn = bool(completed and ack and not unknown)
                    if status.get("paused"):
                        state.pause()
                    else:
                        state.on_turn_complete(
                            success=successful_turn,
                            now=time.monotonic(),
                            preserve_pending=preserve_pending,
                            has_hard_trigger=has_hard_trigger,
                        )
                        # Agent turn 期间收到的普通消息当时处于 idle，不会在入队时
                        # 创建候选 timer。旧 turn 成功收口后，它们应被视为 engaged
                        # 窗口内的新消息，并重新走一次 trailing debounce。
                        if (
                            successful_turn
                            and preserve_pending
                            and not has_hard_trigger
                            and previous_mode not in {"debounce", "judging", "waiting"}
                            and state.mode == "engaged"
                        ):
                            pending_messages = await asyncio.to_thread(
                                self._queue.peek, chat_id
                            )
                            if pending_messages:
                                latest = pending_messages[-1]
                                followup_action = state.observe_message(
                                    chat_type="group",
                                    text=latest.text,
                                    mentioned_self=bool(
                                        latest.metadata.get("onebot11_mentioned_self")
                                    ),
                                    has_context=bool(
                                        status.get("summary") or len(pending_messages) > 1
                                    ),
                                    revision=int(status.get("revision", 0)),
                                    now=time.monotonic(),
                                    last_trigger_at=self._last_trigger_at.get(chat_id),
                                )
                                if followup_action.kind == "schedule":
                                    await self._apply_trigger_action_locked(
                                        chat_id, followup_action
                                    )
                        should_schedule_timer = (
                            state.debounce_due is not None
                            or state.wait_until is not None
                            or state.engaged_until is not None
                        )
                    should_notify = (
                        completed
                        and ack
                        and not unknown
                        and not status.get("paused")
                        and has_hard_trigger
                    )
            elif completion_error is None:
                should_notify = bool(completed and ack and not unknown)
            if should_schedule_timer and not self._closed:
                self._schedule_trigger_timer(chat_id)
            if should_notify and not self._closed:
                await self._dispatcher.notify(chat_id)
        except BaseException as exc:
            post_completion_error = exc
            logger.warning(
                "OneBot11 turn 后续状态更新失败，仍继续清理资源: %s",
                lease_id,
                exc_info=True,
            )
        finally:
            self._unknown_leases.discard(lease_id)
            self._outbound_started.discard(lease_id)
            self._outbound_successful.discard(lease_id)
            self._outbound_known_failure.discard(lease_id)
            self._lease_session_keys.pop(lease_id, None)
            for binding_key, binding in self._bindings.snapshot().items():
                if binding.lease_id == lease_id:
                    self._bindings.discard(*binding_key)
            self._cleanup_media(
                metadata.get("onebot11_media_paths"),
                media_dir=metadata.get("onebot11_media_dir"),
            )
            if completion_error is not None:
                raise completion_error
            if post_completion_error is not None:
                raise post_completion_error

    async def _process_message_background(self, event: MessageEvent, session_key: str) -> None:
        """包装 Hermes background task，确保错误通知发送后才推进下一轮。"""
        metadata = event.metadata or {}
        deferred = bool(metadata.get("onebot11_defer_completion"))
        event_token = _CURRENT_EVENT.set(event)
        caller_token = _CURRENT_CALLER.set(
            _caller_from_metadata(metadata.get("onebot11_caller_context"))
        )
        binding_token = _CURRENT_BINDING.set(None)
        try:
            await super()._process_message_background(event, session_key)
        except asyncio.CancelledError:
            if deferred and str(metadata.get("onebot11_lease_id") or "") not in self._pending_completions:
                self._pending_completions[str(metadata.get("onebot11_lease_id") or "")] = (
                    ProcessingOutcome.CANCELLED,
                    False,
                    False,
                    "Hermes task cancelled",
                )
            raise
        finally:
            if deferred:
                lease_id = str(metadata.get("onebot11_lease_id") or "")
                try:
                    pending = self._pending_completions.pop(lease_id, None)
                    completion_outcome = pending[0] if pending else ProcessingOutcome.FAILURE
                    await self._finish_queue_turn(event, completion_outcome)
                finally:
                    if metadata.get("onebot11_managed_context"):
                        self._discard_event_binding(metadata)
                        _CURRENT_BINDING.set(None)
                        _CURRENT_CALLER.set(None)
            elif metadata.get("onebot11_managed_context"):
                self._discard_event_binding(metadata)
                _CURRENT_BINDING.set(None)
                _CURRENT_CALLER.set(None)
            _CURRENT_BINDING.reset(binding_token)
            _CURRENT_CALLER.reset(caller_token)
            _CURRENT_EVENT.reset(event_token)

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        """按真实 Hermes/QQ 出站结果完成 queue lease。"""
        metadata = event.metadata or {}
        lease_id = str(metadata.get("onebot11_lease_id") or "")
        managed_context = bool(metadata.get("onebot11_managed_context"))
        deferred = bool(metadata.get("onebot11_defer_completion"))
        try:
            if lease_id:
                if deferred:
                    self._pending_completions[lease_id] = (
                        outcome,
                        lease_id in self._unknown_leases,
                        lease_id in self._outbound_known_failure,
                        None,
                    )
                else:
                    await self._finish_queue_turn(event, outcome)
        finally:
            # DM 没有 queue wrapper，必须按 event 的精确 binding key 收尾；
            # 非 deferred 的测试/兼容路径也不能因 completion 异常留下身份。
            if not deferred:
                self._discard_event_binding(metadata)
                binding = _CURRENT_BINDING.get()
                if binding is not None and not lease_id:
                    self._bindings.discard_if_matches(binding)
            if not managed_context:
                _CURRENT_BINDING.set(None)
                _CURRENT_CALLER.set(None)
            if not deferred:
                self._cleanup_media(
                    metadata.get("onebot11_media_paths")
                    or getattr(event, "media_urls", []),
                    media_dir=metadata.get("onebot11_media_dir"),
                )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        """显式解析 ChatTarget 后发送，并记录部分/未知出站结果。"""
        binding = self._binding_from_context()
        lease_id = binding.lease_id if binding else None
        if self._closed:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="OneBot11 adapter is closed", error_kind="not_found")
        if lease_id and not self._lease_is_current(lease_id):
            self._fenced_leases.add(lease_id)
            self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="OneBot11 lease 已失效，拒绝出站", error_kind="fenced")
        target = self._resolve_target(str(chat_id), metadata)
        if target is None:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="OneBot11 target unknown or ambiguous", error_kind="unknown")
        caller_user_id = (
            binding.caller.user_id
            if binding is not None
            else target.chat_id
            if target.chat_type == "dm"
            else None
        )
        if not self._chat_access_allowed(target.chat_type, target.chat_id, caller_user_id):
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="OneBot11 target 不再满足访问策略", error_kind="permission")
        if self._ws is None:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="Not connected", error_kind="not_found")
        pieces = chunk_text(content, self.max_message_length_for_chat(target.chat_id))
        if not pieces and content:
            pieces = [content]
        sent: list[str] = []
        for piece in pieces:
            if lease_id:
                marked = await asyncio.to_thread(self._queue.mark_outbound_started, lease_id)
                if not marked:
                    self._fenced_leases.add(lease_id)
                    self._outbound_known_failure.add(lease_id)
                    return SendResult(
                        False,
                        message_id=sent[-1] if sent else None,
                        error="OneBot11 lease 已失效，拒绝出站",
                        raw_response={"sent_chunks": len(sent), "total_chunks": len(pieces)},
                        error_kind="fenced",
                    )
                self._outbound_started.add(lease_id)
            try:
                sent_id = await self._api.send_message(
                    target.chat_id, piece, chat_type=target.chat_type, reply_to=reply_to
                )
                if not sent_id:
                    if lease_id:
                        self._unknown_leases.add(lease_id)
                    return SendResult(
                        False,
                        message_id=sent[-1] if sent else None,
                        error="OneBot 成功响应缺少 message_id，出站结果未知",
                        raw_response={"sent_chunks": len(sent), "total_chunks": len(pieces)},
                        error_kind="unknown",
                    )
                if lease_id:
                    self._outbound_successful.add(lease_id)
                sent.append(sent_id)
            except OneBotApiError as exc:
                if lease_id:
                    if exc.unknown_outcome or sent:
                        self._unknown_leases.add(lease_id)
                    else:
                        self._outbound_known_failure.add(lease_id)
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error=str(exc),
                    raw_response={"sent_chunks": len(sent), "total_chunks": len(pieces)},
                    error_kind="unknown" if exc.unknown_outcome else exc.error_kind,
                )
            except ValueError as exc:
                if lease_id:
                    self._outbound_known_failure.add(lease_id)
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error=str(exc),
                    raw_response={"sent_chunks": len(sent), "total_chunks": len(pieces)},
                    error_kind="failed",
                )
        return SendResult(
            True,
            message_id=sent[-1] if sent else str(uuid.uuid4()),
            raw_response={"sent_chunks": len(sent), "total_chunks": len(pieces)},
        )

    async def _send_with_retry(self, chat_id: str, content: str, reply_to: str | None = None, metadata: Any = None, **kwargs: Any) -> SendResult:
        """覆盖 Hermes 默认重试/fallback，避免未知出站重复发送。"""
        del kwargs
        return await self.send(chat_id, content, reply_to=reply_to, metadata=metadata if isinstance(metadata, Mapping) else None)

    async def send_typing(self, chat_id: str, metadata: dict | None = None) -> None:
        """OneBot 无 typing 指示器。"""
        del chat_id, metadata

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """返回当前已知目标信息，未知 ID 不猜测类型。"""
        target = self._resolve_target(str(chat_id), None)
        if target is None or not self._chat_access_allowed(
            target.chat_type,
            target.chat_id,
            target.chat_id if target.chat_type == "dm" else None,
        ):
            raise ValueError("OneBot11 target unknown or ambiguous")
        return {"name": target.chat_id, "type": target.chat_type}

    def _resolve_target(self, chat_id: str, metadata: Any) -> ChatTarget | None:
        """按当前 turn binding、显式 metadata 或唯一登记解析目标。"""
        candidate: ChatTarget | None = None
        trusted_target = False
        if isinstance(metadata, Mapping):
            raw = metadata.get("onebot11_target") or metadata.get("target")
            trusted_target = metadata.get("onebot11_trusted_target") is True
            if isinstance(raw, Mapping):
                try:
                    candidate = ChatTarget(str(raw["chat_type"]), str(raw["chat_id"]))
                except (KeyError, TypeError, ValueError):
                    return None
        binding = self._binding_from_context()
        if binding is not None:
            bound_target = binding.caller.target()
            if bound_target.chat_id != chat_id:
                return None
            if candidate is not None and candidate != bound_target:
                return None
            return bound_target
        registered = self._targets.get(chat_id)
        if candidate is not None:
            if candidate.chat_id != chat_id:
                return None
            if chat_id in self._ambiguous_targets and not trusted_target:
                return None
            if registered is not None and registered != candidate and not trusted_target:
                return None
            legacy_type = self._chat_types.get(chat_id)
            if chat_id not in self._ambiguous_targets:
                if registered is None and legacy_type not in {"group", "dm"}:
                    return None
                if legacy_type in {"group", "dm"} and legacy_type != candidate.chat_type:
                    return None
            return candidate
        if chat_id in self._ambiguous_targets:
            return None
        if registered is not None and registered.chat_id == chat_id:
            return registered
        legacy_type = self._chat_types.get(chat_id)
        if legacy_type in {"group", "dm"}:
            return ChatTarget(legacy_type, chat_id)
        return None

    def _binding_from_context(self) -> TurnBinding | None:
        """读取当前异步 turn 的精确 binding。"""
        binding = _CURRENT_BINDING.get()
        if binding is not None:
            caller = _CURRENT_CALLER.get()
            if caller is None or caller == binding.caller:
                return binding
            return None
        caller = _CURRENT_CALLER.get()
        if caller is None:
            return None
        bindings = self._bindings.snapshot()
        if caller.lease_id:
            return next((binding for binding in bindings.values() if binding.lease_id == caller.lease_id), None)
        return next((binding for binding in bindings.values() if binding.caller == caller), None)

    def _resolve_binding(self, session_id: str | None, turn_id: str | None) -> TurnBinding | None:
        """按完整 Hermes 路由键读取 caller，不使用最近来源缓存。"""
        return self._bindings.get(session_id, turn_id)

    def _discard_event_binding(self, metadata: Mapping[str, Any]) -> TurnBinding | None:
        """按 event 写入的精确 session/turn key 清理 binding。"""
        raw_key = metadata.get("onebot11_binding_key")
        if not isinstance(raw_key, Mapping):
            return None
        session_id = str(raw_key.get("session_id") or "").strip()
        turn_id = str(raw_key.get("turn_id") or "").strip()
        if not session_id or not turn_id:
            return None
        binding = self._bindings.get(session_id, turn_id)
        self._bindings.discard_if_matches(binding)
        return binding

    def _make_tool_handler(self, tool_name: str):
        """包装工具 handler，执行角色/作用域硬校验和写操作确认。"""

        async def wrapped(args: dict[str, Any], **kwargs: Any) -> str:
            session_id = kwargs.get("session_id")
            turn_id = kwargs.get("turn_id")
            binding = self._resolve_binding(session_id, turn_id) if turn_id else self._binding_from_context()
            if binding is not None and session_id and binding.session_id != str(session_id):
                binding = None
            if binding is not None and turn_id and binding.turn_id != str(turn_id):
                binding = None
            if binding is None:
                return json.dumps({"status": "permission_error", "error": "当前 turn 身份绑定不存在"}, ensure_ascii=False)
            if binding.lease_id and not self._lease_is_current(binding.lease_id):
                self._audit.record(
                    "permission_denied",
                    {
                        "tool": tool_name,
                        "user_id": binding.caller.user_id,
                        "chat_type": binding.caller.chat_type,
                        "chat_id": binding.caller.chat_id,
                        "reason": "lease 已失效",
                    },
                )
                return json.dumps({"status": "permission_error", "error": "当前 turn lease 已失效"}, ensure_ascii=False)
            caller = binding.caller
            if not self._chat_access_allowed(caller.chat_type, caller.chat_id, caller.user_id):
                self._audit.record(
                    "permission_denied",
                    {
                        "tool": tool_name,
                        "user_id": caller.user_id,
                        "chat_type": caller.chat_type,
                        "chat_id": caller.chat_id,
                        "reason": "当前目标不再满足访问策略",
                    },
                )
                return json.dumps(
                    {"status": "permission_error", "error": "当前目标不再满足访问策略"},
                    ensure_ascii=False,
                )
            error = validate_tool_call(tool_name, args, caller, self.super_admins)
            if error:
                self._audit.record(
                    "permission_denied",
                    {
                        "tool": tool_name,
                        "user_id": caller.user_id,
                        "chat_type": caller.chat_type,
                        "chat_id": caller.chat_id,
                        "reason": error,
                    },
                )
                return json.dumps({"status": "permission_error", "error": error}, ensure_ascii=False)
            try:
                if tool_name in WRITE_TOOL_NAMES:
                    confirmation = self._confirmations.issue(
                        tool_name,
                        args,
                        user_id=caller.user_id,
                        chat_type=caller.chat_type,
                        chat_id=caller.chat_id,
                    )
                    self._audit.record("preview", {"tool": tool_name, "user_id": caller.user_id, "chat_type": caller.chat_type, "chat_id": caller.chat_id, "params": {key: str(value)[:128] for key, value in args.items()}})
                    return json.dumps({"status": "confirmation_required", "command": f"/onebot confirm {confirmation.token}", "tool": tool_name}, ensure_ascii=False)
                if binding.lease_id and not self._lease_is_current(binding.lease_id):
                    self._audit.record(
                        "permission_denied",
                        {
                            "tool": tool_name,
                            "user_id": caller.user_id,
                            "chat_type": caller.chat_type,
                            "chat_id": caller.chat_id,
                            "reason": "lease 在访问 OneBot API 前失效",
                        },
                    )
                    return json.dumps(
                        {"status": "permission_error", "error": "当前 turn lease 已失效"},
                        ensure_ascii=False,
                    )
                result = await _TOOL_HANDLERS[tool_name](self._api, args, caller)
                return json.dumps(result, ensure_ascii=False, default=str)
            except OneBotApiError as exc:
                if exc.unknown_outcome and binding.lease_id:
                    self._unknown_leases.add(binding.lease_id)
                return json.dumps({"status": "unknown" if exc.unknown_outcome else "error", "error": str(exc)}, ensure_ascii=False)
            except (KeyError, TypeError, ValueError) as exc:
                return json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False)

        return wrapped

    def _operation_fingerprint(
        self,
        tool_name: str,
        params: Mapping[str, Any],
        *,
        chat_type: str = "",
        chat_id: str = "",
    ) -> str:
        """生成带目标范围且不含凭据的管理动作指纹。"""
        payload = json.dumps(
            {
                "target": {"chat_type": str(chat_type), "chat_id": str(chat_id)},
                "tool": tool_name,
                "params": dict(params),
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    async def _execute_confirmed(self, confirmation: Any) -> dict[str, Any]:
        """执行已经由直接管理命令消费的确认令牌。"""
        caller = CallerContext(
            user_id=confirmation.user_id,
            chat_type=confirmation.chat_type,
            chat_id=confirmation.chat_id,
            role="super_admin",
            allowed_tools=self.role_tools["super_admin"],
            self_id=self.self_id,
        )
        if confirmation.tool_name not in WRITE_TOOL_NAMES:
            self._audit.record(
                "permission_denied",
                {
                    "tool": confirmation.tool_name,
                    "user_id": confirmation.user_id,
                    "chat_type": confirmation.chat_type,
                    "chat_id": confirmation.chat_id,
                    "reason": "confirmation is not a write tool",
                },
            )
            return {"status": "permission_error", "error": "确认令牌不是群管理写工具"}
        authorization_error = validate_tool_call(
            confirmation.tool_name,
            confirmation.params,
            caller,
            self.super_admins,
        )
        if authorization_error:
            self._audit.record(
                "permission_denied",
                {
                    "tool": confirmation.tool_name,
                    "user_id": confirmation.user_id,
                    "chat_type": confirmation.chat_type,
                    "chat_id": confirmation.chat_id,
                    "reason": authorization_error,
                },
            )
            return {"status": "permission_error", "error": authorization_error}
        fingerprint = self._operation_fingerprint(
            confirmation.tool_name,
            confirmation.params,
            chat_type=confirmation.chat_type,
            chat_id=confirmation.chat_id,
        )
        if fingerprint in self._unknown_operations:
            self._audit.record(
                "unknown_blocked",
                {
                    "tool": confirmation.tool_name,
                    "user_id": confirmation.user_id,
                    "chat_type": confirmation.chat_type,
                    "chat_id": confirmation.chat_id,
                    "operation": fingerprint,
                },
            )
            return {"status": "unknown", "error": "同一管理动作已有未知结果，禁止重复执行"}
        if caller.user_id not in self.super_admins or not self._chat_access_allowed(
            caller.chat_type, caller.chat_id, caller.user_id
        ):
            self._audit.record(
                "permission_denied",
                {
                    "tool": confirmation.tool_name,
                    "user_id": confirmation.user_id,
                    "chat_type": confirmation.chat_type,
                    "chat_id": confirmation.chat_id,
                    "reason": "操作者或目标已不再授权",
                },
            )
            return {"status": "permission_error", "error": "当前操作者或目标已不再授权"}
        try:
            result = await handle_write_action(self._api, confirmation.tool_name, dict(confirmation.params), caller)
        except OneBotApiError as exc:
            result = {"status": "unknown" if exc.unknown_outcome else "error", "error": str(exc)}
            if exc.unknown_outcome:
                self._unknown_operations.add(fingerprint)
                self._audit.record(
                    "unknown",
                    {
                        "tool": confirmation.tool_name,
                        "user_id": confirmation.user_id,
                        "chat_type": confirmation.chat_type,
                        "chat_id": confirmation.chat_id,
                        "operation": fingerprint,
                    },
                )
        if result.get("status") == "unknown":
            self._unknown_operations.add(fingerprint)
        self._audit.record("execute", {"tool": confirmation.tool_name, "user_id": caller.user_id, "chat_type": caller.chat_type, "chat_id": caller.chat_id, "status": result.get("status")})
        return result

    async def _handle_admin_command(self, event: _proto.events.InboundEvent) -> None:
        """在 adapter 入站层直接处理 OneBot 管理命令。"""
        if event.user_id not in self.super_admins:
            await self._send_direct(event, "仅超级管理员可执行 OneBot 管理命令")
            return
        if not self._chat_access_allowed(event.chat_type, event.chat_id, event.user_id):
            self._audit.record(
                "access_denied",
                {
                    "chat_type": event.chat_type,
                    "chat_id": event.chat_id,
                    "user_id": event.user_id,
                    "reason": "admin command target is not authorized",
                },
            )
            await self._send_direct(event, "当前目标不再满足 OneBot11 访问策略")
            return
        parts = event.text.strip().split(maxsplit=2)
        command = parts[1].casefold() if len(parts) > 1 else "status"
        chat_id = event.chat_id
        self._audit.record(
            "admin_command",
            {"command": command, "chat_type": event.chat_type, "chat_id": chat_id, "user_id": event.user_id},
        )
        try:
            if command == "confirm" and len(parts) >= 3:
                confirmation = self._confirmations.consume(parts[2], user_id=event.user_id, chat_type=event.chat_type, chat_id=chat_id)
                if confirmation is None:
                    self._audit.record(
                        "permission_denied",
                        {
                            "user_id": event.user_id,
                            "chat_type": event.chat_type,
                            "chat_id": chat_id,
                            "reason": "确认令牌无效、过期或目标不匹配",
                        },
                    )
                    await self._send_direct(event, "确认令牌无效、已过期或目标不匹配")
                    return
                await self._send_direct(event, json.dumps(await self._execute_confirmed(confirmation), ensure_ascii=False))
            elif command in {"status", "queue"}:
                if event.chat_type != "group":
                    await self._send_direct(event, "status/queue 只能作用于当前群队列")
                    return
                status = await asyncio.to_thread(self._queue.status, chat_id)
                trigger_state = self._trigger_states.get(chat_id)
                status["trigger"] = (
                    trigger_state.snapshot()
                    if trigger_state is not None
                    else {"mode": "idle", "llm_calls": 0, "llm_failures": 0}
                )
                await self._send_direct(event, json.dumps(status, ensure_ascii=False))
            elif command == "flush":
                if event.chat_type != "group":
                    await self._send_direct(event, "flush 只能作用于当前群队列")
                    return
                has_request, started, paused = await self._flush_group(
                    chat_id,
                    caller_user_id=event.user_id,
                    caller_user_name=event.user_name,
                )
                if not has_request:
                    await self._send_direct(event, "当前群没有可 flush 的待处理消息")
                    return
                if paused:
                    await self._send_direct(event, "flush 请求已保留；当前群处于暂停状态")
                    return
                await self._send_direct(
                    event,
                    f"flush: {'started' if started else '没有可 dispatch 的触发请求'}",
                )
            elif command == "clear":
                if event.chat_type != "group":
                    await self._send_direct(event, "clear 只能作用于当前群队列")
                    return
                count = await self._clear_group(chat_id)
                await self._send_direct(event, f"已清理 {count} 条待处理消息；Hermes session 历史未删除")
            elif command in {"pause", "resume"}:
                if event.chat_type != "group":
                    await self._send_direct(event, "pause/resume 只能作用于当前群队列")
                    return
                if not await self._set_group_paused(chat_id, command == "pause"):
                    await self._send_direct(event, "当前群不再满足 OneBot11 访问策略")
                    return
                await self._send_direct(event, f"群 {chat_id} 已{'暂停' if command == 'pause' else '恢复'}自动 dispatch")
            elif command == "resolve" and len(parts) >= 3 and parts[2] in {"retry", "discard"}:
                if event.chat_type != "group":
                    await self._send_direct(event, "resolve 只能作用于当前群队列")
                    return
                count = await asyncio.to_thread(self._queue.resolve_uncertain, chat_id, parts[2])
                if parts[2] == "retry":
                    status = await asyncio.to_thread(self._queue.status, chat_id)
                    if count and int(status.get("pending_trigger_requests", 0)) == 0:
                        await asyncio.to_thread(
                            self._queue.create_trigger,
                            chat_id,
                            "admin_resolve_retry",
                            event.user_id,
                            event.user_name,
                        )
                    await self._dispatcher.notify(chat_id)
                self._audit.record(
                    "admin_resolve",
                    {"chat_type": event.chat_type, "chat_id": chat_id, "user_id": event.user_id, "action": parts[2], "count": count},
                )
                await self._send_direct(event, f"已处理 uncertain/failed 消息 {count} 条: {parts[2]}（retry 可能重复执行）")
            else:
                await self._send_direct(event, "用法: /onebot status|queue|flush|clear|pause|resume|resolve retry|resolve discard|confirm TOKEN")
        except Exception as exc:
            logger.warning("OneBot11 管理命令失败", exc_info=True)
            await self._send_direct(event, f"命令失败: {type(exc).__name__}: {str(exc)[:200]}")

    async def _send_direct(self, event: _proto.events.InboundEvent, text: str) -> None:
        """向入站事件的明确目标发送管理命令结果。"""
        if event.chat_id not in self._ambiguous_targets:
            self._targets[event.chat_id] = ChatTarget(event.chat_type, event.chat_id)
        self._chat_types[event.chat_id] = event.chat_type
        await self.send(
            event.chat_id,
            text,
            reply_to=event.message_id,
            metadata={
                "onebot11_target": {"chat_type": event.chat_type, "chat_id": event.chat_id},
                "onebot11_trusted_target": True,
            },
        )

    def _cleanup_media(self, paths: Any = None, *, media_dir: Any = None) -> None:
        """删除已完成 turn 的媒体，并清理过期的跨进程孤儿目录。"""
        root = self._media_root
        candidates = list(paths) if isinstance(paths, (list, tuple, set)) else []
        directories: set[Path] = set()
        if media_dir:
            directories.add(Path(str(media_dir)))
        for raw_path in candidates:
            path = Path(str(raw_path))
            try:
                resolved = path.resolve(strict=False)
                if path.is_file() and resolved != root and resolved.is_relative_to(root):
                    path.unlink()
                    if path.parent.name.startswith(self._media_prefix):
                        directories.add(path.parent)
            except OSError:
                logger.debug("媒体临时文件清理失败: %s", path, exc_info=True)
        for directory in directories:
            try:
                resolved = directory.resolve(strict=False)
                if (
                    directory.name.startswith(self._media_prefix)
                    and resolved != root
                    and resolved.is_relative_to(root)
                    and directory != Path(self._media_dir)
                    and directory.exists()
                ):
                    shutil.rmtree(directory)
            except OSError:
                logger.debug("媒体 turn 目录清理失败: %s", directory, exc_info=True)
        self._cleanup_orphan_media_dirs()
        try:
            current = Path(self._media_dir)
            if current.is_dir() and not any(current.iterdir()):
                current.rmdir()
        except OSError:
            logger.debug("媒体 turn 目录清理失败", exc_info=True)

    def _cleanup_orphan_media_dirs(self) -> None:
        """删除受控媒体根下超过 TTL 的旧 turn 目录。"""
        now = time.time()
        try:
            for path in self._media_root.iterdir():
                if (
                    not path.is_dir()
                    or path.is_symlink()
                    or not path.name.startswith(self._media_prefix)
                    or path == Path(self._media_dir)
                ):
                    continue
                if now - path.stat().st_mtime <= self._media_orphan_ttl:
                    continue
                shutil.rmtree(path)
        except OSError:
            logger.debug("媒体孤儿目录清理失败", exc_info=True)


def _pre_gateway_dispatch_hook(event: Any, **kwargs: Any) -> None:
    """从当前 MessageEvent 建立 caller ContextVar，供同一 turn 继承。"""
    del kwargs
    _CURRENT_EVENT.set(event)
    source = getattr(event, "source", None)
    if _platform_value(getattr(source, "platform", None)) != _PLATFORM_NAME:
        _CURRENT_EVENT.set(None)
        _CURRENT_CALLER.set(None)
        _CURRENT_BINDING.set(None)
        return
    caller = _caller_from_metadata((getattr(event, "metadata", None) or {}).get("onebot11_caller_context"))
    _CURRENT_CALLER.set(caller)
    _CURRENT_BINDING.set(None)


def _clear_current_turn_binding(adapter: Any) -> None:
    """尽力清理当前 turn 的身份绑定，并清空本地 ContextVar。"""
    if adapter is not None:
        metadata = getattr(_CURRENT_EVENT.get(), "metadata", None) or {}
        discard_event_binding = getattr(adapter, "_discard_event_binding", None)
        if callable(discard_event_binding):
            discard_event_binding(metadata)
        bindings = getattr(adapter, "_bindings", None)
        discard_if_matches = getattr(bindings, "discard_if_matches", None)
        if callable(discard_if_matches):
            discard_if_matches(_CURRENT_BINDING.get())
    _CURRENT_EVENT.set(None)
    _CURRENT_CALLER.set(None)
    _CURRENT_BINDING.set(None)


def _pre_llm_call_hook(session_id: str = "", turn_id: str = "", platform: Any = "", **kwargs: Any) -> dict[str, str] | None:
    """绑定当前 Hermes turn 的 caller，并注入角色/工具提示。"""
    del kwargs
    platform_value = _platform_value(platform)
    caller = _CURRENT_CALLER.get()
    if platform_value and platform_value != _PLATFORM_NAME:
        return None
    if not platform_value and caller is None:
        return None
    adapter = _get_live_adapter()
    if adapter is None or caller is None:
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}
    if not adapter._chat_access_allowed(caller.chat_type, caller.chat_id, caller.user_id):
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller is no longer authorized; all OneBot11 tools must be denied."}
    if caller.lease_id and not adapter._lease_is_current(caller.lease_id):
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller lease unavailable; all OneBot11 tools must be denied."}
    if caller.lease_id and not adapter._lease_matches_target(
        caller.lease_id, caller.chat_type, caller.chat_id
    ):
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller lease target mismatch; all OneBot11 tools must be denied."}
    normalized_session_id = str(session_id or "").strip()
    normalized_turn_id = str(turn_id or "").strip()
    if not normalized_session_id or not normalized_turn_id:
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}
    binding = TurnBinding(normalized_session_id, normalized_turn_id, caller, caller.lease_id)
    try:
        adapter._bindings.bind(binding)
    except ValueError:
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller turn binding conflict; all OneBot11 tools must be denied."}
    _CURRENT_BINDING.set(binding)
    current_event = _CURRENT_EVENT.get()
    if current_event is not None:
        metadata = dict(getattr(current_event, "metadata", None) or {})
        metadata["onebot11_binding_key"] = {
            "session_id": normalized_session_id,
            "turn_id": normalized_turn_id,
        }
        current_event.metadata = metadata
    return {"context": role_prompt(caller)}


def _pre_tool_call_hook(tool_name: str = "", session_id: str = "", turn_id: str = "", args: dict | None = None, **kwargs: Any) -> dict[str, str] | None:
    """在 Hermes 工具执行前硬拦截越权调用。"""
    del kwargs
    if tool_name not in ALL_TOOLS and not str(tool_name).startswith("qq_"):
        return None
    adapter = _get_live_adapter()
    if adapter is None:
        return {"action": "block", "message": "OneBot11 adapter unavailable"}
    if tool_name not in ALL_TOOLS:
        return {"action": "block", "message": "权限错误: 未知 OneBot11 工具"}
    binding = adapter._resolve_binding(session_id, turn_id)
    if binding is None:
        return {"action": "block", "message": "OneBot11 current turn binding unavailable"}
    if binding.lease_id and not adapter._lease_is_current(binding.lease_id):
        adapter._audit.record(
            "permission_denied",
            {
                "tool": tool_name,
                "user_id": binding.caller.user_id,
                "chat_type": binding.caller.chat_type,
                "chat_id": binding.caller.chat_id,
                "reason": "lease 已失效",
            },
        )
        return {"action": "block", "message": "权限错误: 当前 turn lease 已失效"}
    if not adapter._chat_access_allowed(
        binding.caller.chat_type, binding.caller.chat_id, binding.caller.user_id
    ):
        adapter._audit.record(
            "permission_denied",
            {
                "tool": tool_name,
                "user_id": binding.caller.user_id,
                "chat_type": binding.caller.chat_type,
                "chat_id": binding.caller.chat_id,
                "reason": "当前目标不再满足访问策略",
            },
        )
        return {"action": "block", "message": "权限错误: 当前目标不再满足访问策略"}
    error = validate_tool_call(tool_name, args or {}, binding.caller, adapter.super_admins)
    if error:
        adapter._audit.record("permission_denied", {"tool": tool_name, "user_id": binding.caller.user_id, "chat_type": binding.caller.chat_type, "chat_id": binding.caller.chat_id, "reason": error})
        return {"action": "block", "message": f"权限错误: {error}"}
    return None


def _post_llm_call_hook(**kwargs: Any) -> None:
    """观察 turn 结束；不确认队列，因为此时 QQ 出站尚未必成功。"""
    del kwargs


def check_requirements() -> bool:
    """只检查插件运行依赖；部署配置由 validate_config 读取 YAML 或环境变量。"""
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config: Any) -> bool:
    """验证平台配置和 OneBot session/access 合同。"""
    extra = getattr(config, "extra", {}) or {}
    if not isinstance(extra, Mapping):
        return False
    try:
        effective = _effective_extra(extra)
        http_api = str(effective.get("http_api") or "").strip()
        self_id = str(effective.get("self_id") or "").strip()
        if not http_api:
            return False
        parse_http_base_url(http_api)
        if not self_id:
            return False
        if str(effective.get("session_mode", "shared")).casefold() != "shared":
            return False
        if parse_bool(
            effective.get("group_sessions_per_user"),
            default=False,
            name="group_sessions_per_user",
        ):
            return False
        ws_port = int(effective.get("ws_port", 18880))
        if not 0 <= ws_port <= 65535:
            return False
        ws_host = str(effective.get("ws_host") or "127.0.0.1").strip()
        token = str(effective.get("access_token") or "").strip()
        if ws_host not in {"127.0.0.1", "::1", "localhost"} and not token:
            return False
        if not _is_loopback_url(http_api) and not token:
            return False
        build_access_policy(effective, os.environ)
        build_role_tools(effective)
        build_trigger_config(effective)
        parse_bool(effective.get("processing_reaction_enabled"), default=True, name="processing_reaction_enabled")
        if not str(effective.get("processing_reaction_emoji_id", _PROCESSING_REACTION_EMOJI_ID)).strip():
            return False
        for name, default in (
            ("queue_max_messages", 1000),
            ("queue_max_bytes", 2_000_000),
            ("queue_max_message_bytes", 32_000),
            ("queue_max_original_bytes", 8_000),
            ("queue_max_summary_bytes", 16_000),
            ("queue_recent_originals", 3),
            ("queue_max_attempts", 3),
            ("max_images_per_message", 4),
            ("max_image_bytes", 8_000_000),
            ("max_image_total_bytes", 16_000_000),
            ("max_image_redirects", 3),
        ):
            value = int(effective.get(name, default))
            if value < 0 or (name not in {"queue_recent_originals", "max_image_redirects"} and value == 0):
                return False
        for port in parse_id_list(effective.get("media_allowed_ports")):
            if not 1 <= int(port) <= 65535:
                return False
        home_type = effective.get("home_channel_type")
        if home_type is not None and str(home_type) not in {"group", "dm"}:
            return False
        return True
    except (TypeError, ValueError):
        return False


def _apply_yaml_config(yaml_config: dict[str, Any], platform_config: dict[str, Any]) -> dict[str, Any]:
    """把 OneBot 的 shared session 默认值桥接到 Hermes adapter 配置。"""
    platform_extra = platform_config.get("extra")
    if isinstance(platform_extra, Mapping) and "group_sessions_per_user" in platform_extra:
        return {"group_sessions_per_user": platform_extra["group_sessions_per_user"]}
    if "group_sessions_per_user" in platform_config:
        return {"group_sessions_per_user": platform_config["group_sessions_per_user"]}
    if "group_sessions_per_user" in yaml_config:
        return {"group_sessions_per_user": yaml_config["group_sessions_per_user"]}
    return {"group_sessions_per_user": False}


def _env_enablement() -> dict[str, Any] | None:
    """从环境变量生成平台 extra，环境变量优先于 YAML。"""
    http_api = os.getenv("ONEBOT11_HTTP_API", "").strip()
    self_id = os.getenv("ONEBOT11_SELF_ID", "").strip()
    if not (http_api and self_id):
        return None
    seed: dict[str, Any] = {"http_api": http_api, "self_id": self_id, "session_mode": "shared", "group_sessions_per_user": False}
    for key in (
        "ONEBOT11_ACCESS_TOKEN", "ONEBOT11_WS_PORT", "ONEBOT11_WS_HOST", "ONEBOT11_DM_POLICY",
        "ONEBOT11_ALLOWED_USERS", "ONEBOT11_ALLOWED_GROUPS", "ONEBOT11_REQUIRE_MENTION",
        "ONEBOT11_SUPER_ADMINS", "ONEBOT11_ADMINS", "ONEBOT11_QUEUE_DB",
    ):
        value = os.getenv(key, "").strip()
        if value:
            seed[key.removeprefix("ONEBOT11_").lower()] = value
    return seed


async def _standalone_send(pconfig: Any, chat_id: str, message: str, **kwargs: Any) -> dict[str, Any]:
    """cron 独立投递；没有明确 home_channel_type 时 fail-closed。"""
    del kwargs
    extra = _effective_extra(getattr(pconfig, "extra", {}) or {})
    http_api = str(extra.get("http_api") or "").strip()
    token = str(extra.get("access_token") or "").strip()
    chat_type = str(extra.get("home_channel_type") or "").casefold()
    if not http_api or chat_type not in {"group", "dm"}:
        return {"error": "cron 必须配置 ONEBOT11_HTTP_API 和明确 home_channel_type=group|dm"}
    try:
        parse_http_base_url(http_api)
    except ValueError as exc:
        return {"error": str(exc)}
    if not _is_loopback_url(str(http_api)) and not str(token).strip():
        return {"error": "cron 使用非 loopback HTTP API 时必须配置 OneBot access token"}
    try:
        target = ChatTarget(str(chat_type), str(chat_id))
        policy = build_access_policy(extra, os.environ)
        if not policy.allows(
            target.chat_type,
            target.chat_id,
            target.chat_id if target.chat_type == "dm" else None,
        ):
            return {"error": "cron 目标不在当前 OneBot11 访问策略内"}
        api = OneBotHttpApi(str(http_api), str(token), max_retries=0)
        try:
            message_id = await api.send_message(target.chat_id, message, chat_type=target.chat_type)
            return {"success": True, "message_id": message_id}
        finally:
            await api.close()
    except (OneBotApiError, ValueError) as exc:
        return {"error": str(exc), "status": "unknown" if isinstance(exc, OneBotApiError) and exc.unknown_outcome else "error"}


def register(ctx: Any) -> None:
    """注册平台、全角色工具、权限 hooks 和旁路 trigger auxiliary。"""
    ctx.register_platform(
        name="onebot11",
        label="OneBot 11 (QQ)",
        adapter_factory=lambda cfg: OneBot11Adapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=[],
        install_hint="已随 hermes plugins install 安装；运行时依赖 aiohttp",
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        cron_deliver_env_var="ONEBOT11_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=4000,
        emoji="🐧",
        platform_hint="You are chatting via OneBot 11 (QQ). Group messages share one session and are prefixed with the sender nickname.",
    )
    # Hermes 注册的是两个角色默认许可的并集；当前 turn 再由 hooks 和
    # handler 按实际角色做双重 fail-closed 校验。
    registered_tools = READ_ONLY_TOOLS | WRITE_TOOLS
    for name, schema in TOOL_SCHEMAS.items():
        if name not in registered_tools:
            continue
        ctx.register_tool(
            name=name,
            toolset="onebot11",
            schema=schema,
            handler=_tool_dispatch(name),
            is_async=True,
            description=_TOOL_DESCRIPTIONS.get(name, name),
            emoji="🔍" if name in READ_TOOL_NAMES else "🛡️",
        )
    register_hook = getattr(ctx, "register_hook", None)
    if callable(register_hook):
        register_hook("pre_gateway_dispatch", _pre_gateway_dispatch_hook)
        register_hook("pre_llm_call", _pre_llm_call_hook)
        register_hook("pre_tool_call", _pre_tool_call_hook)
        register_hook("post_llm_call", _post_llm_call_hook)
    register_auxiliary = getattr(ctx, "register_auxiliary_task", None)
    if callable(register_auxiliary):
        register_auxiliary(
            key="onebot11_trigger",
            display_name="OneBot11 trigger judge",
            description="Judge whether a queued group message explicitly requests a response",
            defaults={"provider": "", "model": "", "timeout": 10},
        )


def _tool_dispatch(name: str):
    """注册全局 handler，运行时解析当前 OneBot adapter。"""

    async def wrapped(args: dict[str, Any], **kwargs: Any) -> str:
        adapter = _get_live_adapter()
        if adapter is None:
            return json.dumps({"status": "permission_error", "error": "OneBot11 adapter 未运行"}, ensure_ascii=False)
        return await adapter._make_tool_handler(name)(args, **kwargs)

    return wrapped


def _get_live_adapter() -> OneBot11Adapter | None:
    """从 Hermes runner 获取当前 OneBot adapter。"""
    try:
        from gateway.run import _gateway_runner_ref

        runner = _gateway_runner_ref()
        adapter = runner.adapters.get(_platform()) if runner is not None else None
    except Exception:
        return None
    return adapter if isinstance(adapter, OneBot11Adapter) else None


_TOOL_DESCRIPTIONS: dict[str, str] = {
    "qq_get_message": "按消息 ID 查询当前群或当前私聊中的消息",
    "qq_get_group_msg_history": "查询当前 QQ 群最近消息",
    "qq_get_friend_msg_history": "查询当前私聊最近消息",
    "qq_get_group_info": "查询当前群基本信息",
    "qq_get_group_member_info": "查询当前群成员信息",
    "qq_delete_message": "预览并确认撤回当前群消息",
    "qq_set_group_ban": "预览并确认禁言当前群成员",
    "qq_set_group_kick": "预览并确认踢出当前群成员",
    "qq_set_group_whole_ban": "预览并确认全员禁言",
}
