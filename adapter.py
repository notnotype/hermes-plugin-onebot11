"""Hermes 与 OneBot 11 的唯一胶水层。

群消息先进入 ``onebot11.QueueStore``，由确定性触发器创建 durable request，
``GroupDispatcher`` 再以共享 session 启动一个 Hermes turn。协议和状态机本身
保持零 Hermes 依赖，方便独立测试。
"""

from __future__ import annotations

import asyncio
import contextvars
import copy
import hashlib
import inspect
import json
import logging
import mimetypes
import os
import shutil
import tempfile
import threading
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
CONFIG_READ_TOOLS = _proto.permissions.CONFIG_READ_TOOLS
CONFIG_WRITE_TOOLS = _proto.permissions.CONFIG_WRITE_TOOLS
build_inbound_event = _proto.events.build_inbound_event
normalize_auxiliary_event = _proto.events.normalize_auxiliary_event
OneBotApiError = _proto.http_api.OneBotApiError
OneBotHttpApi = _proto.http_api.OneBotHttpApi
is_loopback_http_url = _proto.http_api.is_loopback_http_url
parse_http_base_url = _proto.http_api.parse_http_base_url
chunk_text = _proto.http_api.chunk_text
is_numeric_message_id = _proto.http_api.is_numeric_message_id
is_valid_image_bytes = _proto.http_api.is_valid_image_bytes
image_suffix = _proto.http_api.image_suffix
AuditLog = _proto.audit.AuditLog
ConfirmationStore = _proto.confirm.ConfirmationStore
ToolContext = _proto.permissions.ToolContext
build_role_tools = _proto.permissions.build_role_tools
build_trusted_users = _proto.permissions.build_trusted_users
permission_config = _proto.permissions.permission_config
parse_admin_list = _proto.permissions.parse_admin_list
parse_exact_tool_names = _proto.permissions.parse_exact_tool_names
is_onebot_tool_name = _proto.permissions.is_onebot_tool_name
chat_access_allowed = _proto.permissions.chat_access_allowed
parse_bool = _proto.permissions.parse_bool
parse_id_list = _proto.permissions.parse_id_list
role_for_user = _proto.permissions.role_for_user
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
build_agent_context = _proto.context.build_agent_context
build_dynamic_context = _proto.context.build_dynamic_context
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


def _serializable_caller(context: CallerContext) -> dict[str, Any]:
    """把不可变身份放进当前 synthetic event 的有限 metadata。"""
    return {
        "user_id": context.user_id,
        "chat_type": context.chat_type,
        "chat_id": context.chat_id,
        "role": context.role,
        "allowed_tools": sorted(context.allowed_tools),
        "lease_id": context.lease_id,
    }


def _caller_from_metadata(value: Any) -> CallerContext | None:
    """只从 adapter metadata 读取身份坐标，再由 live adapter 重算角色。"""
    if not isinstance(value, Mapping):
        return None
    try:
        user_id = str(value["user_id"])
        chat_type = str(value["chat_type"])
        chat_id = str(value["chat_id"])
        lease_id = str(value["lease_id"]) if value.get("lease_id") else None
    except (KeyError, TypeError, ValueError):
        return None
    adapter = _get_live_adapter()
    if adapter is None or chat_type not in {"group", "dm"} or not user_id or not chat_id:
        return None
    if not adapter._chat_access_allowed(chat_type, chat_id, user_id):
        return None
    if lease_id and not adapter._lease_matches_target(lease_id, chat_type, chat_id):
        return None
    role = role_for_user(user_id, adapter.super_admins, adapter.trusted_users)
    return CallerContext(
        user_id=user_id,
        chat_type=chat_type,
        chat_id=chat_id,
        role=role,
        allowed_tools=adapter.role_tools.get(role, frozenset()),
        lease_id=lease_id,
    )


class OneBot11Adapter(BasePlatformAdapter):
    """OneBot 11 适配器：私聊直接 turn，群聊持久队列 + 共享 session。"""

    def __init__(self, config: PlatformConfig) -> None:
        """读取并校验配置，初始化协议客户端和群级状态机。"""
        extra = config.extra if isinstance(config.extra, dict) else {}
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

        raw_dm_policy = os.getenv("ONEBOT11_DM_POLICY")
        self.dm_policy = str(
            raw_dm_policy if raw_dm_policy is not None else extra.get("dm_policy", "open")
        ).casefold()
        if self.dm_policy not in {"open", "allowlist", "disabled"}:
            raise ValueError(f"未知 dm_policy: {self.dm_policy}")
        raw_allowed_users = os.getenv("ONEBOT11_ALLOWED_USERS")
        self.allowed_users = parse_id_list(
            raw_allowed_users if raw_allowed_users is not None else extra.get("allowed_users")
        )
        raw_allowed_groups = os.getenv("ONEBOT11_ALLOWED_GROUPS")
        self.allowed_groups = parse_id_list(
            raw_allowed_groups if raw_allowed_groups is not None else extra.get("allowed_groups")
        )
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
        self.trusted_users = build_trusted_users(extra)
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
        self._llm_trigger_tasks: dict[str, asyncio.Task[None]] = {}
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
        raw_media_roots = extra.get("media_source_roots") or ()
        if isinstance(raw_media_roots, str):
            raw_media_roots = raw_media_roots.split(",")
        if not isinstance(raw_media_roots, (list, tuple, set, frozenset)):
            raise ValueError("media_source_roots 必须是字符串或 YAML list")
        self._media_source_roots = tuple(
            Path(str(root)).expanduser().resolve()
            for root in raw_media_roots
            if str(root).strip()
        )

        self._hermes_home = _resolve_hermes_home()
        queue_path = os.getenv("ONEBOT11_QUEUE_DB") or extra.get("queue_db_path")
        if not queue_path:
            queue_path = str(self._hermes_home / "onebot11" / "queue.sqlite3")
        self._agent_input_bytes = max(
            4_096,
            min(256 * 1024, int(extra.get("agent_input_bytes", 64 * 1024))),
        )
        self._agent_recent_originals = max(
            0,
            int(extra.get("agent_recent_originals", extra.get("queue_recent_originals", 3))),
        )
        self._queue = QueueStore(
            queue_path,
            max_messages=int(extra.get("queue_max_messages", 1000)),
            max_queue_bytes=int(extra.get("queue_max_bytes", 2_000_000)),
            max_message_bytes=int(extra.get("queue_max_message_bytes", 32_000)),
            max_original_bytes=int(extra.get("queue_max_original_bytes", 8_000)),
            max_summary_bytes=int(extra.get("queue_max_summary_bytes", 16_000)),
            recent_originals=int(
                extra.get("queue_recent_originals", self._agent_recent_originals)
            ),
            dedupe_ttl_seconds=float(extra.get("queue_dedupe_ttl_seconds", 7 * 24 * 3600)),
            max_attempts=int(extra.get("queue_max_attempts", 3)),
        )
        self._dispatcher = GroupDispatcher(
            self._queue,
            self._start_queue_turn,
            lease_seconds=float(extra.get("queue_lease_seconds", 120)),
            recovery_poll_seconds=float(extra.get("queue_recovery_poll_seconds", 5)),
            cooldown_seconds=self.trigger_config.cooldown_seconds,
            can_dispatch=self._can_dispatch_chat,
            on_lease_lost=self._on_lease_lost,
            on_recovery_tick=self._schedule_pending_llm_triggers,
        )
        self._bindings = TurnBindingStore()
        self._config_write_lock = threading.RLock()
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
        await self._recover_reaction_cleanups()
        await self._dispatcher.recover()
        if self.trigger_config.llm_enabled:
            pending_chats = await asyncio.to_thread(self._queue.pending_chat_ids)
            for chat_id in pending_chats:
                self._schedule_llm_trigger(chat_id)
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
        for task in trigger_tasks:
            task.cancel()
        if trigger_tasks:
            await asyncio.gather(*trigger_tasks, return_exceptions=True)
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

        command_head = event.text.strip().split(maxsplit=1)
        if command_head and command_head[0].casefold() == "/onebot":
            await self._handle_admin_command(event)
            return
        if event.chat_type == "group":
            if await self._handle_group_slash_command(event):
                return
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
            return False
        return chat_access_allowed(
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
        media_dir = self._new_media_dir() if ev.images else self._media_dir
        for image in ev.images[: self._max_images_per_message]:
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
                    continue
                media_urls.append(path)
                media_total_bytes += image_bytes
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
        """下载 URL 图片或通过 get_image 复制受控本地图片。"""
        normalized = str(image or "").strip()
        if not normalized:
            return None
        target_dir = dest_dir or self._media_dir
        if normalized.startswith(("http://", "https://")):
            return await self._api.download_to_temp(normalized, target_dir)
        try:
            resolved = await self._api.get_image(normalized)
        except (OneBotApiError, OSError, ValueError):
            return None
        remote_url = str(resolved.get("url") or "").strip()
        if remote_url.startswith(("http://", "https://")):
            return await self._api.download_to_temp(remote_url, target_dir)
        local_file = str(resolved.get("file") or "").strip()
        if not local_file:
            return None
        return self._copy_local_image(local_file, target_dir)

    def _copy_local_image(self, source_name: str, dest_dir: str) -> str | None:
        """只从显式 media_source_roots 复制并校验 get_image 返回的本地图片。"""
        if not self._media_source_roots:
            return None
        source = Path(source_name).expanduser()
        try:
            resolved = source.resolve(strict=True)
        except OSError:
            return None
        if not resolved.is_file() or not any(
            resolved == root or root in resolved.parents for root in self._media_source_roots
        ):
            return None
        try:
            with resolved.open("rb") as handle:
                data = handle.read(self._api.max_media_bytes + 1)
        except OSError:
            return None
        if len(data) > self._api.max_media_bytes:
            return None
        content_type = mimetypes.guess_type(str(resolved))[0] or ""
        if not is_valid_image_bytes(data, content_type, str(resolved)):
            return None
        path = Path(dest_dir) / f"{uuid.uuid4().hex}{image_suffix(data)}"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        except OSError:
            path.unlink(missing_ok=True)
            return None
        return str(path)

    def _new_media_dir(self) -> str:
        """为一个 turn 创建受控媒体目录，便于完成后精确回收。"""
        return tempfile.mkdtemp(prefix=self._media_prefix, dir=str(self._media_root))

    async def _enqueue_group_event(self, ev: _proto.events.InboundEvent) -> None:
        """先完成规范化、授权和持久入队，媒体在 lease turn 中按需下载。"""
        last_trigger_at = await asyncio.to_thread(self._queue.last_trigger_at, ev.chat_id)
        decision = should_trigger(
            chat_type="group",
            text=ev.text,
            mentioned_self=ev.mentioned_self,
            config=self.trigger_config,
            last_trigger_at=last_trigger_at,
            now=time.time(),
        )
        message_id = str(ev.message_id or "")
        if not message_id:
            message_id = "hash:" + hashlib.sha256(
                json.dumps(ev.raw_metadata, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
                + ev.text.encode("utf-8")
            ).hexdigest()
        metadata = {
            "onebot11_markers": ev.markers[:32],
            "onebot11_images": ev.images[: self._max_images_per_message],
            "onebot11_image_urls": ev.image_urls[: self._max_images_per_message],
            "onebot11_image_files": ev.image_files[: self._max_images_per_message],
            "onebot11_reply_to": ev.reply_to_message_id,
            "onebot11_segments": ev.segments[:32],
            "onebot11_raw_metadata": ev.raw_metadata,
        }
        if decision.reason == "cooldown":
            metadata["onebot11_cooldown_candidate"] = True
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
            SimpleNamespace(
                user_id=ev.user_id,
                chat_type=ev.chat_type,
                chat_id=ev.chat_id,
            )
        )
        trigger = (
            TriggerRequest.create(
                message.chat_id,
                str(message.message_key),
                decision.reason,
                caller.user_id,
                ev.user_name or caller.user_id,
            )
            if decision.triggered
            else None
        )
        try:
            result = await asyncio.to_thread(
                self._queue.enqueue,
                message,
                trigger,
                triggered_at=time.time() if decision.triggered else None,
            )
        except QueueFull:
            self._audit.record(
                "queue_full",
                {"chat_type": "group", "chat_id": ev.chat_id, "user_id": ev.user_id},
            )
            raise
        if result.inserted and decision.triggered:
            await self._dispatcher.notify(ev.chat_id)
        elif result.duplicate and result.trigger_request_id:
            await self._dispatcher.notify(ev.chat_id)
        elif not decision.triggered and decision.reason != "cooldown":
            self._schedule_llm_trigger(ev.chat_id)

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
        if not isinstance(event.metadata, dict):
            event.metadata = {}
        caller = self._caller_for_event(source)
        event.metadata["onebot11_caller_context"] = _serializable_caller(caller)
        if source.chat_type == "dm":
            token = _CURRENT_CALLER.set(caller)
            try:
                await super().handle_message(event)
            finally:
                _CURRENT_CALLER.reset(token)
            return

        decision = should_trigger(
            chat_type="group",
            text=event.text,
            mentioned_self=bool(event.metadata.get("mentioned_self", False)),
            config=self.trigger_config,
            last_trigger_at=await asyncio.to_thread(self._queue.last_trigger_at, source.chat_id),
            now=time.time(),
        )
        message_id = str(event.message_id or "")
        if not message_id:
            message_id = "hash:" + hashlib.sha256(
                json.dumps(event.metadata or {}, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
                + event.text.encode("utf-8")
            ).hexdigest()
        if decision.reason == "cooldown":
            event.metadata["onebot11_cooldown_candidate"] = True
        message = QueueMessage(
            chat_id=str(source.chat_id),
            chat_type="group",
            message_id=message_id,
            user_id=str(source.user_id or ""),
            user_name=str(source.user_name or source.user_id or "unknown"),
            text=event.text,
            raw_text=str((event.metadata or {}).get("onebot11_raw_text") or event.text),
            metadata=event.metadata,
            message_key=f"group:{message_id}",
        )
        trigger = (
            TriggerRequest.create(
                message.chat_id,
                str(message.message_key),
                decision.reason,
                caller.user_id,
                source.user_name or caller.user_id,
            )
            if decision.triggered
            else None
        )
        try:
            result = await asyncio.to_thread(
                self._queue.enqueue,
                message,
                trigger,
                triggered_at=time.time() if decision.triggered else None,
            )
        except QueueFull:
            logger.warning("OneBot11: 群 %s 队列已满，拒绝本次 WS 事件", source.chat_id)
            raise
        if result.inserted and decision.triggered:
            await self._dispatcher.notify(source.chat_id)
        elif result.duplicate and result.trigger_request_id:
            await self._dispatcher.notify(source.chat_id)
        elif not decision.triggered and decision.reason != "cooldown":
            self._schedule_llm_trigger(source.chat_id)

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
        """为每个群合并同时发生的 LLM 判断，避免无限创建判断任务。"""
        if (
            self._closed
            or not self.trigger_config.llm_enabled
            or not self._chat_access_allowed("group", str(chat_id))
            or chat_id not in self.trigger_config.llm_allowed_groups
            or self._llm_trigger_route() is None
        ):
            return
        current = self._llm_trigger_tasks.get(str(chat_id))
        if current is not None and not current.done():
            return
        task = asyncio.create_task(self._judge_llm_trigger(str(chat_id)))
        self._llm_trigger_tasks[str(chat_id)] = task

    async def _schedule_pending_llm_triggers(self) -> None:
        """恢复轮询时重新唤醒冷却结束或重启遗留的 LLM trigger 判断。"""
        if self._closed:
            return
        await self._recover_reaction_cleanups()
        if not self.trigger_config.llm_enabled:
            return
        try:
            chat_ids = await asyncio.to_thread(self._queue.pending_chat_ids)
        except Exception:
            logger.warning("OneBot11 LLM trigger 恢复读取队列失败", exc_info=True)
            return
        for chat_id in chat_ids:
            self._schedule_llm_trigger(chat_id)

    def _llm_trigger_semaphore_for_loop(self) -> asyncio.Semaphore:
        """为当前事件循环创建插件级 LLM 判断并发限制。"""
        loop = asyncio.get_running_loop()
        if self._llm_trigger_semaphore is None or self._llm_trigger_loop is not loop:
            self._llm_trigger_semaphore = asyncio.Semaphore(self.trigger_config.llm_concurrency)
            self._llm_trigger_loop = loop
        return self._llm_trigger_semaphore

    async def _judge_llm_trigger(self, chat_id: str) -> None:
        """只用 peek 判断是否触发；失败、超时和非法 JSON 都按不触发处理。"""
        reschedule = False
        observed_max_seq: int | None = None
        triggered = False
        judgment_started = False
        try:
            if not self._chat_access_allowed("group", chat_id):
                return
            route = self._llm_trigger_route()
            if route is None:
                return
            last_trigger = await asyncio.to_thread(self._queue.last_trigger_at, chat_id)
            if (
                last_trigger is not None
                and self.trigger_config.cooldown_seconds > 0
                and time.time() - last_trigger < self.trigger_config.cooldown_seconds
            ):
                return
            status = await asyncio.to_thread(self._queue.status, chat_id)
            if int(status.get("trigger_requests", 0)) > 0:
                return
            messages = await asyncio.to_thread(self._queue.peek, chat_id)
            if not messages:
                return
            observed_max_seq = max(int(message.seq or 0) for message in messages)
            prompt = build_llm_trigger_input(
                "",
                messages,
                self.trigger_config.llm_input_bytes,
            )
            try:
                from agent.auxiliary_client import async_call_llm
            except ImportError:
                logger.info("OneBot11 LLM trigger auxiliary 不可用，按不触发处理")
                return
            provider, model = route
            async with self._llm_trigger_semaphore_for_loop():
                judgment_started = True
                response = await asyncio.wait_for(
                    async_call_llm(
                        task="onebot11_trigger",
                        provider=provider,
                        model=model,
                        messages=[
                            {"role": "system", "content": "你是严格的消息触发判断器。"},
                            {"role": "user", "content": prompt},
                        ],
                        temperature=0,
                        max_tokens=8,
                        timeout=self.trigger_config.llm_timeout_seconds,
                    ),
                    timeout=self.trigger_config.llm_timeout_seconds + 1.0,
                )
            content = response.choices[0].message.content
            if not isinstance(content, str):
                return
            verdict = json.loads(content.strip())
            if type(verdict) is not bool or not verdict:
                return
            if not self._chat_access_allowed("group", chat_id):
                return
            latest_messages = await asyncio.to_thread(self._queue.peek, chat_id)
            latest = latest_messages[-1] if latest_messages else messages[-1]
            trigger = TriggerRequest.create(
                chat_id,
                str(latest.message_key or f"group:{latest.message_id}"),
                "llm",
                latest.user_id,
                latest.user_name,
            )
            result = await asyncio.to_thread(
                self._queue.enqueue,
                latest,
                trigger,
                triggered_at=time.time(),
            )
            if result.trigger_request_id:
                triggered = True
                await self._dispatcher.notify(chat_id)
        except (TimeoutError, json.JSONDecodeError, ValueError, TypeError, AttributeError) as exc:
            logger.debug("OneBot11 LLM trigger 未触发: %s", type(exc).__name__)
        except Exception:
            logger.info("OneBot11 LLM trigger 失败，按不触发处理", exc_info=True)
        finally:
            current = self._llm_trigger_tasks.get(chat_id)
            if current is asyncio.current_task():
                self._llm_trigger_tasks.pop(chat_id, None)
            if judgment_started and observed_max_seq is not None and not triggered:
                try:
                    latest_messages = await asyncio.to_thread(self._queue.peek, chat_id)
                    reschedule = bool(latest_messages) and max(
                        int(message.seq or 0) for message in latest_messages
                    ) > observed_max_seq
                except Exception:
                    logger.debug("OneBot11 LLM trigger 收尾读取队列失败", exc_info=True)
            if reschedule and not self._closed:
                self._schedule_llm_trigger(chat_id)

    def _caller_for_event(self, source: Any, *, lease_id: str | None = None) -> CallerContext:
        """按当前入站消息解析角色和允许工具集合。"""
        user_id = str(source.user_id or "")
        role = role_for_user(user_id, self.super_admins, self.trusted_users)
        return CallerContext(
            user_id=user_id,
            chat_type=str(source.chat_type),
            chat_id=str(source.chat_id),
            role=role,
            allowed_tools=self.role_tools.get(role, frozenset()),
            lease_id=lease_id,
        )

    def _tool_allowed_now(self, caller: CallerContext, tool_name: str) -> bool:
        """检查当前配置是否仍允许工具；撤销权限立即作用于后续调用。"""
        current_role = role_for_user(
            caller.user_id,
            self.super_admins,
            self.trusted_users,
        )
        return current_role == caller.role and str(tool_name) in self.role_tools.get(
            current_role, frozenset()
        )

    def _permission_snapshot(self) -> dict[str, Any]:
        """返回当前角色权限快照，供管理工具和 slash command 展示。"""
        extra = self.config.extra if isinstance(self.config.extra, Mapping) else {}
        snapshot = permission_config(extra)
        snapshot["super_admins"] = sorted(self.super_admins)
        return snapshot

    async def _start_queue_turn(self, lease: QueueLease) -> None:
        """将 lease 批量编排为一个 synthetic user turn，保持 caller/target 绑定。"""
        if not self._chat_access_allowed("group", lease.chat_id):
            raise PermissionError("当前群已不再满足 OneBot11 allowed_groups 策略")
        if not await asyncio.to_thread(self._queue.mark_agent_started, lease):
            raise PermissionError("OneBot11 queue lease 已失效")
        trigger = lease.trigger
        role = role_for_user(
            trigger.caller_user_id,
            self.super_admins,
            self.trusted_users,
        )
        caller = CallerContext(
            user_id=trigger.caller_user_id,
            chat_type="group",
            chat_id=lease.chat_id,
            role=role,
            allowed_tools=self.role_tools.get(role, frozenset()),
            lease_id=lease.lease_id,
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
            reply_id: str | None = None
            for message in lease.messages:
                images = message.metadata.get("onebot11_images") or []
                if isinstance(images, list):
                    for image in images[: self._max_images_per_message]:
                        path = await self._download_image(str(image), media_dir)
                        if not path:
                            continue
                        try:
                            image_bytes = Path(path).stat().st_size
                        except OSError:
                            image_bytes = 0
                        if media_total_bytes + image_bytes > self._max_media_total_bytes:
                            Path(path).unlink(missing_ok=True)
                            continue
                        media_paths.append(path)
                        media_total_bytes += image_bytes
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
                    "onebot11_caller_context": _serializable_caller(caller),
                    "onebot11_target": {"chat_type": "group", "chat_id": lease.chat_id},
                    "onebot11_media_dir": media_dir if has_images else None,
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
            token = _CURRENT_CALLER.set(caller)
            try:
                await super().handle_message(event)
                handed_off = True
            finally:
                _CURRENT_CALLER.reset(token)
        finally:
            if not handed_off:
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
        if enabled and not await asyncio.to_thread(self._queue.is_lease_current, lease):
            logger.info("OneBot11 reaction 跳过已失效 lease: %s", lease.lease_id)
            return None
        if enabled:
            try:
                await asyncio.to_thread(
                    self._queue.record_reaction,
                    lease.lease_id,
                    lease.chat_id,
                    message_id,
                )
            except Exception:
                logger.warning("OneBot11 reaction 状态持久化失败", exc_info=True)
        try:
            await self._api.set_message_emoji_like(
                message_id,
                self._processing_reaction_emoji_id,
                enabled=enabled,
            )
            if enabled:
                try:
                    marked = await asyncio.to_thread(
                        self._queue.mark_reaction_set, lease.lease_id
                    )
                    if not marked:
                        await asyncio.to_thread(
                            self._queue.record_reaction,
                            lease.lease_id,
                            lease.chat_id,
                            message_id,
                            state="maybe_set",
                        )
                except Exception:
                    logger.warning("OneBot11 reaction 状态更新失败", exc_info=True)
            else:
                try:
                    await asyncio.to_thread(self._queue.delete_reaction, lease.lease_id)
                except Exception:
                    logger.warning("OneBot11 reaction 状态删除失败", exc_info=True)
            return message_id
        except OneBotApiError as exc:
            logger.warning(
                "OneBot11 reaction %s 失败: lease=%s message=%s status=%s",
                "添加" if enabled else "移除",
                lease.lease_id,
                message_id,
                exc.status,
            )
            if enabled and exc.unknown_outcome:
                try:
                    marked = await asyncio.to_thread(
                        self._queue.mark_reaction_set, lease.lease_id
                    )
                    if not marked:
                        await asyncio.to_thread(
                            self._queue.record_reaction,
                            lease.lease_id,
                            lease.chat_id,
                            message_id,
                            state="maybe_set",
                        )
                except Exception:
                    logger.warning("OneBot11 未知 reaction 状态更新失败", exc_info=True)
                return message_id
            if enabled:
                try:
                    await asyncio.to_thread(self._queue.delete_reaction, lease.lease_id)
                except Exception:
                    logger.warning("OneBot11 reaction 失败状态删除失败", exc_info=True)
            else:
                await self._mark_reaction_cleanup_failed(lease.lease_id, str(exc))
            return None
        except (OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 reaction %s 失败: lease=%s message=%s error=%s",
                "添加" if enabled else "移除",
                lease.lease_id,
                message_id,
                exc,
            )
            if enabled:
                try:
                    await asyncio.to_thread(self._queue.delete_reaction, lease.lease_id)
                except Exception:
                    logger.warning("OneBot11 reaction 失败状态删除失败", exc_info=True)
            else:
                await self._mark_reaction_cleanup_failed(lease.lease_id, str(exc))
            return None
        except Exception as exc:
            logger.warning(
                "OneBot11 reaction %s 未预期失败: lease=%s message=%s error=%s",
                "添加" if enabled else "移除",
                lease.lease_id,
                message_id,
                exc,
                exc_info=True,
            )
            if enabled:
                try:
                    await asyncio.to_thread(self._queue.delete_reaction, lease.lease_id)
                except Exception:
                    logger.warning("OneBot11 reaction 失败状态删除失败", exc_info=True)
            else:
                await self._mark_reaction_cleanup_failed(lease.lease_id, str(exc))
            return None

    async def _clear_processing_reaction(self, lease_id: str) -> None:
        """在当前 turn 收尾时尽力移除处理指示器，不改变队列完成结果。"""
        self._processing_reaction_message_ids.pop(str(lease_id), None)
        try:
            reaction = await asyncio.to_thread(self._queue.reaction, str(lease_id))
        except Exception:
            logger.warning("OneBot11 reaction 状态读取失败", exc_info=True)
            return
        if reaction is None or reaction.state != "maybe_set":
            return
        await self._unset_reaction(reaction)

    async def _unset_reaction(self, reaction: Any) -> None:
        """只清理已可能设置成功的 reaction，失败时保留持久记录。"""
        if not self._chat_access_allowed("group", reaction.chat_id):
            return
        try:
            await self._api.set_message_emoji_like(
                reaction.message_id,
                self._processing_reaction_emoji_id,
                enabled=False,
            )
        except Exception as exc:
            await self._mark_reaction_cleanup_failed(reaction.lease_id, str(exc))
            return
        try:
            await asyncio.to_thread(self._queue.delete_reaction, reaction.lease_id)
        except Exception:
            logger.warning("OneBot11 reaction 清理成功但状态删除失败", exc_info=True)

    async def _mark_reaction_cleanup_failed(self, lease_id: str, reason: str) -> None:
        """记录 reaction unset 失败，不影响 Agent/队列完成。"""
        try:
            await asyncio.to_thread(
                self._queue.mark_reaction_cleanup_failed,
                str(lease_id),
                str(reason),
            )
        except Exception:
            logger.warning("OneBot11 reaction 清理失败状态无法持久化", exc_info=True)

    async def _recover_reaction_cleanups(self) -> None:
        """启动时只恢复遗留 reaction 的 unset，不重放 set 或 Agent turn。"""
        try:
            records = await asyncio.to_thread(self._queue.pending_reaction_cleanups)
        except Exception:
            logger.warning("OneBot11 reaction 恢复读取失败", exc_info=True)
            return
        for reaction in records:
            await self._unset_reaction(reaction)

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
        if outcome != ProcessingOutcome.SUCCESS and started:
            unknown = True
        if outcome == ProcessingOutcome.SUCCESS and not unknown:
            return True, False, known_failure, None
        if unknown:
            return False, True, False, "OneBot 出站结果未知或 lease 已发生部分成功"
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
        try:
            if self._closed:
                return
            try:
                ack, unknown, known_failure, reason = self._queue_completion_decision(
                    lease_id, outcome
                )
            except Exception:
                logger.warning("OneBot11 queue completion 状态读取失败", exc_info=True)
                return
            try:
                await self._clear_processing_reaction(lease_id)
            except Exception:
                logger.warning("OneBot11 reaction 清理异常，继续完成 queue lease", exc_info=True)
            if self._closed:
                return
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
            try:
                completed = await self._dispatcher.complete(lease_id, **completion_kwargs)
            except Exception:
                logger.warning("OneBot11 queue lease completion 失败", exc_info=True)
                completed = False
            if completed and not unknown and not self._closed:
                source = getattr(event, "source", None)
                target = metadata.get("onebot11_target")
                next_chat_id = getattr(source, "chat_id", None)
                if not next_chat_id and isinstance(target, Mapping):
                    next_chat_id = target.get("chat_id")
                if next_chat_id:
                    try:
                        await self._dispatcher.notify(str(next_chat_id))
                    except Exception:
                        logger.warning("OneBot11 下一轮 dispatch 启动失败", exc_info=True)
        finally:
            self._unknown_leases.discard(lease_id)
            self._outbound_started.discard(lease_id)
            self._outbound_successful.discard(lease_id)
            self._outbound_known_failure.discard(lease_id)
            self._pending_completions.pop(lease_id, None)
            self._lease_session_keys.pop(lease_id, None)
            binding = _CURRENT_BINDING.get()
            if binding is not None and binding.lease_id == lease_id:
                self._bindings.discard(binding.session_id, binding.turn_id)
            else:
                for binding_key, binding in self._bindings.snapshot().items():
                    if binding.lease_id == lease_id:
                        self._bindings.discard(*binding_key)
            try:
                self._cleanup_media(
                    getattr(event, "metadata", None) and metadata.get("onebot11_media_paths"),
                    media_dir=metadata.get("onebot11_media_dir"),
                )
            except Exception:
                logger.warning("OneBot11 turn 媒体清理失败", exc_info=True)

    async def _process_message_background(self, event: MessageEvent, session_key: str) -> None:
        """包装 Hermes background task，确保错误通知发送后才推进下一轮。"""
        metadata = event.metadata or {}
        deferred = bool(metadata.get("onebot11_defer_completion"))
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
                pending = self._pending_completions.pop(lease_id, None)
                completion_outcome = pending[0] if pending else ProcessingOutcome.FAILURE
                await self._finish_queue_turn(event, completion_outcome)
            if metadata.get("onebot11_managed_context"):
                _CURRENT_BINDING.set(None)
                _CURRENT_CALLER.set(None)

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        """按真实 Hermes/QQ 出站结果完成 queue lease。"""
        lease_id = str((event.metadata or {}).get("onebot11_lease_id") or "")
        if lease_id:
            if (event.metadata or {}).get("onebot11_defer_completion"):
                self._pending_completions[lease_id] = (
                    outcome,
                    lease_id in self._unknown_leases,
                    lease_id in self._outbound_known_failure,
                    None,
                )
            else:
                await self._finish_queue_turn(event, outcome)
        binding = _CURRENT_BINDING.get()
        if binding is not None and not lease_id:
            self._bindings.discard(binding.session_id, binding.turn_id)
        if not (event.metadata or {}).get("onebot11_managed_context"):
            _CURRENT_BINDING.set(None)
            _CURRENT_CALLER.set(None)
        self._cleanup_media(
            (event.metadata or {}).get("onebot11_media_paths")
            or getattr(event, "media_urls", []),
            media_dir=(event.metadata or {}).get("onebot11_media_dir"),
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
        caller_user_id = binding.caller.user_id if binding is not None else None
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

    def _make_tool_handler(self, tool_name: str):
        """包装工具 handler，执行角色/作用域硬校验和写操作确认。"""

        async def wrapped(args: dict[str, Any], **kwargs: Any) -> str:
            requested_platform = _platform_value(kwargs.get("platform"))
            if requested_platform and requested_platform != _PLATFORM_NAME:
                return json.dumps(
                    {
                        "status": "permission_error",
                        "error": "OneBot11 工具不能从其他 platform turn 调用",
                    },
                    ensure_ascii=False,
                )
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
            if not self._tool_allowed_now(caller, tool_name):
                error = f"角色 {caller.role} 当前无权调用 {tool_name}"
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
                if tool_name == "onebot_get_permissions":
                    result = {"status": "ok", **self._permission_snapshot()}
                elif tool_name in _TOOL_HANDLERS:
                    result = await _TOOL_HANDLERS[tool_name](self._api, args, caller)
                else:
                    result = {"status": "permission_error", "error": "OneBot11 工具未注册"}
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

    def _save_permission_change(self, tool_name: str, params: Mapping[str, Any]) -> dict[str, Any]:
        """串行执行权限配置读改写，避免同进程管理员更新互相覆盖。"""
        with self._config_write_lock:
            return self._save_permission_change_unlocked(tool_name, params)

    def _save_permission_change_unlocked(
        self, tool_name: str, params: Mapping[str, Any]
    ) -> dict[str, Any]:
        """只修改 Hermes YAML 的 platforms.onebot11.extra.roles 子树。"""
        try:
            from hermes_cli.config import atomic_config_write, get_config_path, read_user_config_raw

            raw_config = read_user_config_raw()
            if not isinstance(raw_config, dict):
                raw_config = {}
            platforms = raw_config.setdefault("platforms", {})
            if not isinstance(platforms, dict):
                raise ValueError("config.yaml 的 platforms 必须是 mapping")
            platform = platforms.setdefault("onebot11", {})
            if not isinstance(platform, dict):
                raise ValueError("config.yaml 的 platforms.onebot11 必须是 mapping")
            extra = platform.setdefault("extra", {})
            if not isinstance(extra, dict):
                raise ValueError("config.yaml 的 platforms.onebot11.extra 必须是 mapping")
            roles = extra.setdefault("roles", {})
            if not isinstance(roles, dict):
                raise ValueError("config.yaml 的 roles 必须是 mapping")

            if tool_name == "onebot_set_role_tools":
                role = str(params.get("role") or "").strip()
                if role not in {"user", "trusted_user", "super_admin"}:
                    raise ValueError("role 必须是 user、trusted_user 或 super_admin")
                tools = parse_exact_tool_names(
                    params.get("tools"),
                    name=f"roles.{role}.tools",
                )
                if "delegate_task" in tools:
                    raise ValueError("OneBot11 角色暂不允许配置 delegate_task")
                role_entry = roles.setdefault(role, {})
                if not isinstance(role_entry, dict):
                    raise ValueError(f"roles.{role} 必须是 mapping")
                role_entry["tools"] = sorted(tools)
                changed = {"role": role, "tools": sorted(tools)}
            elif tool_name == "onebot_set_trusted_users":
                users = sorted(parse_id_list(params.get("users")))
                role_entry = roles.setdefault("trusted_user", {})
                if not isinstance(role_entry, dict):
                    raise ValueError("roles.trusted_user 必须是 mapping")
                role_entry["users"] = users
                changed = {"trusted_users": users}
            else:
                return {"status": "permission_error", "error": "未知权限配置工具"}

            atomic_config_write(get_config_path(), raw_config)
        except (ImportError, OSError, TypeError, ValueError) as exc:
            return {"status": "error", "error": f"权限配置保存失败: {str(exc)[:300]}"}

        current_extra = self.config.extra if isinstance(self.config.extra, dict) else {}
        current_extra.setdefault("roles", {})
        current_extra["roles"] = copy.deepcopy(raw_config["platforms"]["onebot11"]["extra"]["roles"])
        self.config.extra = current_extra
        self.role_tools = build_role_tools(current_extra)
        self.trusted_users = build_trusted_users(current_extra)
        return {"status": "ok", "changed": changed, "effective_next_turn": True}

    async def _execute_confirmed(self, confirmation: Any) -> dict[str, Any]:
        """执行已经由直接管理命令消费的确认令牌。"""
        caller = CallerContext(
            user_id=confirmation.user_id,
            chat_type=confirmation.chat_type,
            chat_id=confirmation.chat_id,
            role="super_admin",
            allowed_tools=self.role_tools["super_admin"],
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
            if confirmation.tool_name in CONFIG_WRITE_TOOLS:
                result = self._save_permission_change(
                    confirmation.tool_name,
                    dict(confirmation.params),
                )
            else:
                result = await handle_write_action(
                    self._api,
                    confirmation.tool_name,
                    dict(confirmation.params),
                    caller,
                )
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
                status = self._queue.status(chat_id)
                status.pop("summary", None)
                status["chat_type"] = event.chat_type
                await self._send_direct(event, json.dumps(status, ensure_ascii=False))
            elif command == "flush":
                if event.chat_type != "group":
                    await self._send_direct(event, "flush 只能作用于当前群队列")
                    return
                status = await asyncio.to_thread(self._queue.status, chat_id)
                if int(status.get("pending_trigger_requests", 0)) == 0:
                    request_id = await asyncio.to_thread(
                        self._queue.create_trigger,
                        chat_id,
                        "admin_flush",
                        event.user_id,
                        event.user_name,
                    )
                    if request_id is None:
                        await self._send_direct(event, "当前群没有可 flush 的待处理消息")
                        return
                started = await self._dispatcher.notify(chat_id)
                await self._send_direct(event, f"flush: {'started' if started else '没有可 dispatch 的触发请求'}")
            elif command == "clear":
                if event.chat_type != "group":
                    await self._send_direct(event, "clear 只能作用于当前群队列")
                    return
                count = await asyncio.to_thread(self._queue.clear, chat_id)
                await self._send_direct(event, f"已清理 {count} 条待处理消息；Hermes session 历史未删除")
            elif command in {"pause", "resume"}:
                if event.chat_type != "group":
                    await self._send_direct(event, "pause/resume 只能作用于当前群队列")
                    return
                await self._dispatcher.set_paused(chat_id, command == "pause")
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

    async def _handle_group_slash_command(self, event: _proto.events.InboundEvent) -> bool:
        """在入队前处理群只读 slash command，返回是否已消费。"""
        text = event.text.strip()
        if not text.startswith("/"):
            return False
        command = text.split(maxsplit=1)[0].casefold()
        if command in {"/help", "/commands"}:
            await self._send_direct(
                event,
                "群命令: /context 查看待处理上下文；/status 查看队列状态；"
                "/whoami 查看身份；/help 查看命令。管理操作使用 /onebot。",
            )
            return True
        if command == "/whoami":
            caller = self._caller_for_event(event)
            await self._send_direct(
                event,
                json.dumps(
                    {
                        "user_id": caller.user_id,
                        "chat_type": caller.chat_type,
                        "chat_id": caller.chat_id,
                        "role": caller.role,
                        "allowed_tools": sorted(caller.allowed_tools),
                    },
                    ensure_ascii=False,
                ),
            )
            return True
        if command == "/status":
            status = await asyncio.to_thread(self._queue.status, event.chat_id)
            status.pop("summary", None)
            status["chat_type"] = event.chat_type
            await self._send_direct(event, json.dumps(status, ensure_ascii=False))
            return True
        if command == "/context":
            messages = await asyncio.to_thread(self._queue.peek, event.chat_id)
            if not messages:
                response = "当前群没有可展示的待处理队列消息。"
            else:
                response = build_agent_context(
                    "",
                    messages,
                    min(self._agent_input_bytes, 6000),
                    self._agent_recent_originals,
                )
            await self._send_direct(event, response)
            return True
        if command in {"/new", "/reset", "/restart", "/model", "/compress"}:
            await self._send_direct(
                event,
                f"群聊不允许直接执行 {command}；该命令不会进入 Agent session。",
            )
            return True
        return False

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
    source = getattr(event, "source", None)
    if _platform_value(getattr(source, "platform", None)) != _PLATFORM_NAME:
        _CURRENT_CALLER.set(None)
        _CURRENT_BINDING.set(None)
        return
    caller = _caller_from_metadata((getattr(event, "metadata", None) or {}).get("onebot11_caller_context"))
    _CURRENT_CALLER.set(caller)
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
        return {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}
    if not adapter._chat_access_allowed(caller.chat_type, caller.chat_id, caller.user_id):
        _CURRENT_BINDING.set(None)
        return {"context": "OneBot11 caller is no longer authorized; all OneBot11 tools must be denied."}
    if caller.lease_id and not adapter._lease_is_current(caller.lease_id):
        _CURRENT_BINDING.set(None)
        return {"context": "OneBot11 caller lease unavailable; all OneBot11 tools must be denied."}
    if caller.lease_id and not adapter._lease_matches_target(
        caller.lease_id, caller.chat_type, caller.chat_id
    ):
        _CURRENT_BINDING.set(None)
        return {"context": "OneBot11 caller lease target mismatch; all OneBot11 tools must be denied."}
    normalized_session_id = str(session_id or "").strip()
    normalized_turn_id = str(turn_id or "").strip()
    if not normalized_session_id or not normalized_turn_id:
        _CURRENT_BINDING.set(None)
        return {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}
    binding = TurnBinding(normalized_session_id, normalized_turn_id, caller, caller.lease_id)
    try:
        adapter._bindings.bind(binding)
    except ValueError:
        _CURRENT_BINDING.set(None)
        return {"context": "OneBot11 caller turn binding conflict; all OneBot11 tools must be denied."}
    _CURRENT_BINDING.set(binding)
    return {"context": role_prompt(caller)}


def _pre_provider_request_hook(
    request: Any = None,
    session_id: str = "",
    turn_id: str = "",
    **kwargs: Any,
) -> dict[str, Any] | None:
    """在宿主支持时向 request copy 添加动态上下文，不修改 transcript。"""
    del kwargs
    adapter = _get_live_adapter()
    if adapter is None:
        return None
    binding = adapter._resolve_binding(session_id, turn_id)
    if binding is None or not adapter._chat_access_allowed(
        binding.caller.chat_type,
        binding.caller.chat_id,
        binding.caller.user_id,
    ):
        return None
    if not isinstance(request, Mapping):
        return None
    dynamic = build_dynamic_context(
        {
            "当前目标": f"{binding.caller.chat_type}:{binding.caller.chat_id}",
            "当前角色": binding.caller.role,
            "当前时间": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        }
    )
    patched: dict[str, Any] = copy.deepcopy(dict(request))
    body = patched.get("body")
    if not isinstance(body, dict):
        body = patched
    messages = body.get("messages")
    if not isinstance(messages, list):
        return None
    copied_messages = copy.deepcopy(messages)
    copied_messages.append({"role": "user", "content": dynamic})
    body["messages"] = copied_messages
    return {"request": patched}


def _pre_tool_call_hook(tool_name: str = "", session_id: str = "", turn_id: str = "", args: dict | None = None, **kwargs: Any) -> dict[str, str] | None:
    """在所有 Hermes 工具执行前按当前 OneBot turn 硬拦截越权调用。"""
    platform = _platform_value(kwargs.get("platform"))
    normalized_tool = str(tool_name).strip()
    inherited_caller = _CURRENT_CALLER.get()
    onebot_context = bool(
        platform == _PLATFORM_NAME
        or inherited_caller is not None
        or is_onebot_tool_name(normalized_tool)
        or normalized_tool == "delegate_task"
    )
    if not onebot_context:
        return None
    if platform and platform != _PLATFORM_NAME and inherited_caller is not None:
        return {
            "action": "block",
            "message": "权限错误: OneBot11 caller 不能跨到其他 platform 或 subagent",
        }
    if normalized_tool == "delegate_task" and inherited_caller is not None:
        return {
            "action": "block",
            "message": "权限错误: OneBot11 当前禁止 delegate_task",
        }
    unknown_onebot_tool = is_onebot_tool_name(normalized_tool) and normalized_tool not in ALL_TOOLS
    if not unknown_onebot_tool and not normalized_tool:
        return {"action": "block", "message": "权限错误: OneBot11 工具名不能为空"}
    adapter = _get_live_adapter()
    if adapter is None:
        return {
            "action": "block",
            "message": "权限错误: OneBot11 adapter 不可用，已 fail-closed",
        }
    try:
        if unknown_onebot_tool:
            return {"action": "block", "message": "权限错误: 未知 OneBot11 工具"}
        if normalized_tool == "delegate_task":
            return {"action": "block", "message": "权限错误: OneBot11 当前禁止 delegate_task"}
        binding = adapter._resolve_binding(session_id, turn_id)
        if binding is None:
            return {"action": "block", "message": "OneBot11 current turn binding unavailable"}
        if binding.lease_id and not adapter._lease_is_current(binding.lease_id):
            adapter._audit.record(
                "permission_denied",
                {
                    "tool": normalized_tool,
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
        if not adapter._tool_allowed_now(binding.caller, normalized_tool):
            error = f"角色 {binding.caller.role} 当前无权调用 {normalized_tool}"
        else:
            error = validate_tool_call(
                normalized_tool,
                args or {},
                binding.caller,
                adapter.super_admins,
            )
        if error:
            adapter._audit.record(
                "permission_denied",
                {
                    "tool": normalized_tool,
                    "user_id": binding.caller.user_id,
                    "chat_type": binding.caller.chat_type,
                    "chat_id": binding.caller.chat_id,
                    "reason": error,
                },
            )
            return {"action": "block", "message": f"权限错误: {error}"}
        return None
    except Exception as exc:
        try:
            adapter._audit.record(
                "permission_denied",
                {"tool": normalized_tool, "reason": f"permission check failed: {type(exc).__name__}"},
            )
        except Exception:
            logger.warning("OneBot11 fail-closed 审计失败", exc_info=True)
        return {
            "action": "block",
            "message": "权限错误: OneBot11 权限检查异常，已 fail-closed",
        }


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
        http_api = str(os.getenv("ONEBOT11_HTTP_API") or extra.get("http_api") or "").strip()
        self_id = str(os.getenv("ONEBOT11_SELF_ID") or extra.get("self_id") or "").strip()
        if not http_api:
            return False
        parse_http_base_url(http_api)
        if not self_id:
            return False
        if str(extra.get("session_mode", "shared")).casefold() != "shared":
            return False
        if parse_bool(extra.get("group_sessions_per_user"), default=False, name="group_sessions_per_user"):
            return False
        try:
            ws_port = int(os.getenv("ONEBOT11_WS_PORT") or extra.get("ws_port", 18880))
        except (TypeError, ValueError):
            return False
        if not 0 <= ws_port <= 65535:
            return False
        ws_host = str(os.getenv("ONEBOT11_WS_HOST") or extra.get("ws_host") or "127.0.0.1")
        token = str(os.getenv("ONEBOT11_ACCESS_TOKEN") or extra.get("access_token") or "").strip()
        if ws_host not in {"127.0.0.1", "::1", "localhost"} and not token:
            return False
        if not _is_loopback_url(http_api) and not token:
            return False
        raw_dm_policy = os.getenv("ONEBOT11_DM_POLICY")
        dm_policy = str(
            raw_dm_policy if raw_dm_policy is not None else extra.get("dm_policy", "open")
        ).casefold()
        if dm_policy not in {"open", "allowlist", "disabled"}:
            return False
        raw_allowed_users = os.getenv("ONEBOT11_ALLOWED_USERS")
        parse_id_list(
            raw_allowed_users if raw_allowed_users is not None else extra.get("allowed_users")
        )
        raw_allowed_groups = os.getenv("ONEBOT11_ALLOWED_GROUPS")
        parse_id_list(
            raw_allowed_groups if raw_allowed_groups is not None else extra.get("allowed_groups")
        )
        raw_require_mention = os.getenv("ONEBOT11_REQUIRE_MENTION")
        parse_bool(
            raw_require_mention
            if raw_require_mention is not None
            else extra.get("require_mention"),
            default=True,
            name="require_mention",
        )
        raw_media_roots = extra.get("media_source_roots") or ()
        if isinstance(raw_media_roots, str):
            raw_media_roots = raw_media_roots.split(",")
        if not isinstance(raw_media_roots, (list, tuple, set, frozenset)):
            return False
        build_trigger_config(dict(extra))
        build_role_tools(extra)
        build_trusted_users(extra)
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
    extra = getattr(pconfig, "extra", {}) or {}
    http_api = str(os.getenv("ONEBOT11_HTTP_API") or extra.get("http_api") or "").strip()
    token = str(os.getenv("ONEBOT11_ACCESS_TOKEN") or extra.get("access_token") or "").strip()
    chat_type = str(extra.get("home_channel_type") or "").strip().casefold()
    if not http_api or chat_type not in {"group", "dm"}:
        return {"error": "cron 必须配置 ONEBOT11_HTTP_API 和明确 home_channel_type=group|dm"}
    raw_dm_policy = os.getenv("ONEBOT11_DM_POLICY")
    dm_policy = str(
        raw_dm_policy if raw_dm_policy is not None else extra.get("dm_policy", "open")
    ).casefold()
    raw_allowed_users = os.getenv("ONEBOT11_ALLOWED_USERS")
    raw_allowed_groups = os.getenv("ONEBOT11_ALLOWED_GROUPS")
    try:
        allowed_users = parse_id_list(
            raw_allowed_users if raw_allowed_users is not None else extra.get("allowed_users")
        )
        allowed_groups = parse_id_list(
            raw_allowed_groups if raw_allowed_groups is not None else extra.get("allowed_groups")
        )
        allow_all = parse_bool(
            os.getenv("ONEBOT11_ALLOW_ALL_USERS"),
            default=False,
            name="ONEBOT11_ALLOW_ALL_USERS",
        ) or parse_bool(
            os.getenv("GATEWAY_ALLOW_ALL_USERS"),
            default=False,
            name="GATEWAY_ALLOW_ALL_USERS",
        )
    except ValueError as exc:
        return {"error": f"cron 访问策略配置无效: {exc}"}
    if not chat_access_allowed(
        chat_type,
        str(chat_id),
        str(chat_id),
        allowed_groups=allowed_groups,
        dm_policy=dm_policy,
        allowed_users=allowed_users,
        allow_all_users=allow_all,
    ):
        return {"error": "cron 目标不满足 OneBot11 访问白名单策略", "status": "permission_error"}
    try:
        parse_http_base_url(http_api)
    except ValueError as exc:
        return {"error": str(exc)}
    if not _is_loopback_url(str(http_api)) and not str(token).strip():
        return {"error": "cron 使用非 loopback HTTP API 时必须配置 OneBot access token"}
    try:
        target = ChatTarget(str(chat_type), str(chat_id))
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
        allow_all_env="ONEBOT11_ALLOW_ALL_USERS",
        max_message_length=4000,
        emoji="🐧",
        platform_hint="You are chatting via OneBot 11 (QQ). Group messages share one session and are prefixed with the sender nickname.",
    )
    for name, schema in TOOL_SCHEMAS.items():
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
        try:
            from hermes_cli.plugins import VALID_HOOKS
        except ImportError:
            VALID_HOOKS = set()
        if "pre_provider_request" in VALID_HOOKS:
            register_hook("pre_provider_request", _pre_provider_request_hook)
        else:
            logger.info(
                "Hermes 当前未提供 pre_provider_request；OneBot11 动态上下文等待上游接口"
            )
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
