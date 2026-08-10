"""Hermes 与 OneBot 11 的唯一胶水层。

群消息先进入 ``onebot11.QueueStore``，由确定性触发器创建 durable request，
``GroupDispatcher`` 再以共享 session 启动一个 Hermes turn。协议和状态机本身
保持零 Hermes 依赖，方便独立测试。
"""

from __future__ import annotations

import asyncio
import base64
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
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import unquote

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
QueueBusy = _proto.queue.QueueBusy
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
matches_image_magic = _proto.http_api.matches_image_magic
is_loopback_http_url = _proto.http_api.is_loopback_http_url
parse_http_base_url = _proto.http_api.parse_http_base_url
chunk_text = _proto.http_api.chunk_text
is_numeric_message_id = _proto.http_api.is_numeric_message_id
AuditLog = _proto.audit.AuditLog
ConfirmationStore = _proto.confirm.ConfirmationStore
ToolContext = _proto.permissions.ToolContext
build_access_policy = _proto.permissions.build_access_policy
build_trusted_users = _proto.permissions.build_trusted_users
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
build_llm_trigger_input = _proto.triggers.build_llm_trigger_input
PiAiTriggerClient = _proto.pi_ai.PiAiTriggerClient
PiAiTriggerError = _proto.pi_ai.PiAiTriggerError
ConversationCommand = _proto.commands.ConversationCommand
parse_conversation_command = _proto.commands.parse_conversation_command
LayeredTriggerState = _proto.triggers.LayeredTriggerState
TriggerAction = _proto.triggers.TriggerAction
parse_llm_decision = _proto.triggers.parse_llm_decision
build_agent_context = _proto.context.build_agent_context
build_agent_context_parts = _proto.context.build_agent_context_parts
should_trigger = _proto.triggers.should_trigger
ReverseWsServer = _proto.ws_server.ReverseWsServer
parse_runtime_config = _proto.config.parse_runtime_config

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
_CURRENT_RESET_MARKER: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "onebot11_current_reset_marker", default=None
)

_TOOL_HANDLERS: dict[str, Any] = {
    "qq_get_message": handle_get_message,
    "qq_get_group_msg_history": handle_get_group_msg_history,
    "qq_get_friend_msg_history": handle_get_friend_msg_history,
    "qq_get_group_info": handle_get_group_info,
    "qq_get_group_member_info": handle_get_group_member_info,
}


@dataclass(frozen=True)
class _PendingSessionReset:
    """等待 Hermes ``on_session_reset`` 回调的群级 reset 标记。"""

    marker_id: str
    chat_id: str
    session_key: str
    old_session_id: str | None
    command: str
    before_seq: int
    adapter_epoch: int


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
    return _proto.config.effective_extra(extra, os.environ)


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
        "adapter_epoch": context.adapter_epoch,
    }


def _caller_from_metadata(value: Any) -> CallerContext | None:
    """读取持久化身份快照，并只与当前配置做权限交集。"""
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
        raw_epoch = value.get("adapter_epoch")
        adapter_epoch = (
            int(raw_epoch)
            if raw_epoch is not None and not isinstance(raw_epoch, bool)
            else None
        )
        if adapter_epoch is not None and adapter_epoch != adapter._adapter_epoch:
            return None
    except (KeyError, TypeError, ValueError):
        return None
    if chat_type not in {"group", "dm"} or not user_id or not chat_id:
        return None
    if chat_type == "dm" and adapter_epoch is None:
        return None
    if not adapter._chat_access_allowed(chat_type, chat_id, user_id):
        return None
    if lease_id and not adapter._lease_matches_target(lease_id, chat_type, chat_id):
        return None
    role = value.get("role")
    raw_tools = value.get("allowed_tools")
    if not isinstance(role, str) or role not in adapter.role_tools:
        return None
    if not isinstance(raw_tools, (list, tuple, set, frozenset)) or any(
        not isinstance(tool, str) for tool in raw_tools
    ):
        return None
    allowed_tools = frozenset(raw_tools) & adapter.role_tools[role]
    return CallerContext(
        user_id=user_id,
        chat_type=chat_type,
        chat_id=chat_id,
        role=role,
        allowed_tools=allowed_tools,
        lease_id=lease_id,
        self_id=self_id,
        adapter_epoch=adapter_epoch,
    )


class OneBot11Adapter(BasePlatformAdapter):
    """OneBot 11 适配器：私聊直接 turn，群聊持久队列 + 共享 session。"""

    splits_long_messages = True

    def __init__(self, config: PlatformConfig) -> None:
        """读取并校验配置，初始化协议客户端和群级状态机。"""
        runtime = parse_runtime_config(
            {} if config.extra is None else config.extra,
            os.environ,
        )
        extra = runtime.extra
        extra["session_mode"] = "shared"
        extra["group_sessions_per_user"] = False
        config.extra = extra
        super().__init__(config=config, platform=_platform())

        self.ws_port = runtime.ws_port
        self.ws_host = runtime.ws_host
        self._ws_max_queue = runtime.ws_max_queue
        self._ws_max_inflight = runtime.ws_max_inflight
        self.access_token = runtime.access_token
        http_api = runtime.http_api
        self._http_api = http_api
        self.self_id = runtime.self_id

        self._access_policy = runtime.access_policy
        self.dm_policy = self._access_policy.dm_policy
        self.allowed_users = set(self._access_policy.allowed_users)
        self.allowed_groups = set(self._access_policy.allowed_groups)
        self._allow_all_users = self._access_policy.allow_all_users
        self.require_mention = runtime.trigger_config.require_mention
        self.super_admins = set(runtime.super_admins)
        self.trusted_users = set(runtime.trusted_users)
        self.role_tools = runtime.role_tools
        self._processing_reaction_enabled = runtime.processing_reaction_enabled
        self._processing_reaction_emoji_id = runtime.processing_reaction_emoji_id
        self.trigger_config = runtime.trigger_config
        self._last_trigger_at: dict[str, float] = {}
        self._llm_trigger_tasks: dict[str, asyncio.Task[None]] = {}
        self._trigger_timer_tasks: dict[str, asyncio.Task[None]] = {}
        self._trigger_state_locks: dict[str, asyncio.Lock] = {}
        self._trigger_states: dict[str, LayeredTriggerState] = {}
        self._llm_trigger_semaphore: asyncio.Semaphore | None = None
        self._llm_trigger_loop: asyncio.AbstractEventLoop | None = None
        self._channel_prompt_supported: bool | None = None
        self._summary_fallback_audited = False

        self._api = OneBotHttpApi(
            base_url=http_api,
            token=self.access_token,
            timeout=runtime.http_timeout_seconds,
            max_retries=runtime.query_max_retries,
            max_response_bytes=runtime.http_max_response_bytes,
            allowed_media_hosts=set(runtime.media_allowed_hosts),
            allowed_media_ports=set(runtime.media_allowed_ports),
            max_media_bytes=runtime.max_image_bytes,
            max_redirects=runtime.max_image_redirects,
        )
        self._max_media_total_bytes = runtime.max_image_total_bytes
        self._max_images_per_message = runtime.max_images_per_message

        self._hermes_home = _resolve_hermes_home()
        queue_path = runtime.queue_db_path
        if not queue_path:
            queue_path = str(self._hermes_home / "onebot11" / "queue.sqlite3")
        self._queue = QueueStore(
            queue_path,
            max_messages=runtime.queue_max_messages,
            max_queue_bytes=runtime.queue_max_bytes,
            max_message_bytes=runtime.queue_max_message_bytes,
            max_original_bytes=runtime.queue_max_original_bytes,
            max_summary_bytes=runtime.queue_max_summary_bytes,
            recent_originals=runtime.queue_recent_originals,
            dedupe_ttl_seconds=runtime.queue_dedupe_ttl_seconds,
            max_attempts=runtime.queue_max_attempts,
        )
        self._agent_input_bytes = runtime.agent_input_bytes
        self._agent_recent_originals = runtime.agent_recent_originals
        self._dispatcher = GroupDispatcher(
            self._queue,
            self._start_queue_turn,
            lease_seconds=runtime.queue_lease_seconds,
            heartbeat_seconds=runtime.queue_heartbeat_seconds,
            recovery_poll_seconds=runtime.queue_recovery_poll_seconds,
            can_dispatch=self._can_dispatch_chat,
            on_lease_lost=self._on_lease_lost,
            recovery_chat_ids=lambda: (
                frozenset(self.allowed_groups)
                if self.allowed_groups
                else None
            ),
        )
        self._bindings = TurnBindingStore()
        self._confirmations = ConfirmationStore(runtime.confirm_ttl_seconds)
        audit_path = runtime.audit_path or (
            str(self._hermes_home / "onebot11" / "audit.jsonl")
        )
        self._audit = AuditLog(audit_path, max_bytes=runtime.audit_max_bytes)
        self._ws: ReverseWsServer | None = None
        self._chat_types: dict[str, str] = {}
        self._targets: dict[str, ChatTarget | None] = {}
        self._ambiguous_targets: set[str] = set()
        if runtime.home_channel is not None and runtime.home_channel_type is not None:
            self._targets[runtime.home_channel] = ChatTarget(
                runtime.home_channel_type,
                runtime.home_channel,
            )
            self._chat_types[runtime.home_channel] = runtime.home_channel_type
        media_root = self._hermes_home / "onebot11" / "media"
        media_prefix = "turn-"
        media_root.mkdir(parents=True, exist_ok=True)
        self._media_root = media_root.resolve()
        self._media_prefix = media_prefix
        self._media_orphan_ttl = runtime.media_orphan_ttl_seconds
        self._media_dir = tempfile.mkdtemp(prefix=media_prefix, dir=str(self._media_root))
        self._unknown_leases: set[str] = set()
        self._outbound_started: set[str] = set()
        self._outbound_successful: set[str] = set()
        self._outbound_known_failure: set[str] = set()
        self._processing_reaction_message_ids: dict[str, str] = {}
        self._fenced_leases: set[str] = set()
        self._lease_session_keys: dict[str, str] = {}
        self._pending_completions: dict[str, tuple[ProcessingOutcome, bool, bool, str | None]] = {}
        self._pending_session_resets: list[_PendingSessionReset] = []
        self._session_reset_tasks: set[asyncio.Task[None]] = set()
        self._resetting_groups: set[str] = set()
        self._conversation_reset_generations: dict[str, int] = {}
        self._aux_event_count = 0
        self._adapter_epoch = 0
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
        if not self._api_base():
            self._set_fatal_error("config_missing", "ONEBOT11_HTTP_API 未配置", retryable=False)
            return False

        # Hermes 可能复用同一个 adapter 实例调用 connect。只要旧 WS、旧
        # running 状态或调用方明确声明 reconnect，就先完整结算旧 runtime；
        # 不能只清空内存字典，否则旧 heartbeat 仍可能续租旧 lease。
        if is_reconnect or self._ws is not None or self.is_connected:
            await self._stop_runtime(mark_disconnected=False)

        if self._queue.closed:
            self._queue.reopen()
        await self._dispatcher.reopen()
        self._closed = False
        self._ws = ReverseWsServer(
            port=self.ws_port,
            token=self.access_token,
            on_event=self._on_ws_event,
            host=self.ws_host,
            max_queue=self._ws_max_queue,
            max_inflight=self._ws_max_inflight,
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
        return self._http_api

    async def _cancel_trigger_tasks(self) -> None:
        """取消旁路判断和定时任务，避免旧状态在 reconnect 后回写。"""
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

    async def _cancel_session_reset_tasks(self) -> None:
        """断开时取消尚未完成的群级 reset 收尾任务。"""
        tasks = list(self._session_reset_tasks)
        self._session_reset_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _reset_reconnect_state(self) -> None:
        """丢弃只存在内存中的 engaged/debounce/judging 状态。"""
        self._last_trigger_at.clear()
        self._trigger_states.clear()
        self._trigger_state_locks.clear()
        self._llm_trigger_semaphore = None
        self._llm_trigger_loop = None
        self._channel_prompt_supported = None
        self._summary_fallback_audited = False
        self._fenced_leases.clear()
        self._unknown_leases.clear()
        self._outbound_started.clear()
        self._outbound_successful.clear()
        self._outbound_known_failure.clear()
        self._lease_session_keys.clear()
        self._pending_completions.clear()
        self._processing_reaction_message_ids.clear()
        self._pending_session_resets.clear()
        self._resetting_groups.clear()
        self._conversation_reset_generations.clear()
        self._bindings.clear()

    async def _stop_runtime(self, *, mark_disconnected: bool) -> None:
        """按停止、结算、fence、清理顺序关闭当前 runtime。"""
        # 先切换 epoch，再取消旧任务；即使某个旧 task 延迟响应，
        # 也不能在 reconnect 后重新建立 DM 身份绑定。
        self._adapter_epoch += 1
        self._closed = True

        # 先停止入口，防止新的 WS 事件在旧 runtime 结算期间进入队列。
        if self._ws is not None:
            try:
                await self._ws.stop()
            except Exception:
                logger.warning("OneBot11 WS 停止失败，继续执行 lease 结算", exc_info=True)
            finally:
                self._ws = None

        await self._cancel_trigger_tasks()
        await self._cancel_session_reset_tasks()
        cancel_background = getattr(self, "cancel_background_tasks", None)
        if callable(cancel_background):
            try:
                await cancel_background()
            except Exception:
                logger.warning("OneBot11 Hermes background task 清理失败", exc_info=True)

        await self._dispatcher.close()

        # 在 reaction、HTTP session 等 best-effort 清理之前先结算 lease；
        # 这样清理网络半死不会让旧 lease 一直保持 leased。
        if not self._queue.closed:
            try:
                await asyncio.to_thread(self._queue.abandon_owner_leases)
            except Exception:
                logger.warning("OneBot11 owner lease 结算失败", exc_info=True)
            finally:
                self._queue.close()

        try:
            await asyncio.wait_for(self._clear_all_processing_reactions(), timeout=2.0)
        except Exception:
            logger.warning("OneBot11 disconnect 清理 reaction 失败", exc_info=True)
        try:
            await self._api.close()
        except Exception:
            logger.warning("OneBot11 HTTP session 关闭失败", exc_info=True)
        self._cleanup_media()
        self._reset_reconnect_state()
        if mark_disconnected:
            self._mark_disconnected()

    async def disconnect(self) -> None:
        """停止 WS、heartbeat、HTTP 会话并回收本插件创建的媒体文件。"""
        await self._stop_runtime(mark_disconnected=True)

    async def _on_ws_event(self, raw: dict) -> None:
        """归一化事件、执行入队前授权并路由到 DM/群 dispatch。"""
        raw_self_id = raw.get("self_id") if isinstance(raw, Mapping) else None
        if (
            raw_self_id is not None
            and str(raw_self_id).strip() != self.self_id
        ):
            self._audit.record(
                "access_denied",
                {
                    "reason": "OneBot raw self_id mismatch",
                    "expected_self_id": self.self_id,
                    "received_self_id": str(raw_self_id)[:64],
                },
            )
            return
        if (
            raw_self_id is None
            and isinstance(raw, Mapping)
            and str(raw.get("post_type") or "") == "message"
        ):
            self._audit.record(
                "diagnostic",
                {"reason": "OneBot raw self_id missing; compatibility path accepted"},
            )
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
            if str(raw.get("post_type") or "") == "message":
                self._audit.record(
                    "diagnostic",
                    {
                        "reason": "malformed OneBot message discarded",
                        "message_type": str(raw.get("message_type") or "")[:32],
                    },
                )
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
        conversation_command = parse_conversation_command(normalized_text)
        if conversation_command is not None and event.chat_type == "group":
            await self._handle_conversation_command(event, conversation_command)
            return
        if event.chat_type == "group":
            if str(event.chat_id) in self._resetting_groups:
                self._audit.record(
                    "message_deferred",
                    {
                        "chat_type": "group",
                        "chat_id": str(event.chat_id),
                        "user_id": str(event.user_id),
                        "reason": "session reset in progress",
                    },
                )
                await self._send_direct(
                    event,
                    "当前群正在重置会话，请稍后重新发送这条消息。",
                )
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

    async def _handle_conversation_command(
        self,
        event: _proto.events.InboundEvent,
        command: ConversationCommand,
    ) -> None:
        """在群消息入队前桥接受控会话 reset 到 Hermes 公共命令入口。"""
        if event.user_id not in self.super_admins:
            self._audit.record(
                "permission_denied",
                {
                    "chat_type": event.chat_type,
                    "chat_id": event.chat_id,
                    "user_id": event.user_id,
                    "command": command.name,
                    "reason": "群级会话命令仅超级管理员可用",
                },
            )
            await self._send_direct(event, "仅超级管理员可执行群级会话命令")
            return
        if not self._chat_access_allowed("group", event.chat_id, event.user_id):
            self._audit.record(
                "access_denied",
                {
                    "chat_type": event.chat_type,
                    "chat_id": event.chat_id,
                    "user_id": event.user_id,
                    "command": command.name,
                    "reason": "会话命令目标不再满足访问策略",
                },
            )
            await self._send_direct(event, "当前群不再满足 OneBot11 访问策略")
            return

        normalized_chat_id = str(event.chat_id)
        if normalized_chat_id in self._resetting_groups:
            self._audit.record(
                "command_rejected",
                {
                    "chat_type": "group",
                    "chat_id": normalized_chat_id,
                    "user_id": str(event.user_id),
                    "command": command.name,
                    "reason": "session reset already in progress",
                },
            )
            await self._send_direct(event, "当前群已有会话重置正在进行，请稍后重试。")
            return
        status_before_reset = await asyncio.to_thread(
            self._queue.status,
            normalized_chat_id,
        )
        reset_before_seq = int(status_before_reset.get("latest_seq", 0) or 0)
        self._resetting_groups.add(normalized_chat_id)
        prepared = await self._prepare_conversation_reset(normalized_chat_id, event)
        if not prepared:
            self._resetting_groups.discard(normalized_chat_id)
            await self._send_direct(
                event,
                "当前群有未能安全收口的 turn，未执行会话重置；请稍后重试。",
            )
            return

        reset_generation = self._conversation_reset_generations.get(
            normalized_chat_id,
            0,
        ) + 1
        self._conversation_reset_generations[normalized_chat_id] = reset_generation
        session_key = self._hermes_session_key_for_event(event)
        old_session_id = self._session_id_for_key(session_key)
        marker_id = uuid.uuid4().hex
        self._pending_session_resets.append(
            _PendingSessionReset(
                marker_id=marker_id,
                chat_id=normalized_chat_id,
                session_key=session_key,
                old_session_id=old_session_id,
                command=command.name,
                before_seq=reset_before_seq,
                adapter_epoch=self._adapter_epoch,
            )
        )
        message_event = self._build_conversation_command_event(
            event,
            command,
            reset_marker_id=marker_id,
        )
        caller = self._caller_for_event(
            SimpleNamespace(
                user_id=event.user_id,
                chat_type=event.chat_type,
                chat_id=event.chat_id,
            )
        )
        event_token = _CURRENT_EVENT.set(message_event)
        caller_token = _CURRENT_CALLER.set(caller)
        reset_token = _CURRENT_RESET_MARKER.set(marker_id)
        try:
            await super().handle_message(message_event)
        except BaseException:
            self._remove_pending_session_reset(
                normalized_chat_id,
                session_key=session_key,
            )
            self._resetting_groups.discard(normalized_chat_id)
            raise
        finally:
            _CURRENT_RESET_MARKER.reset(reset_token)
            _CURRENT_CALLER.reset(caller_token)
            _CURRENT_EVENT.reset(event_token)

    async def _prepare_conversation_reset(
        self,
        chat_id: str,
        event: _proto.events.InboundEvent,
    ) -> bool:
        """停止旁路触发并 fencing 当前群活动 lease，再交给 Hermes reset。"""
        normalized_chat_id = str(chat_id)
        async with self._trigger_lock_for(normalized_chat_id):
            state = self._trigger_states.get(normalized_chat_id)
            if state is not None:
                state.invalidate_judgement()
            self._last_trigger_at.pop(normalized_chat_id, None)
        self._cancel_llm_judgement(normalized_chat_id)

        active = self._dispatcher.active(normalized_chat_id)
        if active is None:
            status = await asyncio.to_thread(self._queue.status, normalized_chat_id)
            return int(status.get("leased", 0) or 0) == 0

        lease = active.lease
        self._fenced_leases.add(lease.lease_id)
        session_key = self._lease_session_keys.get(lease.lease_id)
        if not session_key:
            session_key = self._hermes_session_key_for_event(event)
        try:
            await self.cancel_session_processing(
                session_key,
                release_guard=True,
                discard_pending=True,
            )
        except Exception:
            logger.warning(
                "OneBot11 reset 取消旧 Hermes turn 失败: chat=%s lease=%s",
                normalized_chat_id,
                lease.lease_id,
                exc_info=True,
            )

        status = await asyncio.to_thread(self._queue.status_for_lease, lease.lease_id)
        outbound_started = bool(status.get("outbound_started")) or (
            lease.lease_id in self._outbound_started
        )
        try:
            await self._dispatcher.complete(
                lease.lease_id,
                outcome="failure",
                unknown=outbound_started or lease.lease_id in self._unknown_leases,
                known_failure=not outbound_started,
                reason="session reset fencing",
            )
        except Exception:
            logger.warning(
                "OneBot11 reset lease 结算失败: chat=%s lease=%s",
                normalized_chat_id,
                lease.lease_id,
                exc_info=True,
            )
        await self._clear_processing_reaction(lease.lease_id)
        final_status = await asyncio.to_thread(self._queue.status, normalized_chat_id)
        return int(final_status.get("leased", 0) or 0) == 0

    def _build_conversation_command_event(
        self,
        event: _proto.events.InboundEvent,
        command: ConversationCommand,
        *,
        reset_marker_id: str | None = None,
    ) -> MessageEvent:
        """构造不带发送者前缀的 Hermes 原生 slash command 事件。"""
        command_text = command.name
        if command.name == "clear":
            # Hermes gateway 的 /clear 是 CLI-only；OneBot 把它作为
            # 群级 reset 别名翻译成公共 /new，不调用 Hermes 私有 reset。
            command_text = "/new"
        elif command.name == "new":
            command_text = "/new"
            if command.argument:
                command_text = f"{command_text} {command.argument}"
        else:
            command_text = "/reset"
        caller = self._caller_for_event(
            SimpleNamespace(
                user_id=event.user_id,
                chat_type=event.chat_type,
                chat_id=event.chat_id,
            )
        )
        source = self.build_source(
            chat_id=event.chat_id,
            chat_name=event.chat_id,
            chat_type="group",
            user_id=event.user_id,
            user_name=event.user_name,
            message_id=event.message_id,
            role_authorized=True,
        )
        return MessageEvent(
            text=command_text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=event.raw_metadata,
            message_id=(
                event.message_id
                if is_numeric_message_id(event.message_id)
                else None
            ),
            reply_to_message_id=(
                event.message_id
                if is_numeric_message_id(event.message_id)
                else None
            ),
            metadata={
                "onebot11_conversation_command": command.name,
                "onebot11_target": {
                    "chat_type": "group",
                    "chat_id": event.chat_id,
                },
                "onebot11_caller_context": _serializable_caller(caller),
                "onebot11_reset_marker_id": reset_marker_id,
                "onebot11_reset_generation": self._conversation_reset_generations.get(
                    str(event.chat_id),
                    0,
                ),
                "onebot11_adapter_epoch": self._adapter_epoch,
            },
        )

    def _hermes_session_key_for_event(
        self,
        event: _proto.events.InboundEvent,
    ) -> str:
        """按 Hermes shared-session 合同生成一个群 session key。"""
        try:
            from gateway.session import build_session_key

            source = self.build_source(
                chat_id=event.chat_id,
                chat_name=event.chat_id,
                chat_type="group",
                user_id=event.user_id,
                user_name=event.user_name,
                message_id=event.message_id,
                role_authorized=True,
            )
            return str(
                build_session_key(
                    source,
                    group_sessions_per_user=False,
                    thread_sessions_per_user=False,
                )
            )
        except Exception:
            return f"onebot11:group:{event.chat_id}"

    def _session_id_for_key(self, session_key: str) -> str | None:
        """读取当前 session ID 作为 reset hook 的身份匹配提示。"""
        store = getattr(self, "_session_store", None)
        peek = getattr(store, "peek_session_id", None)
        if not callable(peek):
            return None
        try:
            value = peek(str(session_key))
        except Exception:
            return None
        return str(value) if value else None

    def _remove_pending_session_reset(
        self,
        chat_id: str,
        *,
        session_key: str | None = None,
        marker_id: str | None = None,
    ) -> None:
        """删除指定群的未完成 reset 标记，避免旧 hook 误清新命令。"""
        normalized = str(chat_id)
        self._pending_session_resets = [
            marker
            for marker in self._pending_session_resets
            if not (
                marker.chat_id == normalized
                and (session_key is None or marker.session_key == session_key)
                and (marker_id is None or marker.marker_id == marker_id)
            )
        ]

    def _match_pending_session_reset(
        self,
        *,
        old_session_id: str | None,
        new_session_id: str | None,
        session_key: str | None = None,
        marker_id: str | None = None,
    ) -> _PendingSessionReset | None:
        """按当前 reset 上下文匹配群，无法确认时保持 fail-closed。"""
        old_id = str(old_session_id or "")
        new_id = str(new_session_id or "")
        hook_session_key = str(session_key or "")
        context_marker_id = str(marker_id or _CURRENT_RESET_MARKER.get() or "")
        current_event = _CURRENT_EVENT.get()
        current_metadata = getattr(current_event, "metadata", None) or {}
        if not context_marker_id and isinstance(current_metadata, Mapping):
            context_marker_id = str(
                current_metadata.get("onebot11_reset_marker_id") or ""
            )
        context_chat_id = ""
        if isinstance(current_metadata, Mapping):
            target = current_metadata.get("onebot11_target")
            if isinstance(target, Mapping):
                context_chat_id = str(target.get("chat_id") or "")

        candidates: dict[str, _PendingSessionReset] = {}
        for marker in self._pending_session_resets:
            if marker.adapter_epoch != self._adapter_epoch:
                continue
            matches_context = (
                bool(context_marker_id) and marker.marker_id == context_marker_id
            )
            matches_session_key = (
                bool(hook_session_key) and marker.session_key == hook_session_key
            )
            matches_chat = bool(context_chat_id) and marker.chat_id == context_chat_id
            matches_old_id = bool(old_id) and marker.old_session_id in {old_id, new_id}
            matches_new_id = bool(new_id) and (
                self._session_id_for_key(marker.session_key) == new_id
            )
            if matches_context or matches_session_key or matches_chat or matches_old_id or matches_new_id:
                candidates[marker.marker_id] = marker

        if not candidates and len(self._pending_session_resets) == 1 and (
            old_id or new_id or hook_session_key or context_marker_id or context_chat_id
        ):
            # 兼容只提供新 session_id 的旧 Hermes；只有一个带身份线索的
            # 未决 reset 时可以安全匹配，多个群则必须拒绝猜测。
            marker = self._pending_session_resets[0]
            if marker.adapter_epoch == self._adapter_epoch:
                candidates[marker.marker_id] = marker
        if len(candidates) != 1:
            return None
        marker = next(iter(candidates.values()))
        self._pending_session_resets.remove(marker)
        return marker

    async def _finalize_session_reset(self, marker: _PendingSessionReset) -> None:
        """在 Hermes reset 成功后清空群队列和内存触发状态。"""
        if marker.adapter_epoch != self._adapter_epoch or self._closed:
            return
        try:
            result = await asyncio.to_thread(
                self._queue.reset_conversation,
                marker.chat_id,
                before_seq=marker.before_seq,
            )
        except QueueBusy:
            self._audit.record(
                "session_reset_failed",
                {
                    "chat_type": "group",
                    "chat_id": marker.chat_id,
                    "command": marker.command,
                    "reason": "queue still has an active lease",
                },
            )
            logger.warning(
                "OneBot11 Hermes reset 后队列仍有活动 lease: chat=%s",
                marker.chat_id,
            )
            self._resetting_groups.discard(marker.chat_id)
            return
        except Exception:
            self._audit.record(
                "session_reset_failed",
                {
                    "chat_type": "group",
                    "chat_id": marker.chat_id,
                    "command": marker.command,
                    "reason": "QueueStore reset failed",
                },
            )
            logger.warning(
                "OneBot11 Hermes reset 后队列清理失败: chat=%s",
                marker.chat_id,
                exc_info=True,
            )
            self._resetting_groups.discard(marker.chat_id)
            return

        async with self._trigger_lock_for(marker.chat_id):
            state = self._trigger_states.pop(marker.chat_id, None)
            if state is not None:
                state.invalidate_judgement()
            self._last_trigger_at.pop(marker.chat_id, None)
        self._cancel_llm_judgement(marker.chat_id)
        self._resetting_groups.discard(marker.chat_id)
        self._audit.record(
            "session_reset",
            {
                "chat_type": "group",
                "chat_id": marker.chat_id,
                "command": marker.command,
                "message_count": result.message_count,
                "trigger_count": result.trigger_count,
                "paused": result.paused,
            },
        )
        if not result.paused:
            status = await asyncio.to_thread(self._queue.status, marker.chat_id)
            if int(status.get("pending_trigger_requests", 0) or 0) > 0:
                await self._dispatcher.notify(marker.chat_id)
            elif int(status.get("pending", 0) or 0) > 0:
                await self._restore_trigger_state(marker.chat_id)

    def _on_session_reset_hook(self, **kwargs: Any) -> None:
        """接收 Hermes 公共 reset 生命周期并异步清理 OneBot 群状态。"""
        if _platform_value(kwargs.get("platform")) != _PLATFORM_NAME:
            return
        marker = self._match_pending_session_reset(
            old_session_id=kwargs.get("old_session_id"),
            new_session_id=kwargs.get("new_session_id") or kwargs.get("session_id"),
            session_key=kwargs.get("session_key"),
            marker_id=kwargs.get("onebot11_reset_marker_id"),
        )
        if marker is None:
            if self._pending_session_resets:
                self._audit.record(
                    "session_reset_unmatched",
                    {
                        "reason": "reset hook identity missing or ambiguous",
                        "pending_count": len(self._pending_session_resets),
                    },
                )
            return
        try:
            task = asyncio.create_task(self._finalize_session_reset(marker))
        except RuntimeError:
            logger.warning(
                "OneBot11 session reset hook 当前没有事件循环: chat=%s",
                marker.chat_id,
            )
            self._resetting_groups.discard(marker.chat_id)
            return
        self._session_reset_tasks.add(task)
        task.add_done_callback(self._session_reset_tasks.discard)

    def _access_allowed(self, event: _proto.events.InboundEvent) -> bool:
        """在图片下载和入队前应用严格访问策略。"""
        allowed = self._chat_access_allowed(event.chat_type, event.chat_id, event.user_id)
        if not allowed and event.chat_type == "group":
            logger.info("OneBot11: 群 %s 不在当前访问策略，拒绝入队", event.chat_id)
        return allowed

    def _chat_access_allowed(
        self, chat_type: str, chat_id: str, user_id: str | None = None
    ) -> bool:
        """用构造期 RuntimeConfig 判断实时入站和恢复 dispatch 是否可以继续。"""
        return access_allowed(
            chat_type,
            chat_id,
            user_id,
            allowed_groups=self.allowed_groups,
            dm_policy=self.dm_policy,
            allowed_users=self.allowed_users,
            allow_all_users=self._allow_all_users,
        )

    def _can_dispatch_chat(self, chat_id: str) -> bool:
        """恢复群 lease 前重新应用当前 adapter 的群访问策略。"""
        if str(chat_id) in self._resetting_groups:
            return False
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

    def validate_media_delivery_path(self, image_path: str) -> str | None:
        """校验图片路径属于受控媒体根，阻止路径穿越和 symlink 越界。"""
        try:
            candidate = Path(str(image_path)).resolve(strict=True)
        except OSError:
            return None
        if not candidate.is_file():
            return None
        allowed_roots = (
            self._media_root,
            (self._hermes_home / "image_cache").resolve(),
            (self._hermes_home / "cache" / "images").resolve(),
        )
        if not any(root == candidate or root in candidate.parents for root in allowed_roots):
            return None
        return str(candidate)

    def _supports_channel_prompt(self) -> bool:
        """检测当前 Hermes 的 MessageEvent 是否支持临时 channel_prompt。"""
        if self._channel_prompt_supported is not None:
            return self._channel_prompt_supported
        try:
            self._channel_prompt_supported = "channel_prompt" in inspect.signature(
                MessageEvent
            ).parameters
        except (TypeError, ValueError):
            self._channel_prompt_supported = False
        return self._channel_prompt_supported

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
        caller = self._caller_for_event(
            SimpleNamespace(user_id=ev.user_id, chat_type=ev.chat_type, chat_id=ev.chat_id)
        )
        metadata = {
            "onebot11_markers": ev.markers[:32],
            "onebot11_images": ev.images[: self._max_images_per_message],
            "onebot11_reply_to": ev.reply_to_message_id,
            "onebot11_segments": ev.segments[:32],
            "onebot11_raw_metadata": ev.raw_metadata,
            "onebot11_mentioned_self": ev.mentioned_self,
            "onebot11_authority": {
                "role": caller.role,
                "allowed_tools": sorted(caller.allowed_tools),
                "self_id": caller.self_id,
            },
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
                    anchor_kind="hard",
                    authority_role=caller.role,
                    authority_tools=caller.allowed_tools,
                    authority_self_id=caller.self_id,
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

    def _pi_ai_trigger_client(self) -> PiAiTriggerClient | None:
        """按 typed 触发配置创建插件自有 pi-ai 客户端。"""
        provider = self.trigger_config.llm_provider.strip()
        model = self.trigger_config.llm_model.strip()
        if not provider or not model:
            return None
        return PiAiTriggerClient(
            provider=provider,
            model=model,
            base_url=self.trigger_config.llm_base_url,
            api_key_env=self.trigger_config.llm_api_key_env,
        )

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

    @staticmethod
    def _selector_block_reason(status: Mapping[str, Any]) -> str | None:
        """返回阻止软触发消耗旁路模型的持久状态原因。"""
        if status.get("paused"):
            return "paused"
        if int(status.get("leased", 0) or 0) > 0:
            return "leased"
        if int(status.get("uncertain", 0) or 0) > 0:
            return "uncertain"
        if int(status.get("failed", 0) or 0) > 0:
            return "failed"
        if int(status.get("pending_trigger_requests", 0) or 0) > 0:
            return "hard_trigger_already_pending"
        return None

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
                    block_reason = self._selector_block_reason(status)
                    if block_reason == "leased" or self._dispatcher.active(chat_id) is not None:
                        # Agent turn 尚未完全收口时不启动旁路仲裁；completion
                        # 会在释放活动 lease 后重新安排现有 due timer。
                        return
                    if block_reason is not None:
                        state.pause() if block_reason == "paused" else state.invalidate_judgement()
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
        block_reason = self._selector_block_reason(status)
        if block_reason is not None:
            if block_reason == "paused":
                state.pause()
            else:
                state.invalidate_judgement()
            return
        # wait 是已经完成的旁路判断结果，不应因为 provider 此刻不可用
        # 丢掉等待状态；下一条候选消息再重新检查 route。
        if action.kind == "wait":
            self._schedule_trigger_timer(normalized)
            return
        if self._pi_ai_trigger_client() is None:
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
                    "candidate_type": action.candidate_type or "candidate",
                    "reason": "provider_missing",
                    "pending": int(status.get("pending", 0)),
                    "input_bytes": 0,
                    "duration_ms": 0,
                    "decision": "ignore",
                    "wait_seconds": 0,
                    "failure": "provider_missing",
                    "concurrency_waited": False,
                },
            )
            return
        self._schedule_trigger_timer(normalized)

    def _llm_trigger_ready(self) -> bool:
        """判断插件自有 pi-ai 旁路配置是否完整。"""
        return bool(self.trigger_config.llm_enabled and self._pi_ai_trigger_client())

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
                or self._selector_block_reason(status) is not None
                or not self._chat_access_allowed("group", normalized)
            ):
                if state is not None:
                    block_reason = self._selector_block_reason(status)
                    state.pause() if block_reason == "paused" else state.invalidate_judgement()
                return
            if not self._llm_trigger_ready():
                failure = "provider_missing"
                state.on_llm_failure(
                    now=time.monotonic(),
                    current_revision=int(status.get("revision", 0)),
                    generation=action.generation,
                )
                self._audit.record(
                    "llm_trigger",
                    {
                        "chat_id": normalized,
                        "candidate_type": action.candidate_type or "candidate",
                        "pending": int(status.get("pending", 0)),
                        "input_bytes": 0,
                        "decision": "ignore",
                        "wait_seconds": 0,
                        "duration_ms": 0,
                        "failure": failure,
                        "concurrency_waited": False,
                        "concurrency_wait_ms": 0,
                    },
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
        authority = self._authority_for_queued_message(latest)
        request_id = await asyncio.to_thread(
            self._queue.create_trigger,
            normalized,
            "llm",
            latest.user_id,
            latest.user_name,
            str(latest.message_key),
            anchor_kind="selector",
            authority_role=authority.role,
            authority_tools=authority.allowed_tools,
            authority_self_id=authority.self_id,
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
        control_message_id: str | None = None,
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
            messages = await asyncio.to_thread(
                self._queue.peek,
                normalized,
                include_backoff=True,
            )
            latest = messages[-1] if messages else None
            request_id = await asyncio.to_thread(
                self._queue.create_trigger,
                normalized,
                "admin_flush",
                caller_user_id,
                caller_user_name,
                str(latest.message_key) if latest is not None else None,
                anchor_kind="operator",
                control_message_id=control_message_id,
                authority_role="super_admin",
                authority_tools=self.role_tools.get("super_admin", frozenset()),
                authority_self_id=self.self_id,
            )
            has_request = request_id is not None or int(
                status.get("pending_trigger_requests", 0)
            ) > 0
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
            if self._selector_block_reason(status) == "leased":
                return False
            if (
                int(status.get("uncertain", 0) or 0) > 0
                or int(status.get("failed", 0) or 0) > 0
            ):
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
                        hard_reason = action.reason if action.reason in {
                            "mention",
                            "keyword",
                            "always",
                        } else "restore"
                        authority = self._authority_for_queued_message(message)
                        request_id = await asyncio.to_thread(
                            self._queue.create_trigger,
                            normalized,
                            hard_reason,
                            message.user_id,
                            message.user_name,
                            str(message.message_key),
                            anchor_kind="recovery",
                            authority_role=authority.role,
                            authority_tools=authority.allowed_tools,
                            authority_self_id=authority.self_id,
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
        block_reason = self._selector_block_reason(status)
        if block_reason is not None:
            if block_reason == "paused":
                state.pause()
            else:
                state.invalidate_judgement()
            return None, False, block_reason
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
        pending: int = 0,
        input_bytes: int = 0,
        duration_ms: int = 0,
        concurrency_waited: bool = False,
        concurrency_wait_ms: int = 0,
        model_call_started: bool = False,
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
                block_reason = self._selector_block_reason(status)
                if block_reason is not None:
                    state.pause() if block_reason == "paused" else state.invalidate_judgement()
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
                "pending": int(pending),
                "input_bytes": int(input_bytes),
                "decision": "ignore",
                "wait_seconds": 0,
                "duration_ms": int(duration_ms),
                "failure": "stale_judgement" if stale else failure,
                "concurrency_waited": bool(concurrency_waited),
                "concurrency_wait_ms": int(concurrency_wait_ms),
                "model_call_started": bool(model_call_started),
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
        concurrency_waited = False
        concurrency_wait_ms = 0
        messages_count = 0
        input_bytes = 0
        notify = False
        model_call_started = False
        try:
            async with self._trigger_lock_for(normalized):
                state = self._trigger_states.get(normalized)
                if state is None or not state.judgement_is_current(action.generation):
                    failure = "stale_judgement"
                elif not self._chat_access_allowed("group", normalized):
                    failure = "access_denied"
                else:
                    status = await asyncio.to_thread(self._queue.status, normalized)
                    block_reason = self._selector_block_reason(status)
                    if block_reason:
                        failure = block_reason
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
                            client = self._pi_ai_trigger_client()
                            if client is None:
                                failure = "provider_missing"
                            else:
                                semaphore = self._llm_trigger_semaphore_for_loop()
                                # 释放群锁后再等待模型，允许新消息入队并推进
                                # dirty_revision；这里仅把调用参数复制出来。
                                request = (client, prompt, semaphore)
            if failure:
                return
            client, prompt, semaphore = request
            semaphore_wait_started = time.monotonic()
            timeout_seconds = self.trigger_config.llm_timeout_seconds
            remaining = timeout_seconds - (time.monotonic() - started_at)
            if remaining <= 0:
                raise TimeoutError("OneBot11 LLM trigger 总超时")
            await asyncio.wait_for(semaphore.acquire(), timeout=remaining)
            try:
                concurrency_wait_ms = int(
                    (time.monotonic() - semaphore_wait_started) * 1000
                )
                concurrency_waited = concurrency_wait_ms > 0
                remaining = timeout_seconds - (time.monotonic() - started_at)
                if remaining <= 0:
                    raise TimeoutError("OneBot11 LLM trigger 等待并发槽超时")
                model_call_started = True
                state = self._trigger_states.get(normalized)
                if state is not None and state.generation_matches(action.generation):
                    state.model_calls += 1
                response = await asyncio.wait_for(
                    client.complete(prompt, timeout_seconds=remaining),
                    timeout=remaining,
                )
            finally:
                semaphore.release()
            content = response
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
                        "concurrency_waited": concurrency_waited,
                        "concurrency_wait_ms": concurrency_wait_ms,
                        "model_call_started": True,
                    },
                )
            if notify:
                await self._dispatcher.notify(normalized)
        except asyncio.CancelledError:
            if not self._closed:
                failure = "cancelled"
            raise
        except PiAiTriggerError as exc:
            failure = exc.kind
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
            if failure and model_call_started:
                state = self._trigger_states.get(normalized)
                if state is not None:
                    state.model_failures += 1
            if failure and not self._closed:
                await self._apply_llm_failure(
                    normalized,
                    action,
                    failure=failure,
                    pending=messages_count,
                    input_bytes=input_bytes,
                    duration_ms=int((time.monotonic() - started_at) * 1000),
                    concurrency_waited=concurrency_waited,
                    concurrency_wait_ms=concurrency_wait_ms,
                    model_call_started=model_call_started,
                )
            current = self._llm_trigger_tasks.get(normalized)
            if current is asyncio.current_task():
                self._llm_trigger_tasks.pop(normalized, None)

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
            self_id=self.self_id,
            adapter_epoch=self._adapter_epoch,
        )

    def _authority_for_queued_message(self, message: QueueMessage) -> CallerContext:
        """读取入队时的 authority 快照；缺失或损坏时返回不可执行快照。"""
        raw = message.metadata.get("onebot11_authority")
        if isinstance(raw, Mapping):
            role = raw.get("role")
            tools = raw.get("allowed_tools")
            self_id = raw.get("self_id")
            if (
                isinstance(role, str)
                and role in self.role_tools
                and isinstance(tools, (list, tuple, set, frozenset))
                and all(isinstance(item, str) for item in tools)
                and isinstance(self_id, str)
                and self_id.strip()
            ):
                return CallerContext(
                    user_id=message.user_id,
                    chat_type=message.chat_type,
                    chat_id=message.chat_id,
                    role=role,
                    allowed_tools=frozenset(tools) & self.role_tools[role],
                    self_id=str(self_id or self.self_id),
                    adapter_epoch=self._adapter_epoch,
                )
        return CallerContext(
            user_id=message.user_id,
            chat_type=message.chat_type,
            chat_id=message.chat_id,
            role="",
            allowed_tools=frozenset(),
            self_id="",
            adapter_epoch=self._adapter_epoch,
        )

    async def _start_queue_turn(self, lease: QueueLease) -> None:
        """将 lease 批量编排为一个 synthetic user turn，保持 caller/target 绑定。"""
        if not self._chat_access_allowed("group", lease.chat_id):
            raise PermissionError("当前群已不再满足 OneBot11 allowed_groups 策略")
        anchor_message = self._anchor_message(lease)
        if anchor_message is None:
            raise PermissionError("OneBot11 durable anchor 找不到对应的待处理消息")
        trigger = lease.trigger
        if trigger.authority_role not in self.role_tools:
            await asyncio.to_thread(
                self._queue.mark_uncertain,
                lease,
                "OneBot11 anchor authority 缺失、损坏或属于其他机器人",
            )
            raise PermissionError("OneBot11 durable anchor authority 无效")
        if not trigger.authority_self_id:
            await asyncio.to_thread(
                self._queue.mark_uncertain,
                lease,
                "OneBot11 anchor authority self_id 缺失",
            )
            raise PermissionError("OneBot11 durable anchor authority self_id 缺失")
        if trigger.authority_self_id != self.self_id:
            await asyncio.to_thread(
                self._queue.mark_uncertain,
                lease,
                "OneBot11 anchor authority self_id 属于其他机器人",
            )
            raise PermissionError("OneBot11 durable anchor authority self_id 不匹配")
        if not await asyncio.to_thread(self._queue.mark_agent_started, lease):
            raise PermissionError("OneBot11 queue lease 已失效")
        role = str(trigger.authority_role)
        caller = CallerContext(
            user_id=anchor_message.user_id,
            chat_type="group",
            chat_id=lease.chat_id,
            role=role,
            allowed_tools=frozenset(trigger.authority_tools) & self.role_tools[role],
            lease_id=lease.lease_id,
            self_id=self.self_id,
            adapter_epoch=self._adapter_epoch,
        )
        anchor_message_id = str(anchor_message.message_id or "").strip()
        reply_id = anchor_message_id if is_numeric_message_id(anchor_message_id) else None
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
            context_parts = build_agent_context_parts(
                lease.summary,
                lease.messages,
                self._agent_input_bytes,
                self._agent_recent_originals,
            )
            lines_text = context_parts.batch_text
            summary_prompt = context_parts.summary_prompt
            summary_mode = "none"
            if summary_prompt and self._supports_channel_prompt():
                summary_mode = "channel_prompt"
            elif summary_prompt:
                # 老 Hermes 没有临时 prompt 字段时才退回单条 user message；
                # 该差异必须可观察，不能静默把摘要再次写进 transcript。
                lines_text = build_agent_context(
                    lease.summary,
                    lease.messages,
                    self._agent_input_bytes,
                    self._agent_recent_originals,
                )
                summary_mode = "text_fallback"
                if not self._summary_fallback_audited:
                    self._audit.record(
                        "summary_prompt_degraded",
                        {
                            "reason": "Hermes MessageEvent 不支持 channel_prompt",
                            "mode": "bounded_text_fallback",
                        },
                    )
                    self._summary_fallback_audited = True
            source = self.build_source(
                chat_id=lease.chat_id,
                chat_name=lease.chat_id,
                chat_type="group",
                user_id=caller.user_id,
                user_name=anchor_message.user_name,
                message_id=reply_id,
                role_authorized=True,
            )
            event_kwargs: dict[str, Any] = {
                "text": lines_text,
                "message_type": MessageType.TEXT,
                "source": source,
                "message_id": reply_id,
                "media_urls": media_paths,
                "media_types": ["photo"] * len(media_paths),
                "reply_to_message_id": reply_id,
                "metadata": {
                    "onebot11_queue_turn": True,
                    "onebot11_lease_id": lease.lease_id,
                    "onebot11_lease_revision": lease.revision,
                    "onebot11_caller_context": _serializable_caller(caller),
                    "onebot11_target": {"chat_type": "group", "chat_id": lease.chat_id},
                    "onebot11_anchor_id": trigger.request_id,
                    "onebot11_anchor_seq": trigger.anchor_seq,
                    "onebot11_anchor_kind": trigger.anchor_kind,
                    "onebot11_anchor_message_key": anchor_message.message_key,
                    "onebot11_anchor_message_id": reply_id,
                    "onebot11_anchor_user_id": anchor_message.user_id,
                    "onebot11_anchor_user_name": anchor_message.user_name,
                    "onebot11_reset_generation": self._conversation_reset_generations.get(
                        str(lease.chat_id),
                        0,
                    ),
                    "onebot11_adapter_epoch": self._adapter_epoch,
                    "onebot11_media_dir": media_dir if has_images else None,
                    "onebot11_media_paths": list(media_paths),
                    "onebot11_media_limited": media_limited,
                    "onebot11_summary_mode": summary_mode,
                    "onebot11_defer_completion": True,
                    "onebot11_managed_context": True,
                },
            }
            if summary_prompt and summary_mode == "channel_prompt":
                event_kwargs["channel_prompt"] = summary_prompt
            try:
                event = MessageEvent(**event_kwargs)
            except TypeError:
                if "channel_prompt" not in event_kwargs:
                    raise
                # 兼容签名检测不完整的旧 Hermes，显式降级一次。
                event_kwargs.pop("channel_prompt", None)
                event_kwargs["text"] = build_agent_context(
                    lease.summary,
                    lease.messages,
                    self._agent_input_bytes,
                    self._agent_recent_originals,
                )
                event_kwargs["metadata"]["onebot11_summary_mode"] = "text_fallback"
                if not self._summary_fallback_audited:
                    self._audit.record(
                        "summary_prompt_degraded",
                        {
                            "reason": "Hermes MessageEvent 构造不接受 channel_prompt",
                            "mode": "bounded_text_fallback",
                        },
                    )
                    self._summary_fallback_audited = True
                event = MessageEvent(**event_kwargs)
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
        message = self._anchor_message(lease)
        if message is None:
            return None
        message_id = str(message.message_id or "").strip()
        return message_id if is_numeric_message_id(message_id) else None

    def _anchor_message(self, lease: QueueLease) -> QueueMessage | None:
        """按 durable anchor 的序号优先定位真实 authority 消息。"""
        anchor_seq = lease.trigger.anchor_seq
        if anchor_seq is not None:
            for message in lease.messages:
                if message.seq == anchor_seq:
                    return message
        anchor_key = str(lease.trigger.message_key or "")
        for message in lease.messages:
            if str(message.message_key) == anchor_key:
                return message
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

    async def _clear_all_processing_reactions(self) -> None:
        """断开前尽力移除所有内存中登记的 👀 reaction。"""
        for lease_id in list(self._processing_reaction_message_ids):
            await self._clear_processing_reaction(lease_id)

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
        fenced_completion = lease_id in self._fenced_leases
        raw_target = metadata.get("onebot11_target")
        if isinstance(raw_target, Mapping):
            chat_id = str(raw_target.get("chat_id") or "")
        if not chat_id:
            chat_id = str(getattr(getattr(event, "source", None), "chat_id", "") or "")
        runtime_fenced = fenced_completion
        raw_epoch = metadata.get("onebot11_adapter_epoch")
        if raw_epoch is not None:
            try:
                runtime_fenced = runtime_fenced or (
                    isinstance(raw_epoch, bool)
                    or int(raw_epoch) != self._adapter_epoch
                )
            except (TypeError, ValueError):
                runtime_fenced = True
        raw_reset_generation = metadata.get("onebot11_reset_generation")
        if raw_reset_generation is not None and chat_id:
            try:
                current_generation = self._conversation_reset_generations.get(chat_id, 0)
                runtime_fenced = runtime_fenced or (
                    isinstance(raw_reset_generation, bool)
                    or int(raw_reset_generation) != current_generation
                )
            except (TypeError, ValueError):
                runtime_fenced = True
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
            if (
                completion_error is None
                and not runtime_fenced
                and chat_id in self.trigger_config.llm_allowed_groups
                and self.trigger_config.llm_enabled
            ):
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
            elif completion_error is None and not runtime_fenced:
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
            self._audit.record(
                "completion_state_error",
                {
                    "lease_id": lease_id,
                    "chat_id": chat_id,
                    "error": f"{type(exc).__name__}: {str(exc)[:200]}",
                },
            )
        finally:
            if (
                not runtime_fenced
                and (completion_error is not None or post_completion_error is not None)
            ):
                try:
                    if chat_id and await self._ensure_completion_recovery_trigger(chat_id):
                        if not self._closed:
                            await self._dispatcher.notify(chat_id)
                except Exception:
                    logger.warning(
                        "OneBot11 completion recovery trigger 创建失败: %s",
                        chat_id,
                        exc_info=True,
                    )
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
            # queue completion 或 engaged 状态恢复失败不能在 QQ 回复之后再
            # 冒泡成第二个 Hermes 用户错误；持久队列/恢复轮询接管后续处理。

    async def _ensure_completion_recovery_trigger(self, chat_id: str) -> bool:
        """为状态收口失败后仍应自动处理的 pending 消息补 durable trigger。"""
        normalized = str(chat_id)
        if not self._chat_access_allowed("group", normalized):
            return False
        status = await asyncio.to_thread(self._queue.status, normalized)
        if int(status.get("pending_trigger_requests", 0)) > 0:
            return True
        # completion 之后新到的普通消息通常 failure_count=0；状态恢复失败
        # 也不能让它们留下 pending=1、trigger=0 的永久静默状态。
        if int(status.get("pending", 0)) <= 0:
            return False
        messages = await asyncio.to_thread(self._queue.peek, normalized)
        if not messages:
            return False
        latest = messages[-1]
        authority = self._authority_for_queued_message(latest)
        request_id = await asyncio.to_thread(
            self._queue.create_trigger,
            normalized,
            "completion_recovery",
            latest.user_id,
            latest.user_name,
            str(latest.message_key),
            anchor_kind="recovery",
            authority_role=authority.role,
            authority_tools=authority.allowed_tools,
            authority_self_id=authority.self_id,
        )
        if request_id:
            self._audit.record(
                "completion_recovery_trigger",
                {
                    "chat_id": normalized,
                    "pending": int(status.get("pending", 0)),
                    "reason": "queue completion/state recovery",
                },
            )
        return request_id is not None

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
        current_event = _CURRENT_EVENT.get()
        current_event_metadata = getattr(current_event, "metadata", None) or {}
        managed_context = bool(
            isinstance(metadata, Mapping)
            and (
                metadata.get("onebot11_managed_context")
                or metadata.get("onebot11_lease_id")
            )
        ) or bool(
            isinstance(current_event_metadata, Mapping)
            and (
                current_event_metadata.get("onebot11_managed_context")
                or current_event_metadata.get("onebot11_lease_id")
            )
        )
        if managed_context and binding is None:
            stale_lease_id = str(
                (
                    metadata.get("onebot11_lease_id")
                    if isinstance(metadata, Mapping)
                    else None
                )
                or (
                    current_event_metadata.get("onebot11_lease_id")
                    if isinstance(current_event_metadata, Mapping)
                    else None
                )
                or ""
            ).strip()
            if stale_lease_id:
                self._fenced_leases.add(stale_lease_id)
                self._outbound_known_failure.add(stale_lease_id)
            return SendResult(
                False,
                error="OneBot11 managed turn binding unavailable，拒绝出站",
                error_kind="fenced",
            )
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
                    if sent:
                        self._unknown_leases.add(lease_id)
                    else:
                        self._outbound_known_failure.add(lease_id)
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error=str(exc),
                    raw_response={"sent_chunks": len(sent), "total_chunks": len(pieces)},
                    error_kind="unknown" if sent else "failed",
                )
        return SendResult(
            True,
            message_id=sent[-1] if sent else str(uuid.uuid4()),
            raw_response={"sent_chunks": len(sent), "total_chunks": len(pieces)},
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: str | None = None,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> SendResult:
        """把受信任的本地图片编码为 OneBot ``base64://`` image segment。"""
        del kwargs
        preflight = self._preflight_image_delivery(chat_id, metadata)
        if preflight is not None:
            return preflight
        binding = self._binding_from_context()
        current_event = _CURRENT_EVENT.get()
        lease_id = binding.lease_id if binding else None
        target = self._resolve_target(str(chat_id), metadata)
        assert target is not None

        safe_path = self.validate_media_delivery_path(str(image_path))
        if not safe_path:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="图片路径不在 Hermes 允许的媒体目录中", error_kind="permission")
        path = Path(safe_path)
        resolved_path = path.resolve()
        allowed_roots = (
            self._media_root,
            (self._hermes_home / "image_cache").resolve(),
            (self._hermes_home / "cache" / "images").resolve(),
        )
        if not any(
            resolved_path == root or root in resolved_path.parents
            for root in allowed_roots
        ):
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="图片路径不在 Hermes 媒体根目录中", error_kind="permission")
        suffix = path.suffix.casefold()
        if suffix not in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="OneBot11 只允许 PNG/JPEG/GIF/WebP 图片出站", error_kind="failed")
        try:
            data = path.read_bytes()
        except OSError as exc:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error=f"读取图片失败: {exc}", error_kind="failed")
        if len(data) > self._api.max_media_bytes:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="图片超过单图大小限制", error_kind="too_large")
        if not matches_image_magic(data, "", path.name):
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error="图片魔数与文件类型不匹配", error_kind="failed")

        reply_id = str(reply_to or "").strip()
        if not reply_id and current_event is not None:
            reply_id = str(getattr(current_event, "message_id", "") or "").strip()
        segments: list[dict[str, Any]] = []
        if is_numeric_message_id(reply_id):
            segments.append({"type": "reply", "data": {"id": reply_id}})
        segments.append(
            {
                "type": "image",
                "data": {
                    "file": "base64://" + base64.b64encode(data).decode("ascii"),
                },
            }
        )
        if caption:
            segments.append({"type": "text", "data": {"text": str(caption)}})

        if lease_id:
            marked = await asyncio.to_thread(self._queue.mark_outbound_started, lease_id)
            if not marked:
                self._fenced_leases.add(lease_id)
                self._outbound_known_failure.add(lease_id)
                return SendResult(False, error="OneBot11 lease 已失效，拒绝出站", error_kind="fenced")
            self._outbound_started.add(lease_id)
        try:
            sent_id = await self._api.send_message_segments(
                target.chat_id,
                segments,
                chat_type=target.chat_type,
            )
            if not sent_id:
                if lease_id:
                    self._unknown_leases.add(lease_id)
                return SendResult(
                    False,
                    error="OneBot 成功响应缺少 message_id，图片出站结果未知",
                    error_kind="unknown",
                )
            if lease_id:
                self._outbound_successful.add(lease_id)
            return SendResult(
                True,
                message_id=sent_id,
                raw_response={"segment_type": "image"},
            )
        except OneBotApiError as exc:
            if lease_id:
                if exc.unknown_outcome:
                    self._unknown_leases.add(lease_id)
                else:
                    self._outbound_known_failure.add(lease_id)
            return SendResult(
                False,
                error=str(exc),
                error_kind="unknown" if exc.unknown_outcome else exc.error_kind,
            )
        except (OSError, ValueError) as exc:
            if lease_id:
                self._outbound_known_failure.add(lease_id)
            return SendResult(False, error=str(exc), error_kind="failed")

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: str | None = None,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        """安全下载远程图片后，以 base64 segment 发送，不回退为 URL 文本。"""
        preflight = self._preflight_image_delivery(chat_id, metadata)
        if preflight is not None:
            return preflight
        media_dir = self._new_media_dir()
        path = await self._api.download_to_temp(str(image_url), media_dir)
        if not path:
            self._cleanup_media(media_dir=media_dir)
            return SendResult(False, error="图片下载失败或未通过安全校验", error_kind="failed")
        try:
            return await self.send_image_file(
                chat_id,
                path,
                caption=caption,
                reply_to=reply_to,
                metadata=metadata,
            )
        finally:
            self._cleanup_media([path], media_dir=media_dir)

    def _preflight_image_delivery(
        self,
        chat_id: str,
        metadata: Mapping[str, Any] | None,
    ) -> SendResult | None:
        """在图片下载或 OneBot 请求前校验身份、目标、权限和连接。"""
        binding = self._binding_from_context()
        current_event = _CURRENT_EVENT.get()
        current_event_metadata = getattr(current_event, "metadata", None) or {}
        managed_context = bool(
            isinstance(metadata, Mapping)
            and (
                metadata.get("onebot11_managed_context")
                or metadata.get("onebot11_lease_id")
            )
        ) or bool(
            isinstance(current_event_metadata, Mapping)
            and (
                current_event_metadata.get("onebot11_managed_context")
                or current_event_metadata.get("onebot11_lease_id")
            )
        )
        if managed_context and binding is None:
            stale_lease_id = str(
                (
                    metadata.get("onebot11_lease_id")
                    if isinstance(metadata, Mapping)
                    else None
                )
                or (
                    current_event_metadata.get("onebot11_lease_id")
                    if isinstance(current_event_metadata, Mapping)
                    else None
                )
                or ""
            ).strip()
            if stale_lease_id:
                self._fenced_leases.add(stale_lease_id)
                self._outbound_known_failure.add(stale_lease_id)
            return SendResult(
                False,
                error="OneBot11 managed turn binding unavailable，拒绝出站",
                error_kind="fenced",
            )
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
        return None

    async def send_multiple_images(
        self,
        chat_id: str,
        images: list[tuple[str, str]],
        metadata: dict[str, Any] | None = None,
        human_delay: float = 0.0,
    ) -> list[SendResult]:
        """先完成全部图片预检，再逐张发送，避免发送一半才发现总量超限。"""
        if not images:
            return []
        if len(images) > self._max_images_per_message:
            return [
                SendResult(
                    False,
                    error="图片数量超过 OneBot11 单条消息限制",
                    error_kind="too_many",
                )
                for _image_url, _caption in images
            ]

        binding = self._binding_from_context()
        current_event = _CURRENT_EVENT.get()
        current_event_metadata = getattr(current_event, "metadata", None) or {}
        managed_context = bool(
            isinstance(metadata, Mapping)
            and (
                metadata.get("onebot11_managed_context")
                or metadata.get("onebot11_lease_id")
            )
        ) or bool(
            isinstance(current_event_metadata, Mapping)
            and (
                current_event_metadata.get("onebot11_managed_context")
                or current_event_metadata.get("onebot11_lease_id")
            )
        )
        if managed_context and (
            binding is None
            or (
                binding.lease_id is not None
                and not self._lease_is_current(binding.lease_id)
            )
        ):
            return [
                SendResult(
                    False,
                    error="OneBot11 managed turn binding unavailable 或 lease 已失效",
                    error_kind="fenced",
                )
                for _image_url, _caption in images
            ]

        media_dir = self._new_media_dir()
        downloaded_paths: list[str] = []
        prepared: list[tuple[str, str]] = []
        total_bytes = 0

        def preflight_failure(error: str, error_kind: str) -> list[SendResult]:
            """为未发出 OneBot 请求的整批图片返回一致的预检失败。"""
            return [
                SendResult(False, error=error, error_kind=error_kind)
                for _image_url, _caption in images
            ]

        try:
            for image_url, caption in images:
                raw_url = str(image_url)
                if raw_url.startswith("file://"):
                    path = unquote(raw_url[7:])
                else:
                    path = await self._api.download_to_temp(raw_url, media_dir)
                    if not path:
                        return preflight_failure("图片下载失败或未通过安全校验", "failed")
                    downloaded_paths.append(path)
                safe_path = self.validate_media_delivery_path(path)
                if not safe_path:
                    return preflight_failure(
                        "图片路径不在 Hermes 允许的媒体目录中",
                        "permission",
                    )
                local_path = Path(safe_path)
                suffix = local_path.suffix.casefold()
                if suffix not in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
                    return preflight_failure(
                        "OneBot11 只允许 PNG/JPEG/GIF/WebP 图片出站",
                        "failed",
                    )
                try:
                    data = local_path.read_bytes()
                except OSError as exc:
                    return preflight_failure(f"读取图片失败: {exc}", "failed")
                if len(data) > self._api.max_media_bytes:
                    return preflight_failure("图片超过单图大小限制", "too_large")
                if not matches_image_magic(data, "", local_path.name):
                    return preflight_failure(
                        "图片魔数与文件类型不匹配",
                        "failed",
                    )
                if total_bytes + len(data) > self._max_media_total_bytes:
                    return preflight_failure(
                        "图片总大小超过 OneBot11 单条消息限制",
                        "too_large",
                    )
                total_bytes += len(data)
                prepared.append((str(local_path), caption))

            results: list[SendResult] = []
            for index, (path, caption) in enumerate(prepared):
                if human_delay > 0:
                    await asyncio.sleep(human_delay)
                result = await self.send_image_file(
                    chat_id,
                    path,
                    caption=caption or None,
                    metadata=metadata,
                )
                results.append(result)
                if result.error_kind in {"unknown", "fenced"}:
                    results.extend(
                        SendResult(
                            False,
                            error="前一张图片出站结果未知，已跳过后续图片",
                            error_kind="unknown",
                        )
                        for _ in prepared[index + 1 :]
                    )
                    break
            return results
        finally:
            self._cleanup_media(downloaded_paths, media_dir=media_dir)

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
        if binding is None:
            return None
        caller = _CURRENT_CALLER.get()
        if caller is None or caller != binding.caller:
            return None
        # ContextVar 可能跨 reconnect/取消边界残留；只有 binding store 中
        # 仍存在完全相同的 (session_id, turn_id) 才允许继续出站。
        current = self._bindings.get(binding.session_id, binding.turn_id)
        return current if current == binding else None

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
            if (
                binding.caller.adapter_epoch is not None
                and binding.caller.adapter_epoch != self._adapter_epoch
            ):
                return json.dumps(
                    {"status": "permission_error", "error": "当前 adapter epoch 已失效"},
                    ensure_ascii=False,
                )
            if self._closed:
                return json.dumps(
                    {"status": "permission_error", "error": "OneBot11 adapter 已关闭"},
                    ensure_ascii=False,
                )
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
        fingerprint = self._operation_fingerprint(
            confirmation.tool_name,
            confirmation.params,
            chat_type=confirmation.chat_type,
            chat_id=confirmation.chat_id,
        )
        operation_start = await asyncio.to_thread(
            self._queue.start_operation,
            fingerprint=fingerprint,
            tool_name=confirmation.tool_name,
            chat_type=confirmation.chat_type,
            chat_id=confirmation.chat_id,
            caller_user_id=confirmation.user_id,
            params=dict(confirmation.params),
        )
        if operation_start.blocked:
            self._audit.record(
                "unknown_blocked",
                {
                    "tool": confirmation.tool_name,
                    "user_id": confirmation.user_id,
                    "chat_type": confirmation.chat_type,
                    "chat_id": confirmation.chat_id,
                    "operation_id": operation_start.operation.operation_id,
                    "fingerprint": fingerprint[:12],
                },
            )
            return {
                "status": "unknown",
                "operation_id": operation_start.operation.operation_id,
                "error": "同一管理动作已有未知结果，禁止重复执行",
            }
        operation_id = operation_start.operation.operation_id
        try:
            result = await handle_write_action(self._api, confirmation.tool_name, dict(confirmation.params), caller)
        except asyncio.CancelledError:
            await asyncio.to_thread(
                self._queue.finish_operation,
                operation_id,
                "unknown",
                reason="管理动作 task 被取消，结果未知",
            )
            raise
        except OneBotApiError as exc:
            result = {"status": "unknown" if exc.unknown_outcome else "error", "error": str(exc)}
            await asyncio.to_thread(
                self._queue.finish_operation,
                operation_id,
                "unknown" if exc.unknown_outcome else "known_failed",
                reason=str(exc),
            )
        except (KeyError, TypeError, ValueError) as exc:
            # 参数缺失或格式错误发生在访问 OneBot 之前，不应制造一个
            # 需要人工 resolve 的“未知出站”假象。
            result = {"status": "error", "error": str(exc)}
            await asyncio.to_thread(
                self._queue.finish_operation,
                operation_id,
                "known_failed",
                reason=str(exc),
            )
        except Exception as exc:
            result = {"status": "unknown", "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
            await asyncio.to_thread(
                self._queue.finish_operation,
                operation_id,
                "unknown",
                reason=str(exc),
            )
        else:
            result_status = "succeeded" if result.get("status") == "ok" else "known_failed"
            await asyncio.to_thread(
                self._queue.finish_operation,
                operation_id,
                result_status,
                reason=str(result.get("error") or "")[:512] or None,
            )
        result["operation_id"] = operation_id
        self._audit.record(
            "execute",
            {
                "tool": confirmation.tool_name,
                "user_id": caller.user_id,
                "chat_type": caller.chat_type,
                "chat_id": caller.chat_id,
                "operation_id": operation_id,
                "fingerprint": fingerprint[:12],
                "status": result.get("status"),
            },
        )
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
        parts = event.text.strip().split()
        command = parts[1].casefold() if len(parts) > 1 else "status"
        chat_id = event.chat_id
        self._audit.record(
            "admin_command",
            {"command": command, "chat_type": event.chat_type, "chat_id": chat_id, "user_id": event.user_id},
        )
        try:
            if command == "confirm" and len(parts) >= 3:
                confirmation = self._confirmations.consume(
                    parts[2],
                    user_id=event.user_id,
                    chat_type=event.chat_type,
                    chat_id=chat_id,
                )
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
                trigger_snapshot = (
                    trigger_state.snapshot()
                    if trigger_state is not None
                    else {"mode": "idle", "llm_calls": 0, "llm_failures": 0}
                )
                now = time.monotonic()
                for field in ("debounce_due", "wait_until", "engaged_until"):
                    value = trigger_snapshot.get(field)
                    trigger_snapshot[f"{field}_remaining_seconds"] = (
                        max(0.0, float(value) - now)
                        if value is not None
                        else None
                    )
                status["trigger"] = trigger_snapshot
                status["operations"] = [
                    self._queue.operation_summary(record)
                    for record in await asyncio.to_thread(
                        self._queue.operation_records,
                        chat_id,
                    )
                ]
                status["unknown_operations"] = await asyncio.to_thread(
                    self._queue.unknown_operation_count,
                    chat_id,
                )
                status["auxiliary_events"] = self._aux_event_count
                await self._send_direct(event, json.dumps(status, ensure_ascii=False))
            elif command == "flush":
                if event.chat_type != "group":
                    await self._send_direct(event, "flush 只能作用于当前群队列")
                    return
                has_request, started, paused = await self._flush_group(
                    chat_id,
                    caller_user_id=event.user_id,
                    caller_user_name=event.user_name,
                    control_message_id=event.message_id,
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
            elif command == "resolve":
                if len(parts) >= 5 and parts[2].casefold() == "action":
                    if event.chat_type != "group":
                        await self._send_direct(event, "resolve action 只能作用于当前群")
                        return
                    action = parts[3].casefold()
                    if action not in {"retry", "discard"}:
                        await self._send_direct(event, "用法: /onebot resolve action retry|discard OPERATION_ID")
                        return
                    record = await asyncio.to_thread(
                        self._queue.resolve_operation,
                        parts[4],
                        action,
                        chat_type=event.chat_type,
                        chat_id=chat_id,
                        caller_user_id=event.user_id,
                    )
                    if record is None:
                        await self._send_direct(event, "operation 不存在、已属于其他群或不是同一超级管理员")
                        return
                    expected = "retry_armed" if action == "retry" else "discarded"
                    self._audit.record(
                        "admin_resolve_action",
                        {
                            "operation_id": record.operation_id,
                            "chat_type": event.chat_type,
                            "chat_id": chat_id,
                            "user_id": event.user_id,
                            "action": action,
                            "status": record.status,
                        },
                    )
                    if record.status != expected:
                        await self._send_direct(
                            event,
                            f"operation 当前状态为 {record.status}，只有 unknown 才能执行 {action}",
                        )
                        return
                    if action == "retry":
                        await self._send_direct(
                            event,
                            f"已解除 operation {record.operation_id} 的重复执行阻断；请重新让 Hermes 生成预览并确认，可能重复执行。",
                        )
                    else:
                        await self._send_direct(
                            event,
                            f"已放弃 operation {record.operation_id}；不会再次访问 OneBot。",
                        )
                elif len(parts) >= 3 and parts[2].casefold() in {"retry", "discard"}:
                    if event.chat_type != "group":
                        await self._send_direct(event, "resolve 只能作用于当前群队列")
                        return
                    action = parts[2].casefold()
                    count = await asyncio.to_thread(self._queue.resolve_uncertain, chat_id, action)
                    if action == "retry":
                        status = await asyncio.to_thread(self._queue.status, chat_id)
                        if count and int(status.get("pending_trigger_requests", 0)) == 0:
                            await asyncio.to_thread(
                                self._queue.create_trigger,
                                chat_id,
                                "admin_resolve_retry",
                                event.user_id,
                                event.user_name,
                                None,
                                anchor_kind="operator",
                                authority_role="super_admin",
                                authority_tools=self.role_tools.get("super_admin", frozenset()),
                                authority_self_id=self.self_id,
                            )
                        await self._dispatcher.notify(chat_id)
                    self._audit.record(
                        "admin_resolve",
                        {
                            "chat_type": event.chat_type,
                            "chat_id": chat_id,
                            "user_id": event.user_id,
                            "action": action,
                            "count": count,
                        },
                    )
                    await self._send_direct(
                        event,
                        f"已处理 uncertain/failed 消息 {count} 条: {action}（retry 可能重复执行）",
                    )
                else:
                    await self._send_direct(
                        event,
                        "用法: /onebot resolve action retry|discard OPERATION_ID",
                    )
            else:
                await self._send_direct(
                    event,
                    "用法: /onebot status|queue|flush|clear|pause|resume|"
                    "resolve retry|resolve discard|resolve action retry|discard OPERATION_ID|confirm TOKEN",
                )
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
    if (
        caller.adapter_epoch is not None
        and caller.adapter_epoch != adapter._adapter_epoch
    ):
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller belongs to an expired adapter epoch; all tools must be denied."}
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
    if getattr(adapter, "_closed", True):
        return {"action": "block", "message": "OneBot11 adapter 已关闭"}
    if tool_name not in ALL_TOOLS:
        return {"action": "block", "message": "权限错误: 未知 OneBot11 工具"}
    binding = adapter._resolve_binding(session_id, turn_id)
    if binding is None:
        return {"action": "block", "message": "OneBot11 current turn binding unavailable"}
    if (
        binding.caller.adapter_epoch is not None
        and binding.caller.adapter_epoch != adapter._adapter_epoch
    ):
        return {"action": "block", "message": "权限错误: 当前 adapter epoch 已失效"}
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


def _on_session_reset_hook(**kwargs: Any) -> None:
    """把 Hermes 公共 session reset 生命周期转给活动 OneBot adapter。"""
    adapter = _get_live_adapter()
    if adapter is not None:
        adapter._on_session_reset_hook(**kwargs)


def check_requirements() -> bool:
    """只检查插件运行依赖；部署配置由 validate_config 读取 YAML 或环境变量。"""
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        return False
    return True


def validate_config(config: Any) -> bool:
    """验证平台配置和 OneBot session/access 合同。"""
    raw_extra = getattr(config, "extra", None)
    extra = {} if raw_extra is None else raw_extra
    if not isinstance(extra, Mapping):
        return False
    try:
        parse_runtime_config(extra, os.environ, require_http_api=True)
        return True
    except (TypeError, ValueError, OverflowError):
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
        "ONEBOT11_LLM_TRIGGER_ENABLED", "ONEBOT11_LLM_TRIGGER_PROVIDER",
        "ONEBOT11_LLM_TRIGGER_MODEL", "ONEBOT11_LLM_TRIGGER_BASE_URL",
        "ONEBOT11_LLM_TRIGGER_API_KEY_ENV", "ONEBOT11_LLM_TRIGGER_GROUPS",
    ):
        value = os.getenv(key, "").strip()
        if value:
            seed[key.removeprefix("ONEBOT11_").lower()] = value
    return seed


async def _standalone_send(pconfig: Any, chat_id: str, message: str, **kwargs: Any) -> dict[str, Any]:
    """cron 独立投递；没有明确 home_channel_type 时 fail-closed。"""
    del kwargs
    try:
        raw_extra = getattr(pconfig, "extra", None)
        runtime = parse_runtime_config(
            {} if raw_extra is None else raw_extra,
            os.environ,
            require_http_api=True,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        return {"error": str(exc)}
    target_id = str(runtime.home_channel or chat_id).strip()
    if runtime.home_channel is not None and target_id != str(chat_id).strip():
        return {"error": "cron 目标必须与配置的 home_channel 一致"}
    if runtime.home_channel_type not in {"group", "dm"}:
        return {"error": "cron 必须配置明确 home_channel_type=group|dm"}
    try:
        target = ChatTarget(runtime.home_channel_type, target_id)
        policy = runtime.access_policy
        if not policy.allows(
            target.chat_type,
            target.chat_id,
            target.chat_id if target.chat_type == "dm" else None,
        ):
            return {"error": "cron 目标不在当前 OneBot11 访问策略内"}
        api = OneBotHttpApi(
            runtime.http_api,
            runtime.access_token,
            timeout=runtime.http_timeout_seconds,
            max_retries=0,
            max_response_bytes=runtime.http_max_response_bytes,
        )
        try:
            message_id = await api.send_message(target.chat_id, message, chat_type=target.chat_type)
            if not message_id:
                return {
                    "status": "unknown",
                    "error": "OneBot 成功响应缺少 message_id，出站结果未知",
                }
            return {"success": True, "message_id": message_id}
        finally:
            await api.close()
    except (OneBotApiError, ValueError) as exc:
        return {"error": str(exc), "status": "unknown" if isinstance(exc, OneBotApiError) and exc.unknown_outcome else "error"}


def register(ctx: Any) -> None:
    """注册平台、全角色工具和权限 hooks。"""
    ctx.register_platform(
        name="onebot11",
        label="OneBot 11 (QQ)",
        adapter_factory=lambda cfg: OneBot11Adapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=[],
        install_hint="已随 hermes plugins install 安装；运行时依赖 aiohttp、Node.js >=22.19 和 npm pi-ai 依赖",
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
        register_hook("on_session_reset", _on_session_reset_hook)
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
