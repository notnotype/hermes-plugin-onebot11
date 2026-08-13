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
from dataclasses import dataclass, replace
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
QueueError = _proto.queue.QueueError
QueueLease = _proto.QueueLease
QueueMessage = _proto.QueueMessage
QueueStore = _proto.QueueStore
ReactionRecord = _proto.queue.ReactionRecord
MediaDeliveryScope = _proto.media.MediaDeliveryScope
TriggerRequest = _proto.TriggerRequest
TurnBinding = _proto.TurnBinding
TurnBindingStore = _proto.TurnBindingStore
WRITE_TOOLS = _proto.permissions.WRITE_TOOLS
READ_ONLY_TOOLS = _proto.permissions.READ_ONLY_TOOLS
ALL_TOOLS = _proto.permissions.ALL_TOOLS
ROLE_NAMES = _proto.permissions.ROLE_NAMES
FORBIDDEN_TOOL_NAMES = _proto.permissions.FORBIDDEN_TOOL_NAMES
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
terminal_writes_sensitive_config = _proto.permissions.terminal_writes_sensitive_config
file_tool_writes_sensitive_config = _proto.permissions.file_tool_writes_sensitive_config
file_tool_reads_sensitive_config = _proto.permissions.file_tool_reads_sensitive_config
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
is_question = _proto.triggers.is_question
PiAiTriggerClient = _proto.pi_ai.PiAiTriggerClient
PiAiTriggerError = _proto.pi_ai.PiAiTriggerError
ConversationCommand = _proto.commands.ConversationCommand
parse_conversation_command = _proto.commands.parse_conversation_command
LayeredTriggerState = _proto.triggers.LayeredTriggerState
TriggerAction = _proto.triggers.TriggerAction
parse_llm_decision = _proto.triggers.parse_llm_decision
build_agent_context = _proto.context.build_agent_context
build_agent_context_parts = _proto.context.build_agent_context_parts
format_onebot_text = _proto.formatting.format_onebot_text
unwrap_markdown_image_markers = _proto.formatting.unwrap_markdown_image_markers
should_trigger = _proto.triggers.should_trigger
ReverseWsServer = _proto.ws_server.ReverseWsServer
parse_runtime_config = _proto.config.parse_runtime_config
RuntimePolicySnapshot = _proto.config.RuntimePolicySnapshot
build_policy_snapshot = _proto.config.build_policy_snapshot
runtime_static_fingerprint = _proto.config.runtime_static_fingerprint
roles_file_path = _proto.config.roles_file_path
FormattedText = _proto.formatting.FormattedText

logger = logging.getLogger(__name__)
_PLATFORM_NAME = "onebot11"
_PROCESSING_REACTION_EMOJI_ID = "128172"  # LLBot 的 QQ Emoji「💬」ID，表示正在回复
_QUEUED_REACTION_EMOJI_ID = "128064"  # LLBot 的 QQ Emoji「👀」ID，表示 selector 正在查看
_REQUIRED_HERMES_HOOKS = frozenset(
    {"pre_gateway_dispatch", "pre_llm_call", "pre_tool_call"}
)
_CONTROL_PLANE_KINDS = frozenset({"long_running", "system_error_notice"})
_CURRENT_CALLER: contextvars.ContextVar[CallerContext | None] = contextvars.ContextVar(
    "onebot11_current_caller", default=None
)
_CURRENT_BINDING: contextvars.ContextVar[TurnBinding | None] = contextvars.ContextVar(
    "onebot11_current_turn_binding", default=None
)
_CURRENT_EVENT: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "onebot11_current_event", default=None
)
# 这是插件自己的平台 lineage 标记。Hermes 的 delegated child 会在新线程
# 中复制父 ContextVar；仅依赖 child 的 ``platform=subagent`` 不足以判断它
# 是否来自 OneBot turn。标记为 True 时，generic 工具也必须经过 OneBot
# binding/lease/敏感文件门禁；普通平台的子代理保持 Hermes 自己的工具策略。
_CURRENT_ONEBOT_CONTEXT: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "onebot11_current_context", default=False
)
_CURRENT_RESET_MARKER: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "onebot11_current_reset_marker", default=None
)
_FINAL_DELIVERY: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "onebot11_final_delivery", default=False
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


@dataclass
class DeliverySummary:
    """记录一个 managed turn 的文本/媒体出站结算。"""

    attempted: int = 0
    successful: int = 0
    known_failed: int = 0
    unknown: int = 0
    fenced: int = 0
    last_text: str = ""

    @property
    def all_successful(self) -> bool:
        """只有至少一个 delivery unit 且全部明确成功时才返回 True。"""
        return (
            self.attempted > 0
            and self.successful == self.attempted
            and self.known_failed == 0
            and self.unknown == 0
            and self.fenced == 0
        )

    @property
    def has_partial_or_unknown(self) -> bool:
        """判断是否发生部分成功、未知结果或 fencing。"""
        return self.unknown > 0 or self.fenced > 0 or (
            self.successful > 0 and self.known_failed > 0
        )


def _platform() -> Platform:
    """惰性解析平台枚举，避免 register 前导入时 registry 尚未注册。"""
    return Platform(_PLATFORM_NAME)


def _platform_value(value: Any) -> str:
    """读取 Hermes Platform 或字符串的稳定值。"""
    return str(getattr(value, "value", value) or "").casefold()


def _is_control_plane_metadata(value: Any) -> bool:
    """只接受 Hermes 明确标记的控制面通知 metadata。"""
    if not isinstance(value, Mapping):
        return False
    if value.get("hermes_system_error_notice") is True:
        return True
    return (
        value.get("hermes_control_plane") is True
        and str(value.get("hermes_control_kind") or "").casefold()
        in _CONTROL_PLANE_KINDS
    )


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


def _extract_onebot_extra(config: Any) -> dict[str, Any] | None:
    """从 Hermes 当前配置结果提取 OneBot extra，不复制 YAML 合并规则。"""

    def extract_block(block: Any) -> dict[str, Any] | None:
        """读取 mapping 或 Hermes PlatformConfig 的 extra。"""
        if isinstance(block, Mapping):
            result: dict[str, Any] = {}
            raw_extra = block.get("extra")
            if isinstance(raw_extra, Mapping):
                result.update(raw_extra)
            for key, value in block.items():
                if key != "extra" and key not in {"enabled", "token", "api_key"}:
                    result.setdefault(str(key), value)
            return result
        raw_extra = getattr(block, "extra", None)
        if isinstance(raw_extra, Mapping):
            return dict(raw_extra)
        return None

    if isinstance(config, Mapping):
        platforms = config.get("platforms")
        candidates: list[Any] = [
            platforms.get("onebot11") if isinstance(platforms, Mapping) else None,
            config.get("onebot11"),
            config.get("gateway", {}).get("platforms", {}).get("onebot11")
            if isinstance(config.get("gateway"), Mapping)
            and isinstance(config.get("gateway", {}).get("platforms"), Mapping)
            else None,
        ]
        for block in candidates:
            result = extract_block(block)
            if result is not None:
                return result

    # ``gateway.config.load_gateway_config()`` returns a GatewayConfig
    # dataclass whose platform map is keyed by Hermes' Platform enum, not a
    # plain YAML mapping.  Reading its already-merged PlatformConfig keeps
    # this adapter from reimplementing Hermes' YAML precedence rules.
    platforms = getattr(config, "platforms", None)
    if isinstance(platforms, Mapping):
        for platform, block in platforms.items():
            platform_name = str(getattr(platform, "value", platform)).casefold()
            if platform_name == _PLATFORM_NAME:
                result = extract_block(block)
                if result is not None:
                    return result
    return None


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


def _binding_key_from_metadata(value: Any) -> tuple[str, str] | None:
    """只读取 metadata 中显式的 session/turn binding key，不做推断。"""
    if not isinstance(value, Mapping):
        return None
    raw_key = value.get("onebot11_binding_key")
    if not isinstance(raw_key, Mapping):
        return None
    session_id = str(raw_key.get("session_id") or "").strip()
    turn_id = str(raw_key.get("turn_id") or "").strip()
    if not session_id or not turn_id:
        return None
    return session_id, turn_id


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
    if not isinstance(role, str) or role not in ROLE_NAMES:
        return None
    if not isinstance(raw_tools, (list, tuple, set, frozenset)) or any(
        not isinstance(tool, str) for tool in raw_tools
    ):
        return None
    allowed_tools = frozenset(
        str(tool).strip()
        for tool in raw_tools
        if str(tool).strip() and str(tool).strip() not in FORBIDDEN_TOOL_NAMES
    )
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
    # OneBot 11 没有可靠的原地编辑合同；Hermes 据此跳过 token
    # streaming，避免把半截回复和最终回复发送成两条永久消息。
    SUPPORTS_MESSAGE_EDITING = False

    def __init__(self, config: PlatformConfig) -> None:
        """读取并校验配置，初始化协议客户端和群级状态机。"""
        raw_extra = {} if config.extra is None else dict(config.extra)
        runtime = parse_runtime_config(
            raw_extra,
            os.environ,
        )
        self._config_extra_source = dict(raw_extra)
        self._runtime_config = runtime
        self._policy_snapshot: RuntimePolicySnapshot = build_policy_snapshot(
            runtime,
            version=1,
            loaded_at=time.time(),
        )
        self._policy_reload_error: str | None = None
        self._policy_reload_lock = asyncio.Lock()
        extra = dict(runtime.extra)
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

        self.require_mention = runtime.trigger_config.require_mention
        self._last_trigger_at: dict[str, float] = {}
        self._llm_trigger_tasks: dict[str, asyncio.Task[None]] = {}
        self._trigger_timer_tasks: dict[str, asyncio.Task[None]] = {}
        self._trigger_state_locks: dict[str, asyncio.Lock] = {}
        self._trigger_states: dict[str, LayeredTriggerState] = {}
        self._selector_gave_up_chats: set[str] = set()
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
        self._media_source_roots = tuple(
            Path(root).resolve(strict=False)
            for root in runtime.media_source_roots
        )

        self._hermes_home = _resolve_hermes_home()
        self._policy_source_signature = self._policy_config_signature()
        self._policy_failed_signature: tuple[object, ...] | None = None
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
            recovery_cooldown_seconds=runtime.trigger_config.cooldown_seconds,
            can_dispatch=self._can_dispatch_chat,
            on_lease_lost=self._on_lease_lost,
            recovery_chat_ids=lambda: (
                frozenset(self.allowed_groups)
                if self.allowed_groups
                else None
            ),
            on_recovery_wakeup=self._recover_trigger_policy,
        )
        self._bindings = TurnBindingStore()
        self._binding_diagnostic_keys: set[tuple[str, str, str, str]] = set()
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
        self._delivery_summaries: dict[str, DeliverySummary] = {}
        self._last_bot_message_ids: dict[str, str] = {}
        self._processing_reaction_message_ids: dict[str, str] = {}
        self._queued_reaction_message_ids: dict[str, tuple[str, str]] = {}
        self._queued_reaction_attempted: dict[str, tuple[str, str]] = {}
        self._queued_reaction_tasks: dict[str, asyncio.Task[None]] = {}
        self._reaction_recovery_task: asyncio.Task[None] | None = None
        self._fenced_leases: set[str] = set()
        self._lease_session_keys: dict[str, str] = {}
        self._pending_completions: dict[str, tuple[ProcessingOutcome, bool, bool, str | None]] = {}
        self._pending_session_resets: list[_PendingSessionReset] = []
        self._session_reset_tasks: set[asyncio.Task[None]] = set()
        self._resetting_groups: set[str] = set()
        self._conversation_reset_generations: dict[str, int] = {}
        self._media_delivery_scopes: dict[str, MediaDeliveryScope] = {}
        self._control_plane_sent_scopes: set[str] = set()
        self._long_running_notice_tasks: dict[str, asyncio.Task[None]] = {}
        self._long_running_notice_events: dict[str, Any] = {}
        self._outbound_gate = asyncio.Lock()
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

    @property
    def policy_snapshot(self) -> RuntimePolicySnapshot:
        """返回当前不可变 policy snapshot。"""
        return self._policy_snapshot

    def _replace_policy(self, **changes: Any) -> None:
        """以单次指针替换更新兼容字段，避免读到半套权限配置。"""
        self._policy_snapshot = replace(self._policy_snapshot, **changes)

    @property
    def _access_policy(self) -> Any:
        """兼容旧代码读取当前访问策略。"""
        return self._policy_snapshot.access_policy

    @_access_policy.setter
    def _access_policy(self, value: Any) -> None:
        """兼容旧代码替换访问策略。"""
        self._replace_policy(access_policy=value)

    @property
    def allowed_groups(self) -> frozenset[str]:
        """读取当前群白名单。"""
        return self._policy_snapshot.access_policy.allowed_groups

    @allowed_groups.setter
    def allowed_groups(self, value: Any) -> None:
        """兼容测试和旧调用方更新群白名单。"""
        self._replace_policy(
            access_policy=replace(
                self._policy_snapshot.access_policy,
                allowed_groups=frozenset(str(item) for item in value),
            )
        )

    @property
    def allowed_users(self) -> frozenset[str]:
        """读取当前私聊白名单。"""
        return self._policy_snapshot.access_policy.allowed_users

    @allowed_users.setter
    def allowed_users(self, value: Any) -> None:
        """兼容测试和旧调用方更新私聊白名单。"""
        self._replace_policy(
            access_policy=replace(
                self._policy_snapshot.access_policy,
                allowed_users=frozenset(str(item) for item in value),
            )
        )

    @property
    def dm_policy(self) -> str:
        """读取当前私聊策略。"""
        return self._policy_snapshot.access_policy.dm_policy

    @dm_policy.setter
    def dm_policy(self, value: str) -> None:
        """兼容旧调用方更新私聊策略。"""
        self._replace_policy(
            access_policy=replace(
                self._policy_snapshot.access_policy,
                dm_policy=str(value).casefold(),
            )
        )

    @property
    def _allow_all_users(self) -> bool:
        """读取当前显式 allow-all 开关。"""
        return self._policy_snapshot.access_policy.allow_all_users

    @_allow_all_users.setter
    def _allow_all_users(self, value: bool) -> None:
        """兼容旧调用方更新显式 allow-all 开关。"""
        self._replace_policy(
            access_policy=replace(
                self._policy_snapshot.access_policy,
                allow_all_users=bool(value),
            )
        )

    @property
    def super_admins(self) -> frozenset[str]:
        """读取当前超级管理员集合。"""
        return self._policy_snapshot.super_admins

    @super_admins.setter
    def super_admins(self, value: Any) -> None:
        """兼容旧调用方更新超级管理员集合。"""
        self._replace_policy(super_admins=frozenset(str(item) for item in value))

    @property
    def trusted_users(self) -> frozenset[str]:
        """读取当前可信用户集合。"""
        return self._policy_snapshot.trusted_users

    @trusted_users.setter
    def trusted_users(self, value: Any) -> None:
        """兼容旧调用方更新可信用户集合。"""
        self._replace_policy(trusted_users=frozenset(str(item) for item in value))

    @property
    def role_tools(self) -> Mapping[str, frozenset[str]]:
        """读取当前角色工具 catalog。"""
        return self._policy_snapshot.role_tools

    @role_tools.setter
    def role_tools(self, value: Mapping[str, Any]) -> None:
        """兼容旧调用方替换角色工具 catalog。"""
        self._replace_policy(
            role_tools={
                str(role): frozenset(str(tool) for tool in tools)
                for role, tools in value.items()
            }
        )

    @property
    def trigger_config(self) -> Any:
        """读取当前触发配置。"""
        return self._policy_snapshot.trigger_config

    @trigger_config.setter
    def trigger_config(self, value: Any) -> None:
        """兼容旧调用方替换触发配置。"""
        self._replace_policy(trigger_config=value)

    @property
    def _processing_reaction_enabled(self) -> bool:
        """读取当前 reaction 开关。"""
        return self._policy_snapshot.processing_reaction_enabled

    @_processing_reaction_enabled.setter
    def _processing_reaction_enabled(self, value: bool) -> None:
        """兼容旧调用方更新 reaction 开关。"""
        self._replace_policy(processing_reaction_enabled=bool(value))

    @property
    def _processing_reaction_emoji_id(self) -> str:
        """读取当前 reaction emoji ID。"""
        return self._policy_snapshot.processing_reaction_emoji_id

    @_processing_reaction_emoji_id.setter
    def _processing_reaction_emoji_id(self, value: str) -> None:
        """兼容旧调用方更新 reaction emoji ID。"""
        self._replace_policy(processing_reaction_emoji_id=str(value))

    @property
    def plain_text_enabled(self) -> bool:
        """读取当前纯文本显示开关。"""
        return self._policy_snapshot.plain_text_enabled

    @property
    def show_interim_group(self) -> bool:
        """读取群聊是否展示 Hermes 中间正文（commentary/progress）。"""
        return self._policy_snapshot.show_interim_group

    @show_interim_group.setter
    def show_interim_group(self, value: Any) -> None:
        """兼容热更新设置群聊中间正文开关。"""
        self._replace_policy(show_interim_group=bool(value))

    @property
    def show_interim_dm(self) -> bool:
        """读取私聊是否展示 Hermes 中间正文（commentary/progress）。"""
        return self._policy_snapshot.show_interim_dm

    @show_interim_dm.setter
    def show_interim_dm(self, value: Any) -> None:
        """兼容热更新设置私聊中间正文开关。"""
        self._replace_policy(show_interim_dm=bool(value))

    def _interim_allowed(self, target: ChatTarget) -> bool:
        """按目标类型返回是否展示 Hermes 中间消息。"""
        return bool(
            self.show_interim_dm if target.chat_type == "dm" else self.show_interim_group
        )

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
        self._start_reaction_recovery()
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
        self._delivery_summaries.clear()
        self._last_bot_message_ids.clear()
        self._lease_session_keys.clear()
        self._pending_completions.clear()
        self._processing_reaction_message_ids.clear()
        self._queued_reaction_message_ids.clear()
        self._queued_reaction_attempted.clear()
        self._queued_reaction_tasks.clear()
        self._binding_diagnostic_keys.clear()
        self._pending_session_resets.clear()
        self._resetting_groups.clear()
        self._conversation_reset_generations.clear()
        self._media_delivery_scopes.clear()
        self._control_plane_sent_scopes.clear()
        self._long_running_notice_tasks.clear()
        self._long_running_notice_events.clear()
        self._bindings.clear()

    def _policy_config_signature(self) -> tuple[object, ...] | None:
        """读取 Hermes config.yaml 与独立 roles 文件的轻量签名。"""
        config_path = self._hermes_home / "config.yaml"
        try:
            config_stat = config_path.stat()
        except OSError:
            return None
        signature = (
            str(config_path),
            int(config_stat.st_mtime_ns),
            int(config_stat.st_size),
        )
        roles_path = roles_file_path(self._runtime_config.extra, os.environ)
        try:
            roles_stat = roles_path.stat()
            signature = signature + (
                str(roles_path),
                int(roles_stat.st_mtime_ns),
                int(roles_stat.st_size),
            )
        except OSError:
            pass
        return signature

    def _load_reload_extra(self) -> dict[str, Any]:
        """通过 Hermes loader 读取当前配置，失败时回退到初始 extra。"""
        loaded: dict[str, Any] | None = None
        try:
            from gateway.config import load_gateway_config

            loaded = _extract_onebot_extra(load_gateway_config())
        except ImportError:
            # 兼容没有 gateway.config 的旧 Hermes；正常 Hermes 运行时
            # 应优先走上面的 gateway loader。
            pass
        if loaded is None:
            try:
                from hermes_cli.config import load_config

                loaded = _extract_onebot_extra(load_config())
            except ImportError:
                loaded = None
        if loaded is None:
            return dict(self._config_extra_source)

        hot_names = {
            "allowed_groups",
            "allowed_users",
            "dm_policy",
            "super_admins",
            "admins",
            "roles",
            "main_agent_read_only",
            "trusted_users",
            "trigger_keywords",
            "keywords",
            "always_trigger",
            "trigger_always",
            "trigger_cooldown_seconds",
            "trigger_cooldown",
            "require_mention",
            "question_trigger_enabled",
            "memory_trigger_enabled",
            "memory_trigger_words",
            "trigger_debounce_seconds",
            "engaged_idle_seconds",
            "engaged_max_seconds",
            "engaged_max_arbitrations",
            "llm_trigger",
            "trigger_llm",
            "llm_trigger_enabled",
            "llm_trigger_provider",
            "llm_trigger_model",
            "llm_trigger_base_url",
            "llm_trigger_api_key_env",
            "llm_trigger_groups",
            "processing_reaction_enabled",
            "processing_reaction_emoji_id",
            "plain_text_enabled",
            "long_running_notice_seconds",
        }
        candidate = {
            key: value
            for key, value in self._runtime_config.extra.items()
            if key not in hot_names
        }
        candidate.update(loaded)
        return candidate

    async def reload_policy(self, *, force: bool = True) -> tuple[bool, str]:
        """原子替换运行时策略；静态连接和队列配置变化必须重启。"""
        if self._closed:
            return False, "OneBot11 adapter 已关闭"
        async with self._policy_reload_lock:
            signature = self._policy_config_signature()
            if (
                not force
                and signature is not None
                and signature == self._policy_source_signature
            ):
                return True, "配置未变化"
            try:
                candidate_extra = await asyncio.to_thread(self._load_reload_extra)
                candidate_runtime = parse_runtime_config(
                    candidate_extra,
                    os.environ,
                )
                if runtime_static_fingerprint(candidate_runtime) != runtime_static_fingerprint(
                    self._runtime_config
                ):
                    raise ValueError("连接、队列或协议配置已变化；这些字段需要重启生效")

                await self._cancel_trigger_tasks()
                for state in self._trigger_states.values():
                    state.invalidate_judgement()
                    state.config = candidate_runtime.trigger_config

                next_version = self._policy_snapshot.version + 1
                snapshot = build_policy_snapshot(
                    candidate_runtime,
                    version=next_version,
                    loaded_at=time.time(),
                )
                self._runtime_config = candidate_runtime
                self._config_extra_source = dict(candidate_extra)
                self._policy_snapshot = snapshot
                self.require_mention = candidate_runtime.trigger_config.require_mention
                self._dispatcher.recovery_cooldown_seconds = (
                    candidate_runtime.trigger_config.cooldown_seconds
                )
                self._policy_reload_error = None
                self._policy_source_signature = signature
                self._policy_failed_signature = None
                self._llm_trigger_semaphore = None
                self._llm_trigger_loop = None
                self._confirmations.clear()
                _safe_audit(
                    self,
                    "policy_reload",
                    {
                        "version": snapshot.version,
                        "loaded_at": snapshot.loaded_at,
                    },
                )

                pending_chats = await asyncio.to_thread(self._queue.pending_chat_ids)
                for chat_id in pending_chats:
                    if not self._closed:
                        await self._restore_trigger_state(chat_id)
                return True, f"策略已生效，version={snapshot.version}"
            except (ImportError, OSError, TypeError, ValueError, RuntimeError) as exc:
                message = f"{type(exc).__name__}: {str(exc)[:240]}"
                self._policy_reload_error = message
                self._policy_failed_signature = signature
                _safe_audit(
                    self,
                    "policy_reload_failed",
                    {"error": message},
                )
                return False, message

    async def _maybe_reload_policy(self) -> None:
        """在配置文件变化后自动尝试一次策略 reload。"""
        signature = self._policy_config_signature()
        if signature is None or signature == self._policy_source_signature:
            return
        if signature == self._policy_failed_signature:
            return
        await self.reload_policy(force=True)

    async def _stop_runtime(self, *, mark_disconnected: bool) -> None:
        """按停止、结算、fence、清理顺序关闭当前 runtime。"""
        # 先切换 epoch，再取消旧任务；即使某个旧 task 延迟响应，
        # 也不能在 reconnect 后重新建立 DM 身份绑定。
        self._fenced_leases.update(
            lease.lease_id for lease in self._dispatcher.active_leases()
        )
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
        await self._cancel_all_long_running_notices()
        await self._cancel_reaction_recovery()
        cancel_background = getattr(self, "cancel_background_tasks", None)
        if callable(cancel_background):
            try:
                await cancel_background()
            except Exception:
                logger.warning("OneBot11 Hermes background task 清理失败", exc_info=True)

        await self._dispatcher.close()

        # 等待已经拿到出站 gate 的请求自然结束；之后关闭状态下不再
        # 允许新的业务出站，reaction 清理由显式 shutdown 路径负责。
        try:
            async with self._outbound_gate:
                pass
        except Exception:
            logger.warning("OneBot11 出站 gate 收口失败", exc_info=True)

        # QueueStore 仍可用时先清理持久 reaction；如果白名单已经收紧，
        # _clear_processing_reaction 只删除本地记录，不访问 OneBot。
        if not self._queue.closed:
            try:
                await asyncio.wait_for(
                    self._clear_all_processing_reactions(allow_shutdown=True),
                    timeout=2.0,
                )
                await asyncio.wait_for(
                    self._clear_all_queued_reactions(allow_shutdown=True),
                    timeout=2.0,
                )
            except Exception:
                logger.warning("OneBot11 disconnect 清理 reaction 失败", exc_info=True)

            # reaction 清理完成后再结算 owner lease；旧 turn 的 completion
            # 会因 adapter epoch/fencing 只清理内存，不会写入新状态。
            try:
                await asyncio.to_thread(self._queue.abandon_owner_leases)
            except Exception:
                logger.warning("OneBot11 owner lease 结算失败", exc_info=True)
            finally:
                self._queue.close()
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
        await self._maybe_reload_policy()
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
        if conversation_command is not None and conversation_command.name == "context":
            await self._handle_context_command(event)
            return
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

    async def _handle_context_command(
        self,
        event: _proto.events.InboundEvent,
    ) -> None:
        """在入队前返回有界诊断，不读取 transcript 或 system prompt。"""
        active = (
            self._dispatcher.active(str(event.chat_id))
            if event.chat_type == "group"
            else None
        )
        if event.chat_type == "group":
            status = await asyncio.to_thread(self._queue.status, str(event.chat_id))
            active_info = (
                {
                    "lease_id": active.lease.lease_id,
                    "anchor_id": active.lease.trigger.request_id,
                    "anchor_seq": active.lease.trigger.anchor_seq,
                    "phase": active.lease.phase,
                    "lease_lost": active.lease_lost,
                }
                if active is not None
                else None
            )
            diagnostic = {
                "target": {"chat_type": "group", "chat_id": str(event.chat_id)},
                "pending": int(status.get("pending", 0)),
                "leased": int(status.get("leased", 0)),
                "active": active_info,
                "summary_present": bool(str(status.get("summary") or "")),
                "failed": int(status.get("failed", 0)),
                "uncertain": int(status.get("uncertain", 0)),
                "failure_reasons": [str(item)[:160] for item in status.get("failure_reasons", [])[:8]],
                "uncertain_reasons": [
                    str(item)[:160] for item in status.get("uncertain_reasons", [])[:8]
                ],
                "paused": bool(status.get("paused")),
            }
        else:
            diagnostic = {
                "target": {"chat_type": "dm", "chat_id": str(event.chat_id)},
                "pending": 0,
                "leased": 0,
                "active": None,
                "summary_present": False,
                "failed": 0,
                "uncertain": 0,
                "failure_reasons": [],
                "uncertain_reasons": [],
                "paused": False,
            }
        diagnostic["policy"] = {
            "version": self.policy_snapshot.version,
            "loaded_at": self.policy_snapshot.loaded_at,
            "reload_error": self._policy_reload_error,
        }
        diagnostic["hermes_context_usage"] = None
        self._audit.record(
            "context_command",
            {
                "chat_type": event.chat_type,
                "chat_id": str(event.chat_id),
                "user_id": str(event.user_id),
            },
        )
        await self._send_direct(event, json.dumps(diagnostic, ensure_ascii=False))

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
        onebot_context_token = _CURRENT_ONEBOT_CONTEXT.set(True)
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
            _CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
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
        await self._clear_processing_reaction(
            lease.lease_id,
            allow_recovery=True,
        )
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
            "onebot11_message_id": ev.message_id,
            "onebot11_images": ev.images[: self._max_images_per_message],
            "onebot11_image_urls": ev.image_urls[: self._max_images_per_message],
            "onebot11_image_files": ev.image_files[: self._max_images_per_message],
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
        """下载 URL 或通过 get_image 解析受控 file 标识。"""
        source = str(image or "").strip()
        target_dir = dest_dir or self._media_dir
        if source.startswith(("http://", "https://")):
            return await self._api.download_to_temp(source, target_dir)
        if not self._media_source_roots:
            return None
        try:
            resolved_source = await self._api.get_image(source)
        except (OneBotApiError, OSError, ValueError):
            return None
        if not resolved_source:
            return None
        return await asyncio.to_thread(
            self._copy_checked_media_source,
            resolved_source,
            target_dir,
        )

    def _copy_checked_media_source(
        self,
        source: str,
        dest_dir: str,
    ) -> str | None:
        """只复制显式媒体根目录内且通过图片校验的 get_image 文件。"""
        try:
            resolved = Path(str(source)).expanduser().resolve(strict=True)
            if not resolved.is_file() or not any(
                resolved.is_relative_to(root)
                for root in self._media_source_roots
            ):
                return None
            if resolved.stat().st_size > self._api.max_media_bytes:
                return None
            data = resolved.read_bytes()
            if len(data) > self._api.max_media_bytes or not matches_image_magic(
                data,
                "",
                resolved.name,
            ):
                return None
            suffix = ".bin"
            if data.startswith(b"\x89PNG"):
                suffix = ".png"
            elif data.startswith(b"\xff\xd8\xff"):
                suffix = ".jpg"
            elif data.startswith((b"GIF87a", b"GIF89a")):
                suffix = ".gif"
            elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
                suffix = ".webp"
            destination = Path(dest_dir) / f"{uuid.uuid4().hex}{suffix}"
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
            return str(destination)
        except OSError:
            return None

    def _new_media_dir(self) -> str:
        """为一个 turn 创建受控媒体目录，便于完成后精确回收。"""
        return tempfile.mkdtemp(prefix=self._media_prefix, dir=str(self._media_root))

    def _media_scope_key(self, metadata: Mapping[str, Any] | None = None) -> str | None:
        """按 lease 或精确 session/turn 计算同轮媒体 scope。"""
        sources: list[Mapping[str, Any]] = []
        if isinstance(metadata, Mapping):
            sources.append(metadata)
        current_event = _CURRENT_EVENT.get()
        current_metadata = getattr(current_event, "metadata", None) or {}
        if isinstance(current_metadata, Mapping) and current_metadata is not metadata:
            sources.append(current_metadata)

        binding = self._binding_from_context(metadata)
        if binding is not None:
            if binding.lease_id:
                return f"lease:{binding.lease_id}"
            return f"turn:{binding.session_id}:{binding.turn_id}"

        for source in sources:
            lease_id = str(source.get("onebot11_lease_id") or "").strip()
            if lease_id:
                return f"lease:{lease_id}"
        for source in sources:
            raw_key = source.get("onebot11_binding_key")
            if isinstance(raw_key, Mapping):
                session_id = str(raw_key.get("session_id") or "").strip()
                turn_id = str(raw_key.get("turn_id") or "").strip()
                if session_id and turn_id:
                    return f"turn:{session_id}:{turn_id}"
        for source in sources:
            message_id = str(
                source.get("onebot11_message_id")
                or source.get("message_id")
                or ""
            ).strip()
            if message_id:
                return f"message:{message_id}"
        event_message_id = str(getattr(current_event, "message_id", "") or "").strip()
        return f"message:{event_message_id}" if event_message_id else None

    def _media_scope_for(
        self,
        metadata: Mapping[str, Any] | None = None,
        *,
        create: bool = True,
    ) -> MediaDeliveryScope | None:
        """读取或创建一个短生命周期的媒体去重 scope。"""
        key = self._media_scope_key(metadata)
        if key is None:
            return None
        scope = self._media_delivery_scopes.get(key)
        if scope is None and create:
            scope = MediaDeliveryScope(key)
            self._media_delivery_scopes[key] = scope
        return scope

    def _clear_media_scope(
        self,
        metadata: Mapping[str, Any] | None = None,
        *,
        lease_id: str | None = None,
    ) -> None:
        """在 turn 完成或断开时回收内存媒体去重状态。"""
        key = f"lease:{lease_id}" if lease_id else self._media_scope_key(metadata)
        if key is None:
            return
        scope = self._media_delivery_scopes.pop(key, None)
        if scope is not None:
            scope.clear()
        stale_control_scopes = {
            entry
            for entry in self._control_plane_sent_scopes
            if entry.startswith(f"{key}:control:")
        }
        self._control_plane_sent_scopes.difference_update(stale_control_scopes)

    def _control_plane_scope_key(
        self,
        metadata: Mapping[str, Any],
    ) -> str | None:
        """按当前 turn 生成控制面通知的有限去重键。"""
        scope = self._media_scope_key(metadata)
        if scope is None:
            return None
        if metadata.get("hermes_system_error_notice") is True:
            kind = "system_error_notice"
        else:
            kind = str(metadata.get("hermes_control_kind") or "").casefold()
        if kind not in _CONTROL_PLANE_KINDS:
            return None
        return f"{scope}:control:{kind}"

    def _schedule_long_running_notice(self, event: Any, lease_id: str) -> None:
        """为一个活动群 turn 安排一次性长时间处理提示（保存 event 供重置复用）。"""
        delay = float(self.policy_snapshot.long_running_notice_seconds)
        normalized_lease_id = str(lease_id or "").strip()
        if self._closed or delay <= 0 or not normalized_lease_id:
            return
        current = self._long_running_notice_tasks.get(normalized_lease_id)
        if current is not None and not current.done():
            return
        try:
            self._long_running_notice_events[normalized_lease_id] = event
            task = asyncio.create_task(
                self._send_long_running_notice_after_delay(
                    event,
                    normalized_lease_id,
                    delay,
                )
            )
        except RuntimeError:
            logger.debug("OneBot11 无法创建长时间处理提示 task", exc_info=True)
            return
        self._long_running_notice_tasks[normalized_lease_id] = task

    def _reset_long_running_notice(self, chat_id: str) -> None:
        """中间正文成功发送后重置当前活动 turn 的长时间处理计时器。"""
        normalized = str(chat_id or "").strip()
        if self._closed or not normalized:
            return
        active = self._dispatcher.active(normalized)
        if active is None:
            return
        lease_id = str(active.lease.lease_id or "").strip()
        if not lease_id:
            return
        current = self._long_running_notice_tasks.pop(lease_id, None)
        if current is not None and not current.done():
            # 只有取消一个仍在等待的计时器才重新排程；提示已经发出后
            # （task 已完成）不再重建，避免同一 turn 反复出现"仍在处理中"。
            current.cancel()
            event = self._long_running_notice_events.get(lease_id)
            if event is not None:
                self._schedule_long_running_notice(event, lease_id)

    async def _send_long_running_notice_after_delay(
        self,
        event: Any,
        lease_id: str,
        delay: float,
    ) -> None:
        """等待指定时间后发送一次控制面提示，不改变业务出站阶段。"""
        try:
            await asyncio.sleep(max(0.0, float(delay)))
            if self._closed or lease_id in self._fenced_leases:
                return
            if not self._lease_is_current(lease_id):
                return
            metadata = dict(getattr(event, "metadata", None) or {})
            target = metadata.get("onebot11_target")
            if not isinstance(target, Mapping):
                return
            chat_id = str(target.get("chat_id") or "")
            if not chat_id or not self._chat_access_allowed("group", chat_id):
                return
            # 控制面通知不需要 Hermes turn binding：lease 与访问策略已在
            # 上方校验，直接走 OneBot API 发送，避免 worker 线程 binding
            # 恢复失败导致提示永远发不出去。
            result = await self._send_notice_message(
                chat_id,
                "仍在处理中，请稍候…",
                reply_to=str(metadata.get("onebot11_anchor_message_id") or "") or None,
            )
            if not result.success:
                logger.info(
                    "OneBot11 长时间处理提示未发送: lease=%s error=%s",
                    lease_id,
                    result.error,
                )
                self._audit.record(
                    "long_running_notice",
                    {
                        "chat_id": chat_id,
                        "lease_id": lease_id,
                        "sent": False,
                        "error": str(result.error or "")[:200],
                    },
                )
            else:
                self._audit.record(
                    "long_running_notice",
                    {
                        "chat_id": chat_id,
                        "lease_id": lease_id,
                        "sent": True,
                    },
                )
        except asyncio.CancelledError:
            return
        except Exception:
            logger.info(
                "OneBot11 长时间处理提示失败: lease=%s",
                lease_id,
                exc_info=True,
            )
        finally:
            current = self._long_running_notice_tasks.get(lease_id)
            if current is asyncio.current_task():
                self._long_running_notice_tasks.pop(lease_id, None)

    async def _send_notice_message(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: str | None = None,
    ) -> SendResult:
        """发送一条不带 Hermes turn 身份的控制面提示消息。"""
        if self._closed or self._ws is None:
            return SendResult(False, error="OneBot11 adapter is closed", error_kind="not_found")
        try:
            async with self._outbound_gate:
                if self._closed:
                    return SendResult(False, error="OneBot11 adapter is closed", error_kind="not_found")
                sent_id = await self._api.send_message(
                    chat_id,
                    content,
                    chat_type="group",
                    reply_to=reply_to,
                )
                if not sent_id:
                    return SendResult(
                        False,
                        error="OneBot 成功响应缺少 message_id，出站结果未知",
                        error_kind="unknown",
                    )
                return SendResult(True, message_id=str(sent_id))
        except Exception as exc:
            return SendResult(
                False,
                error=f"OneBot 控制面提示发送失败: {exc}",
                error_kind="unknown",
            )

    async def _cancel_long_running_notice(self, lease_id: str) -> None:
        """取消一个 turn 的一次性长时间提示。"""
        normalized = str(lease_id)
        self._long_running_notice_events.pop(normalized, None)
        task = self._long_running_notice_tasks.pop(normalized, None)
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _cancel_all_long_running_notices(self) -> None:
        """取消所有尚未发送的长时间处理提示。"""
        tasks = list(self._long_running_notice_tasks.values())
        self._long_running_notice_tasks.clear()
        self._long_running_notice_events.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _deduplicated_media_result(fingerprint: str) -> SendResult:
        """构造不访问 OneBot 的重复媒体成功结果。"""
        return SendResult(
            True,
            raw_response={"deduplicated": True, "fingerprint": str(fingerprint)[:32]},
        )

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
        """返回真实 OneBot message_id；没有真实 ID 时保持空字符串。"""
        normalized = str(message_id or "").strip()
        if normalized:
            return normalized
        return ""

    def _stable_message_key(
        self,
        message_id: Any,
        *,
        chat_type: str,
        chat_id: str,
        text: str,
        metadata: Mapping[str, Any],
    ) -> str:
        """生成与真实 message_id 分离的稳定去重 key。"""
        normalized = str(message_id or "").strip()
        if normalized:
            return f"{str(chat_type)}:{normalized}"
        stable_metadata = {
            str(key): value
            for key, value in dict(metadata).items()
            if str(key) not in {
                "onebot11_caller_context",
                "onebot11_authority",
            }
        }
        payload = json.dumps(
            {
                "chat_id": str(chat_id),
                "chat_type": str(chat_type),
                "text": str(text),
                "metadata": stable_metadata,
            },
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
            "onebot11_image_urls": ev.image_urls[: self._max_images_per_message],
            "onebot11_image_files": ev.image_files[: self._max_images_per_message],
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
        message_key = str(getattr(ev, "message_key", "") or "").strip() or (
            self._stable_message_key(
                ev.message_id,
                chat_type="group",
                chat_id=ev.chat_id,
                text=ev.text,
                metadata=metadata,
            )
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
            message_key=message_key,
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
        clear_queued_reaction = False
        queued_reaction_message: QueueMessage | None = None
        async with self._trigger_lock_for(chat_id):
            before = await asyncio.to_thread(self._queue.status, chat_id)
            now = time.monotonic()
            wall_now = time.time()
            previous_trigger_at = before.get("last_trigger_at")
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
                result = await asyncio.to_thread(
                    self._queue.enqueue,
                    message,
                    trigger,
                    triggered_at=wall_now if decision.triggered else None,
                )
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
                    clear_queued_reaction = True
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
                        user_id=str(message.user_id or ""),
                        reply_to_bot=self._message_replies_to_bot(
                            chat_id,
                            str(message.metadata.get("onebot11_reply_to") or ""),
                        ),
                    )
                    if action.kind == "schedule":
                        await self._apply_trigger_action_locked(chat_id, action)
                        if state.mode == "debounce":
                            queued_reaction_message = message
                if decision.triggered or action.kind == "direct":
                    if decision.triggered:
                        self._last_trigger_at[chat_id] = wall_now
                        state = self._trigger_states.get(chat_id)
                        if state is not None:
                            state.invalidate_judgement()
                            cancel_judgement = True
                    if decision.triggered:
                        clear_queued_reaction = True
                    should_notify = True

        if cancel_judgement:
            self._cancel_llm_judgement(chat_id)
        if queued_reaction_message is not None:
            self._schedule_queued_reaction(chat_id, queued_reaction_message)
        elif clear_queued_reaction:
            self._schedule_clear_queued_reaction(chat_id)
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
            onebot_context_token = _CURRENT_ONEBOT_CONTEXT.set(True)
            try:
                await super().handle_message(event)
            finally:
                _CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
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
        message_key = self._stable_message_key(
            event.message_id,
            chat_type="group",
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
            message_key=message_key,
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

    async def _selector_give_up(
        self,
        chat_id: str,
        *,
        status: Mapping[str, Any],
        candidate_type: str,
        revision: int | None,
        generation: int | None = None,
    ) -> bool:
        """selector 连续失败达到上限后放弃自动重试；返回是否应停止判断。

        消息保留 pending，清理 👀 并审计一次；下一次新消息入队会重置
        SQLite 失败计数，从而重新允许判断。
        """
        normalized = str(chat_id)
        llm_failures = int(status.get("llm_failure_count", 0) or 0)
        if llm_failures < self.trigger_config.llm_max_failures:
            self._selector_gave_up_chats.discard(normalized)
            return False
        if normalized in self._selector_gave_up_chats:
            return True
        self._selector_gave_up_chats.add(normalized)
        state = self._trigger_states.get(normalized)
        if state is not None:
            # 不能走 on_llm_failure：dirty_revision 存在时会重新安排判断，
            # 违背 give_up 语义。直接失效 pending judgement 并保留 engaged
            # 窗口本身。
            state.invalidate_judgement()
        self._schedule_clear_queued_reaction(normalized)
        self._audit.record(
            "llm_trigger",
            {
                "chat_id": normalized,
                "candidate_type": candidate_type or "candidate",
                "candidate_message_key": None,
                "candidate_seq": None,
                "queue_revision": int(revision or 0),
                "pending": int(status.get("pending", 0)),
                "input_bytes": 0,
                "decision": "ignore",
                "anchor_seq": None,
                "duration_ms": 0,
                "failure": "give_up",
                "llm_failure_count": llm_failures,
                "provider": self.trigger_config.llm_provider,
                "model": self.trigger_config.llm_model,
                "concurrency_waited": False,
                "concurrency_wait_ms": 0,
            },
        )
        return True

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
                if action.reason in {"wait_expired", "engaged_expired"}:
                    self._schedule_clear_queued_reaction(chat_id)
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
        if action.kind == "schedule":
            if await self._selector_give_up(
                normalized,
                status=status,
                candidate_type=action.candidate_type,
                revision=action.revision,
            ):
                return
            next_attempt_at = status.get("llm_next_attempt_at")
            if (
                next_attempt_at is not None
                and float(next_attempt_at) > time.time()
            ):
                due_at = time.monotonic() + max(
                    0.0,
                    float(next_attempt_at) - time.time(),
                )
                state.mode = "debounce"
                state.debounce_due = max(
                    state.debounce_due or due_at,
                    due_at,
                )
                state.dirty_revision = int(status.get("revision", 0))
                self._schedule_trigger_timer(normalized)
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
            await self._persist_llm_failure_locked(
                normalized,
                action,
                status=status,
                failure="provider_missing",
                observed_seq=None,
            )
            if state.engaged_until is not None:
                self._schedule_trigger_timer(normalized)
            self._audit.record(
                "llm_trigger_skip",
                {
                    "chat_id": normalized,
                    "candidate_type": action.candidate_type or "candidate",
                    "candidate_message_key": None,
                    "candidate_seq": None,
                    "queue_revision": int(action.revision or 0),
                    "reason": "provider_missing",
                    "pending": int(status.get("pending", 0)),
                    "input_bytes": 0,
                    "duration_ms": 0,
                    "decision": "ignore",
                    "anchor_seq": None,
                    "failure": "provider_missing",
                    "provider": self.trigger_config.llm_provider,
                    "model": self.trigger_config.llm_model,
                    "concurrency_waited": False,
                },
            )
            self._schedule_clear_queued_reaction(normalized)
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
            if await self._selector_give_up(
                normalized,
                status=status,
                candidate_type=action.candidate_type,
                revision=action.revision,
                generation=action.generation,
            ):
                return
            next_attempt_at = status.get("llm_next_attempt_at")
            if (
                next_attempt_at is not None
                and float(next_attempt_at) > time.time()
            ):
                # 失败退避是持久合同；内存 timer 只能把下一次尝试
                # 推迟到 SQLite 记录的 wall-clock 时间，不能重新消耗模型。
                state.mode = "debounce"
                state.debounce_due = time.monotonic() + max(
                    0.0,
                    float(next_attempt_at) - time.time(),
                )
                state.dirty_revision = int(status.get("revision", 0))
                self._schedule_trigger_timer(normalized)
                return
            if not self._llm_trigger_ready():
                failure = "provider_missing"
                state.on_llm_failure(
                    now=time.monotonic(),
                    current_revision=int(status.get("revision", 0)),
                    generation=action.generation,
                )
                await self._persist_llm_failure_locked(
                    normalized,
                    action,
                    status=status,
                    failure=failure,
                    observed_seq=None,
                )
                self._audit.record(
                    "llm_trigger",
                    {
                        "chat_id": normalized,
                        "candidate_type": action.candidate_type or "candidate",
                        "candidate_message_key": None,
                        "candidate_seq": None,
                        "queue_revision": int(action.revision or 0),
                        "pending": int(status.get("pending", 0)),
                        "input_bytes": 0,
                        "decision": "ignore",
                        "anchor_seq": None,
                        "duration_ms": 0,
                        "failure": failure,
                        "provider": self.trigger_config.llm_provider,
                        "model": self.trigger_config.llm_model,
                        "concurrency_waited": False,
                        "concurrency_wait_ms": 0,
                    },
                )
                self._schedule_clear_queued_reaction(normalized)
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
        anchor_seq: int | None = None,
        observed_seq: int | None = None,
    ) -> bool:
        """在群锁内安全创建旁路 trigger，释放锁后再启动 dispatcher。"""
        normalized = str(chat_id)
        async with self._trigger_lock_for(normalized):
            request_id = await self._create_llm_trigger_locked(
                normalized,
                expected_generation=expected_generation,
                expected_revision=expected_revision,
                anchor_seq=anchor_seq,
                observed_seq=observed_seq,
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
        anchor_seq: int | None = None,
        observed_seq: int | None = None,
    ) -> str | None:
        """在已持有群触发锁时创建旁路 trigger；调用方不得在此发网络请求。"""
        normalized = str(chat_id)
        if type(anchor_seq) is not int or anchor_seq <= 0:
            return None
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
        anchor = next((message for message in messages if message.seq == anchor_seq), None)
        if anchor is None:
            return None
        authority = self._authority_for_queued_message(anchor)
        request_id = await asyncio.to_thread(
            self._queue.create_message_anchor,
            normalized,
            anchor_seq,
            "llm",
            triggered_at=time.time(),
            llm_observed_seq=observed_seq,
            anchor_kind="selector",
            authority_role=authority.role,
            authority_tools=authority.allowed_tools,
            authority_self_id=authority.self_id,
        )
        if request_id:
            self._last_trigger_at[normalized] = time.time()
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
                triggered_at=time.time(),
            )
            has_request = request_id is not None or int(
                status.get("pending_trigger_requests", 0)
            ) > 0
        if cancel_judgement:
            self._cancel_llm_judgement(normalized)
        self._schedule_clear_queued_reaction(normalized)
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
        self._schedule_clear_queued_reaction(normalized, reset_attempted=True)
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
        if paused:
            self._schedule_clear_queued_reaction(normalized, reset_attempted=True)
        if not paused:
            await self._restore_trigger_state(normalized)
        return True

    async def _recover_trigger_policy(self) -> bool:
        """让 selector-aware 群恢复 pending 状态，避免 dispatcher 直建 anchor。"""
        if not self.trigger_config.llm_enabled:
            return False
        if self._closed:
            return True
        try:
            pending_chat_ids = await asyncio.to_thread(self._queue.pending_chat_ids)
        except QueueError:
            return True
        allowed_groups = frozenset(self.allowed_groups)
        for chat_id in pending_chat_ids:
            normalized = str(chat_id)
            if allowed_groups and normalized not in allowed_groups:
                continue
            try:
                chat_type = await asyncio.to_thread(self._queue.chat_type, normalized)
            except QueueError:
                return True
            if chat_type != "group":
                continue
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
                judged_seq = int(status.get("llm_judged_seq", 0) or 0)
                eligible_messages: list[QueueMessage] = []
                has_hard_trigger = False
                for message in messages:
                    hard_decision = should_trigger(
                        chat_type="group",
                        text=message.text,
                        mentioned_self=bool(
                            message.metadata.get("onebot11_mentioned_self")
                        ),
                        config=self.trigger_config,
                        last_trigger_at=None,
                        now=time.monotonic(),
                    )
                    if hard_decision.triggered:
                        has_hard_trigger = True
                    if hard_decision.triggered or (
                        message.seq is not None and int(message.seq) > judged_seq
                    ):
                        eligible_messages.append(message)
                if not eligible_messages:
                    return False
                next_attempt_at = status.get("llm_next_attempt_at")
                if (
                    next_attempt_at is not None
                    and float(next_attempt_at) > time.time()
                    and not has_hard_trigger
                ):
                    return False
                revision = int(status.get("revision", 0))
                last_trigger_at = status.get("last_trigger_at")
                cooldown_remaining = 0.0
                if (
                    last_trigger_at is not None
                    and self.trigger_config.cooldown_seconds > 0
                ):
                    cooldown_remaining = max(
                        0.0,
                        self.trigger_config.cooldown_seconds
                        - (time.time() - float(last_trigger_at)),
                    )
                action = TriggerAction("none", reason="restore")
                for index, message in enumerate(eligible_messages):
                    mentioned = bool(message.metadata.get("onebot11_mentioned_self"))
                    action = state.observe_message(
                        chat_type="group",
                        text=message.text,
                        mentioned_self=mentioned,
                        has_context=bool(status.get("summary") or index > 0),
                        revision=revision,
                        now=time.monotonic(),
                        last_trigger_at=last_trigger_at,
                        user_id=str(message.user_id or ""),
                        reply_to_bot=self._message_replies_to_bot(
                            normalized,
                            str(message.metadata.get("onebot11_reply_to") or ""),
                        ),
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
                            triggered_at=time.time(),
                        )
                        should_notify = bool(request_id)
                        if should_notify:
                            self._last_trigger_at[normalized] = time.time()
                        break
                if action.kind == "schedule":
                    if cooldown_remaining > 0:
                        due_at = time.monotonic() + cooldown_remaining
                        state.debounce_due = max(
                            state.debounce_due or due_at,
                            due_at,
                        )
                        self._schedule_trigger_timer(normalized)
                        self._schedule_queued_reaction(
                            normalized,
                            eligible_messages[-1],
                        )
                    else:
                        await self._apply_trigger_action_locked(normalized, action)
                        # give_up 会把状态机恢复为 idle，此时不能再给候选添加
                        # 👀，否则刚清理的 reaction 会被重新挂上。
                        if state.mode == "debounce":
                            self._schedule_queued_reaction(
                                normalized,
                                eligible_messages[-1],
                            )
        if should_notify:
            await self._dispatcher.notify(normalized)
        return should_notify

    async def _apply_llm_result_locked(
        self,
        chat_id: str,
        action: TriggerAction,
        *,
        decision: str,
        anchor_seq: int | None,
        observed_revision: int,
        observed_seq: int | None = None,
    ) -> tuple[TriggerAction | None, bool, str | None]:
        """在群锁内 fence 旁路结果；返回动作、是否需要 notify 和失败原因。"""
        normalized = str(chat_id)
        state = self._trigger_states.get(normalized)
        if state is None or not state.judgement_is_current(action.generation):
            self._schedule_clear_queued_reaction(normalized)
            return None, False, "stale_judgement"
        if not self._chat_access_allowed("group", normalized):
            state.invalidate_judgement()
            self._schedule_clear_queued_reaction(normalized)
            return None, False, "access_denied"
        status = await asyncio.to_thread(self._queue.status, normalized)
        block_reason = self._selector_block_reason(status)
        if block_reason is not None:
            if block_reason == "paused":
                state.pause()
            else:
                state.invalidate_judgement()
            self._schedule_clear_queued_reaction(normalized)
            return None, False, block_reason
        current_revision = int(status.get("revision", 0))
        result_action = state.on_llm_result(
            decision=decision,
            anchor_seq=anchor_seq,
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
                anchor_seq=result_action.anchor_seq,
                observed_seq=observed_seq,
            )
            self._schedule_clear_queued_reaction(normalized)
            return result_action, bool(request_id), None if request_id else "trigger_not_created"
        if (
            current_revision == observed_revision
            and observed_seq is not None
            and result_action.reason in {"llm_ignore", "llm_wait"}
        ):
            try:
                await asyncio.to_thread(
                    self._queue.mark_llm_judged,
                    normalized,
                    int(observed_seq),
                )
            except QueueError:
                return result_action, False, "queue_closed"
        if result_action.kind in {"schedule", "wait"}:
            await self._apply_trigger_action_locked(normalized, result_action)
            if result_action.reason == "queue_dirty":
                messages = await asyncio.to_thread(
                    self._queue.peek,
                    normalized,
                )
                latest = messages[-1] if messages else None
                if latest is None:
                    self._schedule_clear_queued_reaction(normalized)
                else:
                    self._schedule_queued_reaction(normalized, latest)
        elif state.engaged_until is not None:
            self._schedule_trigger_timer(normalized)
        if result_action.reason in {"llm_ignore", "invalid_result"}:
            self._schedule_clear_queued_reaction(normalized)
        return result_action, False, None

    async def _persist_llm_failure_locked(
        self,
        chat_id: str,
        action: TriggerAction,
        *,
        status: Mapping[str, Any],
        failure: str,
        observed_seq: int | None,
    ) -> None:
        """在群锁内持久化 selector 失败退避。"""
        if failure in {
            "stale_judgement",
            "access_denied",
            "cooldown",
            "paused",
            "leased",
            "uncertain",
            "failed",
            "hard_trigger_already_pending",
        }:
            return
        try:
            llm_state = await asyncio.to_thread(
                self._queue.llm_state,
                str(chat_id),
            )
            current_revision = int(status.get("revision", 0))
            newer_messages = current_revision > int(action.revision or 0)
            old_failure_count = int(
                llm_state.get("llm_failure_count", 0) or 0
            )
            next_attempt_at = time.time() + (
                0.0
                if newer_messages
                else min(60.0, 2.0 ** (old_failure_count + 1))
            )
            await asyncio.to_thread(
                self._queue.mark_llm_failure,
                str(chat_id),
                observed_seq=int(observed_seq or 0),
                error=failure,
                next_attempt_at=next_attempt_at,
            )
        except QueueError:
            logger.debug(
                "OneBot11 selector 失败时 queue 已关闭: %s",
                chat_id,
                exc_info=True,
            )

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
        observed_seq: int | None = None,
        candidate_message_key: str | None = None,
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
                if state is not None:
                    await self._persist_llm_failure_locked(
                        normalized,
                        action,
                        status=status,
                        failure=failure,
                        observed_seq=observed_seq,
                    )
        self._schedule_clear_queued_reaction(normalized)
        self._audit.record(
            "llm_trigger",
            {
                "chat_id": normalized,
                "candidate_type": action.candidate_type or "candidate",
                "candidate_message_key": candidate_message_key,
                "candidate_seq": observed_seq,
                "queue_revision": int(action.revision or 0),
                "pending": int(pending),
                "input_bytes": int(input_bytes),
                "decision": "ignore",
                "anchor_seq": None,
                "duration_ms": int(duration_ms),
                "failure": "stale_judgement" if stale else failure,
                "provider": self.trigger_config.llm_provider,
                "model": self.trigger_config.llm_model,
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
        anchor_seq: int | None = None
        failure = ""
        concurrency_waited = False
        concurrency_wait_ms = 0
        messages_count = 0
        input_bytes = 0
        observed_seq: int | None = None
        candidate_message_key: str | None = None
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
                    else:
                        last_trigger = status.get("last_trigger_at")
                        if (
                            self.trigger_config.cooldown_seconds > 0
                            and last_trigger is not None
                            and time.time() - float(last_trigger)
                            < self.trigger_config.cooldown_seconds
                        ):
                            failure = "cooldown"
                        else:
                            messages = await asyncio.to_thread(
                                self._queue.peek,
                                normalized,
                            )
                            messages_count = len(messages)
                            if not messages:
                                failure = "no_pending_messages"
                            else:
                                observed_seq = max(
                                    (
                                        int(message.seq)
                                        for message in messages
                                        if message.seq is not None
                                    ),
                                    default=None,
                                )
                                candidate_message_key = str(
                                    messages[-1].message_key
                                )
                                if not observed_revision:
                                    observed_revision = int(status.get("revision", 0))
                                # 按当前 engage 预算档选择输入预算和超时。
                                trigger_state = self._trigger_states.get(normalized)
                                tier = (
                                    trigger_state.config.tier_for(trigger_state.level)
                                    if trigger_state is not None
                                    else self.trigger_config.tier_for("normal")
                                )
                                prompt = build_llm_trigger_input(
                                    str(status.get("summary") or ""),
                                    messages,
                                    tier.input_bytes,
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
            trigger_state = self._trigger_states.get(normalized)
            timeout_seconds = (
                trigger_state.config.tier_for(trigger_state.level).timeout_seconds
                if trigger_state is not None
                else self.trigger_config.llm_timeout_seconds
            )
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
            decision_name, anchor_seq = parsed
            async with self._trigger_lock_for(normalized):
                result_action, notify, result_failure = await self._apply_llm_result_locked(
                    normalized,
                    action,
                    decision=decision_name,
                    anchor_seq=anchor_seq,
                    observed_revision=observed_revision,
                    observed_seq=observed_seq,
                )
                if result_failure:
                    failure = result_failure
            if not failure:
                self._audit.record(
                    "llm_trigger",
                    {
                        "chat_id": normalized,
                        "candidate_type": candidate_type,
                        "candidate_message_key": candidate_message_key,
                        "candidate_seq": observed_seq,
                        "queue_revision": observed_revision,
                        "pending": messages_count,
                        "input_bytes": input_bytes,
                        "decision": decision_name,
                        "anchor_seq": anchor_seq,
                        "duration_ms": int((time.monotonic() - started_at) * 1000),
                        "provider": self.trigger_config.llm_provider,
                        "model": self.trigger_config.llm_model,
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
                    observed_seq=observed_seq,
                    candidate_message_key=candidate_message_key,
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
                and role in ROLE_NAMES
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
                    allowed_tools=frozenset(
                        str(tool).strip()
                        for tool in tools
                        if str(tool).strip()
                        and str(tool).strip() not in FORBIDDEN_TOOL_NAMES
                    ),
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
        if self._closed or lease.lease_id in self._fenced_leases:
            raise PermissionError("OneBot11 adapter 或 queue lease 已 fencing")
        if not self._chat_access_allowed("group", lease.chat_id):
            raise PermissionError("当前群已不再满足 OneBot11 allowed_groups 策略")
        anchor_message = self._anchor_message(lease)
        if anchor_message is None:
            raise PermissionError("OneBot11 durable anchor 找不到对应的待处理消息")
        trigger = lease.trigger
        if trigger.authority_role not in ROLE_NAMES:
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
        if trigger.anchor_kind not in {"operator", "admin_flush"}:
            message_authority = self._authority_for_queued_message(anchor_message)
            trigger_tools = frozenset(
                tool
                for tool in trigger.authority_tools
                if tool not in FORBIDDEN_TOOL_NAMES
            )
            if (
                message_authority.role != trigger.authority_role
                or message_authority.allowed_tools != trigger_tools
                or message_authority.self_id != trigger.authority_self_id
            ):
                await asyncio.to_thread(
                    self._queue.mark_uncertain,
                    lease,
                    "OneBot11 anchor authority 与真实消息快照不一致",
                )
                raise PermissionError(
                    "OneBot11 durable anchor authority 与真实消息快照不一致"
                )
        if not await asyncio.to_thread(self._queue.mark_agent_started, lease):
            raise PermissionError("OneBot11 queue lease 已失效")
        self._delivery_summaries[lease.lease_id] = DeliverySummary()
        role = str(trigger.authority_role)
        caller = CallerContext(
            user_id=anchor_message.user_id,
            chat_type="group",
            chat_id=lease.chat_id,
            role=role,
            allowed_tools=frozenset(
                tool
                for tool in trigger.authority_tools
                if tool not in FORBIDDEN_TOOL_NAMES
            ),
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
                        if (
                            self._closed
                            or lease.lease_id in self._fenced_leases
                            or not self._chat_access_allowed("group", lease.chat_id)
                        ):
                            raise PermissionError(
                                "OneBot11 lease、adapter 或群访问策略在媒体处理前失效"
                            )
                        try:
                            lease_current = await asyncio.to_thread(
                                self._queue.is_lease_current,
                                lease,
                            )
                        except QueueError as exc:
                            raise PermissionError(
                                "OneBot11 QueueStore 在媒体处理前已关闭"
                            ) from exc
                        if not lease_current:
                            raise PermissionError(
                                "OneBot11 queue lease 在媒体处理前失效"
                            )
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
            onebot_context_token = _CURRENT_ONEBOT_CONTEXT.set(True)
            try:
                await super().handle_message(event)
                handed_off = True
                self._schedule_long_running_notice(event, lease.lease_id)
            finally:
                _CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
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

    def _schedule_queued_reaction(self, chat_id: str, message: QueueMessage) -> None:
        """异步给当前 selector 候选消息添加一次性 ⏳ 提示。"""
        normalized = str(chat_id)
        if not self._processing_reaction_enabled:
            self._schedule_clear_queued_reaction(normalized)
            return
        message_id = str(message.message_id or "").strip()
        if (
            not self._chat_access_allowed("group", normalized)
            or not is_numeric_message_id(message_id)
        ):
            self._schedule_clear_queued_reaction(normalized)
            return
        entry = (str(message.message_key or ""), message_id)
        if self._queued_reaction_attempted.get(normalized) == entry:
            # 同一候选不允许重复调用 OneBot reaction API；上次失败也保持
            # 该标记，直到候选消息变化或管理员 clear/pause 显式重置。
            return
        self._queued_reaction_attempted[normalized] = entry
        previous = self._queued_reaction_tasks.get(normalized)
        current = asyncio.current_task()
        if previous is not None and not previous.done() and previous is not current:
            previous.cancel()
        try:
            task = asyncio.create_task(
                self._set_queued_reaction(
                    normalized,
                    str(message.message_key),
                    message_id,
                )
            )
        except RuntimeError:
            logger.debug("OneBot11 无法创建 queued reaction task", exc_info=True)
            return
        self._queued_reaction_tasks[normalized] = task

    def _schedule_clear_queued_reaction(
        self,
        chat_id: str,
        *,
        reset_attempted: bool = False,
    ) -> None:
        """异步清理一个群当前内存登记的 ⏳ reaction。"""
        normalized = str(chat_id)
        if reset_attempted:
            self._queued_reaction_attempted.pop(normalized, None)
        current = asyncio.current_task()
        previous = self._queued_reaction_tasks.get(normalized)
        if previous is not None and not previous.done() and previous is not current:
            previous.cancel()
        try:
            task = asyncio.create_task(self._clear_queued_reaction(normalized))
        except RuntimeError:
            self._queued_reaction_message_ids.pop(normalized, None)
            logger.debug("OneBot11 无法创建 queued reaction cleanup task", exc_info=True)
            return
        self._queued_reaction_tasks[normalized] = task

    async def _set_queued_reaction(
        self,
        chat_id: str,
        message_key: str,
        message_id: str,
    ) -> None:
        """添加 ⏳，并在同群候选替换时先清理旧 reaction。"""
        normalized = str(chat_id)
        entry = (str(message_key), str(message_id))
        try:
            previous = self._queued_reaction_message_ids.get(normalized)
            if previous is not None and previous != entry:
                await self._unset_queued_reaction_entry(normalized, previous)
            if (
                self._closed
                or not self._chat_access_allowed("group", normalized)
                or self._queued_reaction_tasks.get(normalized) is not asyncio.current_task()
            ):
                return
            self._queued_reaction_message_ids[normalized] = entry
            async with self._outbound_gate:
                if (
                    self._closed
                    or self._queued_reaction_message_ids.get(normalized) != entry
                    or not self._chat_access_allowed("group", normalized)
                ):
                    return
                await self._api.set_message_emoji_like(
                    message_id,
                    _QUEUED_REACTION_EMOJI_ID,
                    enabled=True,
                )
        except OneBotApiError as exc:
            logger.warning(
                "OneBot11 queued reaction 添加失败: chat=%s message=%s status=%s",
                normalized,
                message_id,
                exc.status,
            )
            if not exc.unknown_outcome and self._queued_reaction_message_ids.get(normalized) == entry:
                self._queued_reaction_message_ids.pop(normalized, None)
        except (OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 queued reaction 添加失败: chat=%s message=%s error=%s",
                normalized,
                message_id,
                exc,
            )
            if self._queued_reaction_message_ids.get(normalized) == entry:
                self._queued_reaction_message_ids.pop(normalized, None)
        finally:
            if self._queued_reaction_tasks.get(normalized) is asyncio.current_task():
                self._queued_reaction_tasks.pop(normalized, None)

    async def _unset_queued_reaction_entry(
        self,
        chat_id: str,
        entry: tuple[str, str],
        *,
        allow_shutdown: bool = False,
    ) -> None:
        """尽力移除一个已登记的 ⏳，不改变队列和 Agent 状态。"""
        normalized = str(chat_id)
        _message_key, message_id = entry
        if not is_numeric_message_id(message_id):
            if self._queued_reaction_message_ids.get(normalized) == entry:
                self._queued_reaction_message_ids.pop(normalized, None)
            return
        if (
            (self._closed and not allow_shutdown)
            or not self._chat_access_allowed("group", normalized)
        ):
            if self._queued_reaction_message_ids.get(normalized) == entry:
                self._queued_reaction_message_ids.pop(normalized, None)
            return
        try:
            async with self._outbound_gate:
                if self._closed and not allow_shutdown:
                    return
                await self._api.set_message_emoji_like(
                    message_id,
                    _QUEUED_REACTION_EMOJI_ID,
                    enabled=False,
                )
        except (OneBotApiError, OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 queued reaction 移除失败: chat=%s message=%s error=%s",
                normalized,
                message_id,
                exc,
            )
            # 移除失败（例如 LLBot 瞬时错误）会留下远端 👀；短延迟后
            # 重试一次，仍失败则记录并放弃，不阻塞队列或 Agent 状态。
            if not (self._closed and not allow_shutdown):
                await asyncio.sleep(2.0)
                try:
                    async with self._outbound_gate:
                        if self._closed and not allow_shutdown:
                            return
                        await self._api.set_message_emoji_like(
                            message_id,
                            _QUEUED_REACTION_EMOJI_ID,
                            enabled=False,
                        )
                except (OneBotApiError, OSError, ValueError) as retry_exc:
                    logger.warning(
                        "OneBot11 queued reaction 重试移除仍失败: chat=%s message=%s error=%s",
                        normalized,
                        message_id,
                        retry_exc,
                    )
        finally:
            if self._queued_reaction_message_ids.get(normalized) == entry:
                self._queued_reaction_message_ids.pop(normalized, None)

    async def _clear_queued_reaction(
        self,
        chat_id: str,
        *,
        allow_shutdown: bool = False,
    ) -> None:
        """取消当前群的 queued reaction task 并尽力移除 ⏳。"""
        normalized = str(chat_id)
        current = asyncio.current_task()
        task = self._queued_reaction_tasks.get(normalized)
        if task is not None and task is not current and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        entry = self._queued_reaction_message_ids.get(normalized)
        if entry is not None:
            await self._unset_queued_reaction_entry(
                normalized,
                entry,
                allow_shutdown=allow_shutdown,
            )
        if self._queued_reaction_tasks.get(normalized) is current:
            self._queued_reaction_tasks.pop(normalized, None)

    async def _clear_all_queued_reactions(self, *, allow_shutdown: bool = False) -> None:
        """断开或 reset 时清理所有内存登记的 ⏳ reaction。"""
        current = asyncio.current_task()
        tasks = [
            task
            for task in self._queued_reaction_tasks.values()
            if task is not current and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for chat_id in list(self._queued_reaction_message_ids):
            await self._clear_queued_reaction(
                chat_id,
                allow_shutdown=allow_shutdown,
            )

    async def _set_processing_reaction(self, lease: QueueLease, *, enabled: bool) -> str | None:
        """持久化并按需添加处理指示器；绝不自动重放历史 set 请求。"""
        if not enabled or not self._processing_reaction_enabled:
            return None
        if not self._chat_access_allowed("group", lease.chat_id):
            return None
        message_id = self._reaction_message_id(lease)
        if message_id is None:
            logger.debug("OneBot11 reaction 跳过无真实 message_id 的触发消息: %s", lease.lease_id)
            return None
        try:
            lease_current = await asyncio.to_thread(
                self._queue.is_lease_current,
                lease,
            )
        except QueueError:
            logger.info("OneBot11 reaction 跳过已关闭 QueueStore: %s", lease.lease_id)
            return None
        if not lease_current:
            logger.info("OneBot11 reaction 跳过已失效 lease: %s", lease.lease_id)
            return None
        try:
            existing = await asyncio.to_thread(
                self._queue.reaction_for_target,
                lease.chat_id,
                message_id,
            )
            await asyncio.to_thread(
                self._queue.record_reaction,
                lease.lease_id,
                lease.chat_id,
                message_id,
            )
        except (QueueError, OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 reaction 落盘失败，跳过远端 set: lease=%s error=%s",
                lease.lease_id,
                exc,
            )
            return None
        self._processing_reaction_message_ids[lease.lease_id] = message_id
        if existing is not None:
            # 旧 pending/maybe_set 都只能在收尾时 unset，不能再次 set。
            return message_id
        if self._closed or lease.lease_id in self._fenced_leases:
            return None
        try:
            async with self._outbound_gate:
                if (
                    self._closed
                    or lease.lease_id in self._fenced_leases
                    or not await asyncio.to_thread(
                        self._queue.is_lease_current,
                        lease,
                    )
                    or not self._chat_access_allowed("group", lease.chat_id)
                ):
                    return None
                await self._api.set_message_emoji_like(
                    message_id,
                    self._processing_reaction_emoji_id,
                    enabled=True,
                )
            try:
                await asyncio.to_thread(
                    self._queue.mark_reaction_set,
                    lease.lease_id,
                )
            except QueueError:
                # 远端已明确成功但本地状态仍为 pending；恢复路径只执行 unset。
                logger.warning(
                    "OneBot11 reaction 成功状态落盘失败: %s",
                    lease.lease_id,
                    exc_info=True,
                )
            return message_id
        except OneBotApiError as exc:
            logger.warning(
                "OneBot11 reaction %s 失败: lease=%s message=%s status=%s",
                "添加",
                lease.lease_id,
                message_id,
                exc.status,
            )
            if exc.unknown_outcome:
                try:
                    await asyncio.to_thread(
                        self._queue.mark_reaction_set,
                        lease.lease_id,
                    )
                except QueueError:
                    logger.warning(
                        "OneBot11 reaction unknown 状态落盘失败: %s",
                        lease.lease_id,
                        exc_info=True,
                    )
                return message_id
            try:
                await asyncio.to_thread(
                    self._queue.delete_reaction,
                    lease.lease_id,
                )
            except QueueError:
                logger.warning(
                    "OneBot11 reaction 明确失败后的记录删除失败: %s",
                    lease.lease_id,
                    exc_info=True,
                )
            self._processing_reaction_message_ids.pop(lease.lease_id, None)
            return None
        except (OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 reaction %s 失败: lease=%s message=%s error=%s",
                "添加",
                lease.lease_id,
                message_id,
                exc,
            )
            # 非标准客户端异常也按结果未知处理，保留记录只等待 unset。
            try:
                await asyncio.to_thread(
                    self._queue.mark_reaction_set,
                    lease.lease_id,
                )
            except QueueError:
                logger.warning(
                    "OneBot11 reaction 异常状态落盘失败: %s",
                    lease.lease_id,
                    exc_info=True,
                )
            return message_id
    async def _clear_processing_reaction(
        self,
        lease_id: str,
        *,
        allow_shutdown: bool = False,
        allow_recovery: bool = False,
    ) -> None:
        """尽力 unset reaction；恢复路径只允许清理，不允许重新添加。"""
        normalized_lease_id = str(lease_id)
        record: ReactionRecord | None = None
        if not self._queue.closed:
            try:
                record = await asyncio.to_thread(
                    self._queue.reaction_for_lease,
                    normalized_lease_id,
                )
            except QueueError:
                record = None
        if record is None:
            # 没有持久记录就无法证明 reaction 的群目标；内存中的 message_id
            # 不能单独授权一个 unset 请求，避免 stale cleanup 向未知目标出站。
            self._processing_reaction_message_ids.pop(normalized_lease_id, None)
            return
        message_id = record.message_id
        if (
            self._closed
            and not allow_shutdown
        ) or (
            normalized_lease_id in self._fenced_leases
            and not (allow_shutdown or allow_recovery)
        ):
            # 旧 turn 在 fencing 后不能再访问 OneBot；持久记录交给恢复路径。
            return
        record_chat_id = record.chat_id if record is not None else ""
        if not self._chat_access_allowed("group", record_chat_id):
            if record is not None and not self._queue.closed:
                try:
                    await asyncio.to_thread(
                        self._queue.delete_reaction,
                        normalized_lease_id,
                    )
                except QueueError:
                    logger.warning(
                        "OneBot11 白名单收紧后删除 reaction 记录失败: %s",
                        normalized_lease_id,
                        exc_info=True,
                    )
            self._processing_reaction_message_ids.pop(normalized_lease_id, None)
            return
        if record is not None:
            if record.attempts >= 3:
                return
            if (
                record.next_attempt_at is not None
                and record.next_attempt_at > time.time()
            ):
                return
        if not is_numeric_message_id(message_id):
            if record is not None and not self._queue.closed:
                try:
                    await asyncio.to_thread(
                        self._queue.delete_reaction,
                        normalized_lease_id,
                    )
                except QueueError:
                    logger.warning(
                        "OneBot11 无效 reaction message_id 记录删除失败: %s",
                        normalized_lease_id,
                        exc_info=True,
                    )
            self._processing_reaction_message_ids.pop(normalized_lease_id, None)
            return
        try:
            async with self._outbound_gate:
                if self._closed and not allow_shutdown:
                    return
                await self._api.set_message_emoji_like(
                    message_id,
                    self._processing_reaction_emoji_id,
                    enabled=False,
                )
        except OneBotApiError as exc:
            logger.warning(
                "OneBot11 reaction 移除失败: lease=%s message=%s status=%s",
                normalized_lease_id,
                message_id,
                exc.status,
            )
            if record is not None and not self._queue.closed:
                try:
                    await asyncio.to_thread(
                        self._queue.mark_reaction_cleanup_failure,
                        normalized_lease_id,
                        str(exc),
                    )
                except QueueError:
                    logger.warning(
                        "OneBot11 reaction 清理失败状态落盘失败: %s",
                        normalized_lease_id,
                        exc_info=True,
                    )
        except (OSError, ValueError) as exc:
            logger.warning(
                "OneBot11 reaction 移除失败: lease=%s message=%s error=%s",
                normalized_lease_id,
                message_id,
                exc,
            )
            if record is not None and not self._queue.closed:
                try:
                    await asyncio.to_thread(
                        self._queue.mark_reaction_cleanup_failure,
                        normalized_lease_id,
                        str(exc),
                    )
                except QueueError:
                    logger.warning(
                        "OneBot11 reaction 清理异常状态落盘失败: %s",
                        normalized_lease_id,
                        exc_info=True,
                    )
        else:
            if record is not None and not self._queue.closed:
                try:
                    deleted = await asyncio.to_thread(
                        self._queue.delete_reaction,
                        normalized_lease_id,
                    )
                except QueueError:
                    deleted = False
                    logger.warning(
                        "OneBot11 reaction 已 unset 但本地记录删除失败: %s",
                        normalized_lease_id,
                        exc_info=True,
                    )
                    try:
                        await asyncio.to_thread(
                            self._queue.mark_reaction_cleanup_failure,
                            normalized_lease_id,
                            "远端 unset 成功但本地记录删除失败",
                        )
                    except QueueError:
                        logger.warning(
                            "OneBot11 reaction 删除失败退避状态落盘失败: %s",
                            normalized_lease_id,
                            exc_info=True,
                        )
                if deleted:
                    self._processing_reaction_message_ids.pop(
                        normalized_lease_id,
                        None,
                    )
            else:
                self._processing_reaction_message_ids.pop(
                    normalized_lease_id,
                    None,
                )

    async def _clear_all_processing_reactions(self, *, allow_shutdown: bool = False) -> None:
        """清理当前内存和持久化中的有限 reaction 记录。"""
        lease_ids = set(self._processing_reaction_message_ids)
        if not self._queue.closed:
            try:
                records = await asyncio.to_thread(
                    self._queue.pending_reaction_cleanups,
                    None,
                    limit=32,
                    include_not_due=True,
                    include_exhausted=True,
                )
            except QueueError:
                records = ()
            lease_ids.update(record.lease_id for record in records)
        for reaction_lease_id in lease_ids:
            await self._clear_processing_reaction(
                reaction_lease_id,
                allow_shutdown=allow_shutdown,
            )

    async def _recover_processing_reactions_once(
        self,
        *,
        allow_shutdown: bool = False,
    ) -> None:
        """后台按有限批次回收遗留 reaction，只执行 unset。"""
        if self._queue.closed:
            return
        try:
            due_records = await asyncio.to_thread(
                self._queue.pending_reaction_cleanups,
                None,
                limit=32,
            )
            all_records = await asyncio.to_thread(
                self._queue.pending_reaction_cleanups,
                None,
                limit=32,
                include_not_due=True,
                include_exhausted=True,
            )
        except QueueError:
            return
        records = {record.lease_id: record for record in (*due_records, *all_records)}
        for record in records.values():
            if not self._chat_access_allowed("group", record.chat_id):
                await self._clear_processing_reaction(
                    record.lease_id,
                    allow_shutdown=allow_shutdown,
                    allow_recovery=True,
                )
                continue
            if not allow_shutdown:
                active = self._dispatcher.active_by_lease(record.lease_id)
                if active is not None and not active.lease_lost:
                    continue
                try:
                    status = await asyncio.to_thread(
                        self._queue.status_for_lease,
                        record.lease_id,
                    )
                except QueueError:
                    continue
                if (
                    status.get("state") == "leased"
                    and float(status.get("lease_until") or 0) > time.time()
                ):
                    continue
            await self._clear_processing_reaction(
                record.lease_id,
                allow_shutdown=allow_shutdown,
                allow_recovery=True,
            )

    def _start_reaction_recovery(self) -> None:
        """启动 reaction 轻量恢复轮询，不阻塞 adapter connect。"""
        if self._closed:
            return
        task = self._reaction_recovery_task
        if task is not None and not task.done():
            return
        try:
            self._reaction_recovery_task = asyncio.create_task(
                self._reaction_recovery_loop()
            )
        except RuntimeError:
            self._reaction_recovery_task = None
            logger.debug("OneBot11 无法创建 reaction 恢复 task", exc_info=True)

    async def _reaction_recovery_loop(self) -> None:
        """周期处理少量 reaction 清理任务，避免恢复阶段阻塞启动。"""
        try:
            while not self._closed:
                await self._recover_processing_reactions_once()
                await asyncio.sleep(
                    max(0.5, float(self._runtime_config.queue_recovery_poll_seconds))
                )
        except asyncio.CancelledError:
            return
        except Exception:
            logger.warning("OneBot11 reaction 恢复轮询失败", exc_info=True)
        finally:
            if self._reaction_recovery_task is asyncio.current_task():
                self._reaction_recovery_task = None

    async def _cancel_reaction_recovery(self) -> None:
        """停止 reaction 恢复 task，确保 QueueStore 关闭前不再新读写。"""
        task = self._reaction_recovery_task
        self._reaction_recovery_task = None
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

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
        await self._cancel_long_running_notice(lease.lease_id)
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
        summary = self._delivery_summaries.get(lease_id)
        # 只有每个 delivery unit 都拿到明确成功结果，才允许删除队列消息。
        # Hermes SUCCESS 只代表 turn 结束，不代表 OneBot 已收到回复。
        if outcome == ProcessingOutcome.SUCCESS and started and not unknown:
            if summary is not None:
                if summary.all_successful:
                    return True, False, False, None
                if (
                    summary.attempted == 0
                    and lease_id in self._outbound_successful
                ):
                    # 兼容旧宿主/测试路径只写入成功集合的调用方；
                    # 新 managed turn 的真实出站会始终填充 summary。
                    return True, False, False, None
                return False, True, False, "OneBot11 出站存在部分成功、失败或 fencing"
            if lease_id in self._outbound_successful:
                return True, False, False, None
            return False, True, False, "Hermes turn 成功但没有完整的 OneBot 出站成功记录"
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
        runtime_fenced = fenced_completion or self._closed
        active_turn = self._dispatcher.active_by_lease(lease_id)
        if active_turn is not None and active_turn.lease_lost:
            runtime_fenced = True
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
        try:
            lease_revision = int(metadata.get("onebot11_lease_revision") or 0)
        except (TypeError, ValueError):
            lease_revision = 0
            runtime_fenced = True
        try:
            if not runtime_fenced:
                ack, unknown, known_failure, reason = self._queue_completion_decision(
                    lease_id,
                    outcome,
                )
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
            else:
                reason = "adapter、lease 或 turn epoch 已 fencing，跳过 queue completion"
        except BaseException as exc:
            completion_error = exc
            logger.warning("OneBot11 queue turn 收口失败，等待 lease 恢复: %s", lease_id, exc_info=True)
        finally:
            # reaction 是 best-effort，不能阻断 binding、媒体和内存状态释放。
            if runtime_fenced:
                # 失效 turn 只清理内存；SQLite reaction 记录交给启动恢复，
                # 不能让迟到的 Hermes task 触碰新 runtime 的持久状态。
                self._processing_reaction_message_ids.pop(lease_id, None)
            else:
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
                        summary = self._delivery_summaries.get(lease_id)
                        last_reply_text = (
                            str(summary.last_text or "")
                            if summary is not None
                            else ""
                        )
                        anchor_user_id = str(
                            metadata.get("onebot11_anchor_user_id") or ""
                        )
                        state.on_turn_complete(
                            success=successful_turn,
                            now=time.monotonic(),
                            preserve_pending=preserve_pending,
                            has_hard_trigger=has_hard_trigger,
                            bot_asked=self._reply_asks_user(last_reply_text),
                            anchor_user_id=anchor_user_id,
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
                                    last_trigger_at=status.get("last_trigger_at"),
                                    user_id=str(latest.user_id or ""),
                                    reply_to_bot=self._message_replies_to_bot(
                                        chat_id,
                                        str(
                                            latest.metadata.get(
                                                "onebot11_reply_to"
                                            )
                                            or ""
                                        ),
                                    ),
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
            try:
                await self._cancel_long_running_notice(lease_id)
            except Exception:
                logger.debug("OneBot11 长时间提示收尾失败: %s", lease_id, exc_info=True)
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
            self._delivery_summaries.pop(lease_id, None)
            self._lease_session_keys.pop(lease_id, None)
            self._clear_media_scope(metadata, lease_id=lease_id)
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
        source = getattr(event, "source", None)
        onebot_context_token = _CURRENT_ONEBOT_CONTEXT.set(
            bool(metadata.get("onebot11_managed_context"))
            or _platform_value(getattr(source, "platform", None)) == _PLATFORM_NAME
        )
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
            _CURRENT_ONEBOT_CONTEXT.reset(onebot_context_token)
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
                self._clear_media_scope(metadata)
            if not managed_context:
                _CURRENT_BINDING.set(None)
                _CURRENT_CALLER.set(None)
            if not deferred:
                self._cleanup_media(
                    metadata.get("onebot11_media_paths")
                    or getattr(event, "media_urls", []),
                    media_dir=metadata.get("onebot11_media_dir"),
                )

    def format_message(self, content: str) -> str:
        """把 Hermes 回复转换为 OneBot 默认纯文本。"""
        if self.plain_text_enabled:
            formatted = format_onebot_text(str(content or ""))
        else:
            unwrapped, requested = unwrap_markdown_image_markers(str(content or ""))
            formatted = FormattedText(
                text=unwrapped.strip(),
                markdown_image_requested=requested,
            )
        if formatted.markdown_image_requested:
            _safe_audit(
                self,
                "markdown_image_requested_unavailable",
                {
                    "renderer": "unavailable",
                    "external_urls_fetched": False,
                },
            )
        return formatted.text

    def format_tool_event(
        self,
        event: Any,
        *,
        mode: str = "all",
        preview_max_len: int = 40,
    ) -> None:
        """OneBot 不发送 Hermes 工具进度，避免永久消息污染最终回复。"""
        del event, mode, preview_max_len
        return None

    def _delivery_summary_for(self, lease_id: str | None) -> DeliverySummary | None:
        """读取或创建 managed turn 的出站结算对象。"""
        normalized = str(lease_id or "").strip()
        if not normalized:
            return None
        return self._delivery_summaries.setdefault(normalized, DeliverySummary())

    def _message_replies_to_bot(self, chat_id: str, reply_to: str) -> bool:
        """判断回复目标是否是 bot 在该群最后发送的消息。"""
        normalized = str(chat_id or "").strip()
        raw_reply = str(reply_to or "").strip()
        if not normalized or not raw_reply:
            return False
        return raw_reply == str(self._last_bot_message_ids.get(normalized) or "")

    def _reply_asks_user(self, text: str) -> bool:
        """判断 bot 回复是否以问句或请求用户提供信息收尾。"""
        content = str(text or "").strip()
        if not content:
            return False
        if is_question(content):
            return True
        # 请求词只检查回复尾部，避免"这是日志"这类陈述误判成提问。
        tail = content[-80:].casefold()
        return any(
            str(word).casefold() in tail
            for word in self.trigger_config.bot_asked_words
        )

    async def _prepare_business_delivery(
        self,
        lease_id: str | None,
        target: ChatTarget,
        caller_user_id: str | None,
    ) -> bool:
        """在业务出站前执行 marker、二次 fencing 和当前目标授权检查。"""
        if self._closed:
            return False
        if not self._chat_access_allowed(
            target.chat_type,
            target.chat_id,
            caller_user_id,
        ):
            return False
        if not lease_id:
            return True
        if lease_id in self._fenced_leases:
            return False
        try:
            marked = await asyncio.to_thread(
                self._queue.mark_outbound_started,
                lease_id,
            )
        except QueueError:
            self._fenced_leases.add(lease_id)
            return False
        if not marked:
            self._fenced_leases.add(lease_id)
            return False
        self._outbound_started.add(lease_id)
        # marker 写入后必须再次检查；否则 shutdown/heartbeat 可能在
        # marker 与 HTTP 请求之间夺走 lease。
        if (
            self._closed
            or lease_id in self._fenced_leases
            or not self._lease_is_current(lease_id)
            or not self._chat_access_allowed(
                target.chat_type,
                target.chat_id,
                caller_user_id,
            )
        ):
            self._fenced_leases.add(lease_id)
            return False
        return True

    async def _send_control_plane(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: str | None,
        metadata: Mapping[str, Any],
    ) -> SendResult:
        """发送 Hermes 明确标记的控制面消息，不污染业务 lease 状态。"""
        binding = self._binding_for_outbound(chat_id, metadata)
        if self._closed:
            return SendResult(False, error="OneBot11 adapter is closed", error_kind="not_found")
        managed_context = bool(
            metadata.get("onebot11_managed_context")
            or metadata.get("onebot11_lease_id")
            or metadata.get("onebot11_binding_key")
        )
        if managed_context and binding is None:
            self._log_binding_diagnostic(
                metadata,
                reason="control_plane_binding_missing_or_invalid",
            )
            return SendResult(
                False,
                error="OneBot11 managed control-plane binding unavailable",
                error_kind="fenced",
            )
        if binding is not None and binding.lease_id and not self._lease_is_current(
            binding.lease_id
        ):
            return SendResult(False, error="OneBot11 lease 已失效，拒绝控制面出站", error_kind="fenced")
        target = self._resolve_target(str(chat_id), metadata, binding=binding)
        if target is None:
            return SendResult(False, error="OneBot11 target unknown or ambiguous", error_kind="unknown")
        caller_user_id = (
            binding.caller.user_id
            if binding is not None
            else target.chat_id
            if target.chat_type == "dm"
            else None
        )
        if not self._chat_access_allowed(target.chat_type, target.chat_id, caller_user_id):
            return SendResult(False, error="OneBot11 target 不再满足访问策略", error_kind="permission")
        if self._ws is None:
            return SendResult(False, error="Not connected", error_kind="not_found")
        control_scope = self._control_plane_scope_key(metadata)
        if (
            control_scope is not None
            and control_scope in self._control_plane_sent_scopes
        ):
            return SendResult(
                True,
                raw_response={
                    "control_plane": True,
                    "deduplicated": True,
                },
            )
        if control_scope is not None:
            # 控制面通知没有可靠编辑/撤回合同；第一次尝试后即不在同一
            # turn 内再次发送，避免 Hermes heartbeat 重复刷屏。
            self._control_plane_sent_scopes.add(control_scope)

        pieces = chunk_text(
            self.format_message(content),
            self.max_message_length_for_chat(target.chat_id),
        )
        sent: list[str] = []
        for piece in pieces:
            if self._closed:
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error="OneBot11 adapter is closed",
                    error_kind="fenced" if not sent else "unknown",
                    raw_response={"control_plane": True, "sent_chunks": len(sent)},
                )
            if binding is not None and binding.lease_id and not self._lease_is_current(
                binding.lease_id
            ):
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error="OneBot11 lease 已失效，拒绝控制面出站",
                    error_kind="fenced" if not sent else "unknown",
                    raw_response={"control_plane": True, "sent_chunks": len(sent)},
                )
            if not self._chat_access_allowed(
                target.chat_type,
                target.chat_id,
                binding.caller.user_id
                if binding is not None
                else target.chat_id
                if target.chat_type == "dm"
                else None,
            ):
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error="OneBot11 target 不再满足访问策略",
                    error_kind="permission" if not sent else "unknown",
                    raw_response={"control_plane": True, "sent_chunks": len(sent)},
                )
            try:
                async with self._outbound_gate:
                    if self._closed:
                        return SendResult(
                            False,
                            message_id=sent[-1] if sent else None,
                            error="OneBot11 adapter is closed",
                            error_kind="fenced" if not sent else "unknown",
                            raw_response={
                                "control_plane": True,
                                "sent_chunks": len(sent),
                            },
                        )
                    if binding is not None and binding.lease_id and not self._lease_is_current(
                        binding.lease_id
                    ):
                        return SendResult(
                            False,
                            message_id=sent[-1] if sent else None,
                            error="OneBot11 lease 已失效，拒绝控制面出站",
                            error_kind="fenced" if not sent else "unknown",
                            raw_response={
                                "control_plane": True,
                                "sent_chunks": len(sent),
                            },
                        )
                    if not self._chat_access_allowed(
                        target.chat_type,
                        target.chat_id,
                        binding.caller.user_id
                        if binding is not None
                        else target.chat_id
                        if target.chat_type == "dm"
                        else None,
                    ):
                        return SendResult(
                            False,
                            message_id=sent[-1] if sent else None,
                            error="OneBot11 target 不再满足访问策略",
                            error_kind="permission" if not sent else "unknown",
                            raw_response={
                                "control_plane": True,
                                "sent_chunks": len(sent),
                            },
                        )
                    message_id = await self._api.send_message(
                        target.chat_id,
                        piece,
                        chat_type=target.chat_type,
                        reply_to=reply_to,
                    )
            except OneBotApiError as exc:
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error=str(exc),
                    error_kind="unknown" if exc.unknown_outcome else exc.error_kind,
                    raw_response={"control_plane": True, "sent_chunks": len(sent)},
                )
            except (OSError, ValueError) as exc:
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error=str(exc),
                    error_kind="unknown" if isinstance(exc, OSError) or sent else "failed",
                    raw_response={"control_plane": True, "sent_chunks": len(sent)},
                )
            if not message_id:
                return SendResult(
                    False,
                    message_id=sent[-1] if sent else None,
                    error="控制面消息响应缺少 message_id",
                    error_kind="unknown",
                    raw_response={"control_plane": True, "sent_chunks": len(sent)},
                )
            sent.append(message_id)
        return SendResult(
            True,
            message_id=sent[-1] if sent else None,
            raw_response={"control_plane": True, "sent_chunks": len(sent)},
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        """显式解析 ChatTarget 后发送，并记录部分/未知出站结果。"""
        if _is_control_plane_metadata(metadata):
            return await self._send_control_plane(
                chat_id,
                content,
                reply_to=reply_to,
                metadata=metadata,
            )
        if not _FINAL_DELIVERY.get():
            # Hermes 的中间正文（commentary/工具进度/状态提示）直调
            # adapter.send()，不走 _send_with_retry。按目标类型决定是否
            # 展示：群聊默认隐藏（避免刷屏），私聊默认展示。
            interim_target = self._resolve_target(
                str(chat_id),
                metadata if isinstance(metadata, Mapping) else None,
            )
            if interim_target is not None and not self._interim_allowed(interim_target):
                self._audit.record(
                    "interim_hidden",
                    {
                        "chat_type": interim_target.chat_type,
                        "chat_id": interim_target.chat_id,
                        "reason": "中间正文按配置隐藏",
                    },
                )
                return SendResult(
                    True,
                    message_id=str(uuid.uuid4()),
                    error="OneBot11 中间正文按配置隐藏",
                    error_kind="interim_hidden",
                )
        binding = self._binding_for_outbound(
            chat_id,
            metadata if isinstance(metadata, Mapping) else None,
        )
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
            self._log_binding_diagnostic(
                metadata if isinstance(metadata, Mapping) else None,
                reason="binding_missing_or_invalid",
            )
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
        target = self._resolve_target(str(chat_id), metadata, binding=binding)
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
        formatted_content = self.format_message(content)
        pieces = chunk_text(
            formatted_content,
            self.max_message_length_for_chat(target.chat_id),
        )
        if not pieces and formatted_content:
            pieces = [formatted_content]
        sent: list[str] = []
        for piece in pieces:
            summary = self._delivery_summary_for(lease_id)
            async with self._outbound_gate:
                if self._closed:
                    if summary is not None:
                        summary.fenced += 1
                    return SendResult(
                        False,
                        message_id=sent[-1] if sent else None,
                        error="OneBot11 adapter is closed",
                        raw_response={
                            "sent_chunks": len(sent),
                            "total_chunks": len(pieces),
                        },
                        error_kind="fenced" if not sent else "unknown",
                    )
                prepared = await self._prepare_business_delivery(
                    lease_id,
                    target,
                    caller_user_id,
                )
                if not prepared:
                    if lease_id:
                        self._fenced_leases.add(lease_id)
                        self._outbound_known_failure.add(lease_id)
                        if summary is not None:
                            summary.fenced += 1
                    return SendResult(
                        False,
                        message_id=sent[-1] if sent else None,
                        error="OneBot11 lease、adapter 或目标在出站前失效",
                        raw_response={
                            "sent_chunks": len(sent),
                            "total_chunks": len(pieces),
                        },
                        error_kind="fenced" if lease_id else "permission",
                    )
                if summary is not None:
                    summary.attempted += 1
                try:
                    sent_id = await self._api.send_message(
                        target.chat_id,
                        piece,
                        chat_type=target.chat_type,
                        reply_to=reply_to,
                    )
                    if not sent_id:
                        if lease_id:
                            self._unknown_leases.add(lease_id)
                        if summary is not None:
                            summary.unknown += 1
                        return SendResult(
                            False,
                            message_id=sent[-1] if sent else None,
                            error="OneBot 成功响应缺少 message_id，出站结果未知",
                            raw_response={
                                "sent_chunks": len(sent),
                                "total_chunks": len(pieces),
                            },
                            error_kind="unknown",
                        )
                    if lease_id:
                        self._outbound_successful.add(lease_id)
                    if summary is not None:
                        summary.successful += 1
                        summary.last_text = piece
                    if sent_id:
                        self._last_bot_message_ids[target.chat_id] = str(sent_id)
                    sent.append(sent_id)
                    if (
                        not _FINAL_DELIVERY.get()
                        and target.chat_type == "group"
                    ):
                        # 中间正文是"bot 仍在活动"的最直接证据：成功发出一条
                        # interim 后重置长时间处理计时器，避免 bot 一直有输出
                        # 却仍触发"仍在处理中"的冗余提示。
                        self._reset_long_running_notice(target.chat_id)
                except OneBotApiError as exc:
                    if lease_id:
                        if exc.unknown_outcome or sent:
                            self._unknown_leases.add(lease_id)
                        else:
                            self._outbound_known_failure.add(lease_id)
                    if summary is not None:
                        if exc.unknown_outcome or sent:
                            summary.unknown += 1
                        else:
                            summary.known_failed += 1
                    return SendResult(
                        False,
                        message_id=sent[-1] if sent else None,
                        error=str(exc),
                        raw_response={
                            "sent_chunks": len(sent),
                            "total_chunks": len(pieces),
                        },
                        error_kind="unknown" if exc.unknown_outcome else exc.error_kind,
                    )
                except ValueError as exc:
                    if lease_id:
                        if sent:
                            self._unknown_leases.add(lease_id)
                        else:
                            self._outbound_known_failure.add(lease_id)
                    if summary is not None:
                        if sent:
                            summary.unknown += 1
                        else:
                            summary.known_failed += 1
                    return SendResult(
                        False,
                        message_id=sent[-1] if sent else None,
                        error=str(exc),
                        raw_response={
                            "sent_chunks": len(sent),
                            "total_chunks": len(pieces),
                        },
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
        media_scope = kwargs.pop("_onebot11_media_scope", None)
        media_source = kwargs.pop("_onebot11_media_source", None) or image_path
        preflight = self._preflight_image_delivery(chat_id, metadata)
        if preflight is not None:
            return preflight
        binding = self._binding_for_outbound(
            chat_id,
            metadata if isinstance(metadata, Mapping) else None,
        )
        current_event = _CURRENT_EVENT.get()
        lease_id = binding.lease_id if binding else None
        target = self._resolve_target(str(chat_id), metadata, binding=binding)
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

        if media_scope is None:
            media_scope = self._media_scope_for(metadata)
        if media_scope is not None:
            is_new, fingerprint = media_scope.claim(str(media_source), data)
            if not is_new:
                return self._deduplicated_media_result(fingerprint)

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
            segments.append(
                {"type": "text", "data": {"text": self.format_message(str(caption))}}
            )

        summary = self._delivery_summary_for(lease_id)
        async with self._outbound_gate:
            if not await self._prepare_business_delivery(
                lease_id,
                target,
                binding.caller.user_id if binding is not None else None,
            ):
                if lease_id:
                    self._fenced_leases.add(lease_id)
                    self._outbound_known_failure.add(lease_id)
                    if summary is not None:
                        summary.fenced += 1
                return SendResult(
                    False,
                    error="OneBot11 lease、adapter 或目标在出站前失效",
                    error_kind="fenced" if lease_id else "permission",
                )
            if summary is not None:
                summary.attempted += 1
            try:
                sent_id = await self._api.send_message_segments(
                    target.chat_id,
                    segments,
                    chat_type=target.chat_type,
                )
                if not sent_id:
                    if lease_id:
                        self._unknown_leases.add(lease_id)
                    if summary is not None:
                        summary.unknown += 1
                    return SendResult(
                        False,
                        error="OneBot 成功响应缺少 message_id，图片出站结果未知",
                        error_kind="unknown",
                    )
                if lease_id:
                    self._outbound_successful.add(lease_id)
                if summary is not None:
                    summary.successful += 1
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
                if summary is not None:
                    if exc.unknown_outcome:
                        summary.unknown += 1
                    else:
                        summary.known_failed += 1
                return SendResult(
                    False,
                    error=str(exc),
                    error_kind="unknown" if exc.unknown_outcome else exc.error_kind,
                )
            except OSError as exc:
                if lease_id:
                    self._unknown_leases.add(lease_id)
                if summary is not None:
                    summary.unknown += 1
                return SendResult(False, error=str(exc), error_kind="unknown")
            except ValueError as exc:
                if lease_id:
                    self._outbound_known_failure.add(lease_id)
                if summary is not None:
                    summary.known_failed += 1
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
        media_scope = self._media_scope_for(metadata)
        if media_scope is not None and media_scope.would_duplicate(str(image_url)):
            return self._deduplicated_media_result(
                media_scope.source_fingerprint(str(image_url))
            )
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
                _onebot11_media_scope=media_scope,
                _onebot11_media_source=str(image_url),
            )
        finally:
            self._cleanup_media([path], media_dir=media_dir)

    def _preflight_image_delivery(
        self,
        chat_id: str,
        metadata: Mapping[str, Any] | None,
    ) -> SendResult | None:
        """在图片下载或 OneBot 请求前校验身份、目标、权限和连接。"""
        binding = self._binding_for_outbound(chat_id, metadata)
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
            self._log_binding_diagnostic(
                metadata,
                reason="binding_missing_or_invalid_before_media",
            )
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
        target = self._resolve_target(str(chat_id), metadata, binding=binding)
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

        binding = self._binding_for_outbound(
            chat_id,
            metadata if isinstance(metadata, Mapping) else None,
        )
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
            self._log_binding_diagnostic(
                metadata if isinstance(metadata, Mapping) else None,
                reason="binding_missing_or_invalid",
            )
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
        prepared: list[tuple[int, str, str, str]] = []
        results_by_index: list[SendResult | None] = [None] * len(images)
        persistent_scope = self._media_scope_for(
            metadata if isinstance(metadata, Mapping) else None
        )
        batch_scope = MediaDeliveryScope(f"batch:{id(images)}")
        delivery_scope = persistent_scope or MediaDeliveryScope(f"delivery:{id(images)}")
        total_bytes = 0

        def preflight_failure(error: str, error_kind: str) -> list[SendResult]:
            """为未发出 OneBot 请求的整批图片返回一致的预检失败。"""
            return [
                SendResult(False, error=error, error_kind=error_kind)
                for _image_url, _caption in images
            ]

        try:
            for index, (image_url, caption) in enumerate(images):
                raw_source = str(image_url)
                if persistent_scope and persistent_scope.would_duplicate(raw_source):
                    results_by_index[index] = self._deduplicated_media_result(
                        persistent_scope.source_fingerprint(raw_source)
                    )
                    continue
                if batch_scope.would_duplicate(raw_source):
                    results_by_index[index] = self._deduplicated_media_result(
                        batch_scope.source_fingerprint(raw_source)
                    )
                    continue
                if raw_source.casefold().startswith("file://"):
                    path = unquote(raw_source[7:])
                    if (
                        os.name == "nt"
                        and len(path) >= 3
                        and path[0] == "/"
                        and path[2] == ":"
                    ):
                        path = path[1:]
                elif raw_source.casefold().startswith(("http://", "https://")):
                    path = await self._api.download_to_temp(raw_source, media_dir)
                    if not path:
                        return preflight_failure("图片下载失败或未通过安全校验", "failed")
                    downloaded_paths.append(path)
                else:
                    path = raw_source
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
                if batch_scope.would_duplicate(raw_source, data) or (
                    persistent_scope is not None
                    and persistent_scope.would_duplicate(raw_source, data)
                ):
                    if path in downloaded_paths:
                        Path(path).unlink(missing_ok=True)
                        downloaded_paths.remove(path)
                    results_by_index[index] = self._deduplicated_media_result(
                        batch_scope.content_fingerprint(data)
                    )
                    continue
                if total_bytes + len(data) > self._max_media_total_bytes:
                    return preflight_failure(
                        "图片总大小超过 OneBot11 单条消息限制",
                        "too_large",
                    )
                batch_scope.remember(raw_source, data)
                total_bytes += len(data)
                prepared.append((index, str(local_path), caption, raw_source))

            for prepared_index, (index, path, caption, raw_source) in enumerate(prepared):
                if human_delay > 0:
                    await asyncio.sleep(human_delay)
                result = await self.send_image_file(
                    chat_id,
                    path,
                    caption=caption or None,
                    metadata=metadata,
                    _onebot11_media_scope=delivery_scope,
                    _onebot11_media_source=raw_source,
                )
                results_by_index[index] = result
                if result.error_kind in {"unknown", "fenced"}:
                    for original_index, _path, _caption, _source in prepared[
                        prepared_index + 1 :
                    ]:
                        results_by_index[original_index] = SendResult(
                            False,
                            error="前一张图片出站结果未知，已跳过后续图片",
                            error_kind="unknown",
                        )
                    break
            return [
                result
                if result is not None
                else SendResult(
                    False,
                    error="图片未完成出站",
                    error_kind="failed",
                )
                for result in results_by_index
            ]
        finally:
            self._cleanup_media(downloaded_paths, media_dir=media_dir)

    async def _send_with_retry(self, chat_id: str, content: str, reply_to: str | None = None, metadata: Any = None, **kwargs: Any) -> SendResult:
        """覆盖 Hermes 默认重试/fallback，避免未知出站重复发送。"""
        del kwargs
        # Hermes 最终回复统一走 _send_with_retry；标记当前调用是最终回复，
        # send() 据此区分直调的中间正文（commentary/progress/status）。
        token = _FINAL_DELIVERY.set(True)
        try:
            return await self.send(
                chat_id,
                content,
                reply_to=reply_to,
                metadata=metadata if isinstance(metadata, Mapping) else None,
            )
        finally:
            _FINAL_DELIVERY.reset(token)

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

    def _resolve_target(
        self,
        chat_id: str,
        metadata: Any,
        *,
        binding: TurnBinding | None = None,
    ) -> ChatTarget | None:
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
        resolved_binding = binding or self._binding_from_context(
            metadata if isinstance(metadata, Mapping) else None
        )
        if resolved_binding is not None:
            bound_target = resolved_binding.caller.target()
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

    def _binding_is_current(
        self,
        binding: TurnBinding,
        *,
        expected_caller: CallerContext | None = None,
        require_self_id: bool = False,
        check_lease: bool = False,
    ) -> bool:
        """验证 binding 仍属于当前 adapter、目标和 lease。"""
        current = self._bindings.get(binding.session_id, binding.turn_id)
        if current != binding:
            return False
        if expected_caller is not None and expected_caller != binding.caller:
            return False
        caller = binding.caller
        if require_self_id and not caller.self_id:
            return False
        if caller.self_id and caller.self_id != self.self_id:
            return False
        if (
            caller.adapter_epoch is not None
            and caller.adapter_epoch != self._adapter_epoch
        ):
            return False
        if not self._chat_access_allowed(
            caller.chat_type,
            caller.chat_id,
            caller.user_id,
        ):
            return False
        if binding.lease_id != caller.lease_id:
            return False
        if check_lease and binding.lease_id:
            try:
                if not self._lease_is_current(binding.lease_id):
                    return False
                if not self._lease_matches_target(
                    binding.lease_id,
                    caller.chat_type,
                    caller.chat_id,
                ):
                    return False
            except (OSError, RuntimeError, TypeError, ValueError):
                return False
        return True

    def _log_binding_diagnostic(
        self,
        metadata: Mapping[str, Any] | None,
        *,
        reason: str,
    ) -> None:
        """按 session/turn/lease 只记录一次 binding 恢复诊断。"""
        current_event = _CURRENT_EVENT.get()
        event_metadata = getattr(current_event, "metadata", None) or {}
        sources = [event_metadata, metadata]
        session_id = ""
        turn_id = ""
        lease_id = ""
        for source in sources:
            if not isinstance(source, Mapping):
                continue
            key = _binding_key_from_metadata(source)
            if key is not None:
                session_id, turn_id = key
            raw_lease_id = str(source.get("onebot11_lease_id") or "").strip()
            if raw_lease_id:
                lease_id = raw_lease_id
        diagnostic_key = (session_id, turn_id, lease_id, reason)
        if diagnostic_key in self._binding_diagnostic_keys:
            return
        self._binding_diagnostic_keys.add(diagnostic_key)
        logger.warning(
            "OneBot11 managed outbound binding unavailable: "
            "session_id=%s turn_id=%s lease_id=%s reason=%s",
            session_id or "<missing>",
            turn_id or "<missing>",
            lease_id or "<missing>",
            reason,
        )

    def _binding_from_context(
        self,
        metadata: Mapping[str, Any] | None = None,
        *,
        chat_id: str | None = None,
    ) -> TurnBinding | None:
        """恢复当前 turn binding；缺失 metadata 时仅允许唯一活动 lease。"""
        current_event = _CURRENT_EVENT.get()
        event_metadata = getattr(current_event, "metadata", None) or {}
        sources = [
            source
            for source in (event_metadata, metadata)
            if isinstance(source, Mapping)
        ]
        binding_keys = [
            key
            for source in sources
            if (key := _binding_key_from_metadata(source)) is not None
        ]
        if binding_keys and any(key != binding_keys[0] for key in binding_keys[1:]):
            return None
        metadata_key = binding_keys[0] if binding_keys else None
        context_binding = _CURRENT_BINDING.get()

        for source in sources:
            raw_caller = source.get("onebot11_caller_context")
            if isinstance(raw_caller, Mapping):
                raw_self_id = raw_caller.get("self_id")
                if (
                    raw_self_id is not None
                    and str(raw_self_id).strip() != self.self_id
                ):
                    return None
            raw_lease_id = str(source.get("onebot11_lease_id") or "").strip()
            if raw_lease_id:
                # 先在 metadata 边界检查 lease 冲突，再交给下面的
                # _lease_is_current 检查它是否仍然有效。
                if (
                    context_binding is not None
                    and raw_lease_id
                    != str(context_binding.lease_id or "")
                ):
                    return None

        current_caller = _CURRENT_CALLER.get()
        if context_binding is not None:
            # ContextVar 可能跨 reconnect/取消边界残留；只有 binding store
            # 中仍是同一个对象，并且 caller 没有串到另一个 turn，才可复用。
            if not self._binding_is_current(
                context_binding,
                expected_caller=current_caller,
            ):
                return None
            context_key = (context_binding.session_id, context_binding.turn_id)
            if metadata_key is not None and metadata_key != context_key:
                return None
            return context_binding

        if metadata_key is None:
            # Hermes 的中途/状态回调可能只携带 chat_id。此时不从
            # session key 或最近来源猜测身份，只接受该群唯一活动 lease
            # 对应的 binding，并继续执行完整 fencing 校验。
            onebot_evidence = bool(_CURRENT_ONEBOT_CONTEXT.get()) or any(
                isinstance(source, Mapping)
                and any(
                    source.get(key) is not None
                    for key in (
                        "onebot11_managed_context",
                        "onebot11_lease_id",
                        "onebot11_caller_context",
                        "onebot11_binding_key",
                    )
                )
                for source in sources
            )
            if not onebot_evidence:
                return None
            if chat_id is None:
                return None
            normalized_chat_id = str(chat_id).strip()
            if not normalized_chat_id:
                return None
            active = self._dispatcher.active(normalized_chat_id)
            if active is None or active.lease_lost:
                return None
            binding = self._bindings.get_by_lease(active.lease.lease_id)
            if binding is None:
                return None
            if current_caller is not None and current_caller != binding.caller:
                return None
            for source in sources:
                raw_lease_id = str(source.get("onebot11_lease_id") or "").strip()
                if raw_lease_id and raw_lease_id != str(binding.lease_id or ""):
                    return None
                raw_caller = source.get("onebot11_caller_context")
                if isinstance(raw_caller, Mapping):
                    if any(
                        str(raw_caller.get(name) or "")
                        != str(getattr(binding.caller, name) or "")
                        for name in ("user_id", "chat_type", "chat_id", "lease_id", "self_id")
                    ):
                        return None
            if (
                binding.caller.chat_type != "group"
                or binding.caller.chat_id != normalized_chat_id
                or not self._binding_is_current(
                    binding,
                    require_self_id=True,
                    check_lease=True,
                )
            ):
                return None
            return binding
        binding = self._bindings.get(*metadata_key)
        if binding is None:
            return None
        if current_caller is not None and current_caller != binding.caller:
            return None
        # Metadata 恢复是跨线程/任务边界的安全入口，要求 snapshot 带有
        # 当前 bot self_id；缺失或伪造身份都不能获得出站能力。
        if not self._binding_is_current(
            binding,
            require_self_id=True,
            check_lease=True,
        ):
            return None
        for source in sources:
            raw_lease_id = str(source.get("onebot11_lease_id") or "").strip()
            if raw_lease_id and raw_lease_id != str(binding.lease_id or ""):
                return None
        return binding

    def _binding_for_outbound(
        self,
        chat_id: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> TurnBinding | None:
        """读取出站 binding 的统一入口，显式传入当前目标。"""
        return self._binding_from_context(metadata, chat_id=str(chat_id))

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
            delegated_child = _is_delegated_child_turn(kwargs)
            raw_session_id = kwargs.get("session_id")
            raw_turn_id = kwargs.get("turn_id")
            session_id = str(raw_session_id or "").strip()
            turn_id = str(raw_turn_id or "").strip()
            if (raw_session_id is None) != (raw_turn_id is None):
                return json.dumps(
                    {
                        "status": "permission_error",
                        "error": "当前 turn 身份绑定不存在",
                    },
                    ensure_ascii=False,
                )
            if raw_session_id is not None or raw_turn_id is not None:
                if not session_id or not turn_id:
                    return json.dumps(
                        {
                            "status": "permission_error",
                            "error": "当前 turn 身份绑定不存在",
                        },
                        ensure_ascii=False,
                    )
                binding = self._resolve_binding(session_id, turn_id)
                context_binding = _CURRENT_BINDING.get()
                context_caller = _CURRENT_CALLER.get()
                if (
                    binding is None
                    or (
                        context_binding is not None
                        and context_binding != binding
                    )
                    or (
                        context_caller is not None
                        and context_caller != binding.caller
                    )
                ):
                    binding = None
            else:
                binding = self._binding_from_context()
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
            error = validate_tool_call(
                tool_name,
                args,
                caller,
                self.super_admins,
                main_agent_read_only=self.policy_snapshot.main_agent_read_only,
                delegated_child=delegated_child,
            )
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
        if self._closed:
            self._audit.record(
                "permission_denied",
                {
                    "tool": str(getattr(confirmation, "tool_name", ""))[:128],
                    "user_id": str(getattr(confirmation, "user_id", ""))[:128],
                    "chat_type": str(getattr(confirmation, "chat_type", ""))[:32],
                    "chat_id": str(getattr(confirmation, "chat_id", ""))[:128],
                    "reason": "adapter 已关闭",
                },
            )
            return {"status": "permission_error", "error": "OneBot11 adapter 已关闭"}
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
            elif command == "reload":
                success, message = await self.reload_policy(force=True)
                await self._send_direct(
                    event,
                    f"OneBot11 policy reload {'成功' if success else '失败'}: {message}",
                )
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
                trigger_snapshot["llm_max_failures"] = (
                    self.trigger_config.llm_max_failures
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
                status["policy"] = {
                    "version": self._policy_snapshot.version,
                    "loaded_at": self._policy_snapshot.loaded_at,
                    "reload_error": self._policy_reload_error,
                    "plain_text_enabled": self.plain_text_enabled,
                }
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
                    if action == "retry" and count:
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
                    if action == "retry" and not count:
                        await self._send_direct(
                            event,
                            "没有可安全重试的旧 anchor；原 authority 或 batch 无法证明，"
                            "请使用 discard，或发送新的明确 @/关键词消息。",
                        )
                        return
                    await self._send_direct(
                        event,
                        f"已处理 uncertain/failed 消息 {count} 条: {action}"
                        + (
                            "（已生成新的 anchor，保留原 authority；可能重复执行）"
                            if action == "retry"
                            else ""
                        ),
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
                    "reload|resolve retry|resolve discard|resolve action retry|discard OPERATION_ID|confirm TOKEN",
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
        _CURRENT_ONEBOT_CONTEXT.set(False)
        return
    caller = _caller_from_metadata((getattr(event, "metadata", None) or {}).get("onebot11_caller_context"))
    _CURRENT_CALLER.set(caller)
    _CURRENT_BINDING.set(None)
    _CURRENT_ONEBOT_CONTEXT.set(True)


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
    _CURRENT_ONEBOT_CONTEXT.set(False)


def _pre_llm_call_hook(session_id: str = "", turn_id: str = "", platform: Any = "", **kwargs: Any) -> dict[str, str] | None:
    """绑定当前 Hermes turn 的 caller，并注入角色/工具提示。"""
    delegated_child = _is_delegated_child_turn({"platform": platform, **kwargs})
    platform_value = _platform_value(platform)
    caller = _CURRENT_CALLER.get()
    context_binding = _CURRENT_BINDING.get()
    normalized_session_id = str(session_id or "").strip()
    normalized_turn_id = str(turn_id or "").strip()
    adapter = _get_live_adapter()
    if not delegated_child and _event_binding_conflicts(
        _CURRENT_EVENT.get(),
        normalized_session_id,
        normalized_turn_id,
    ):
        return {
            "context": "OneBot11 event metadata 与显式 turn binding 冲突；所有工具必须拒绝。"
        }
    exact_binding = None
    if (
        adapter is not None
        and normalized_session_id
        and normalized_turn_id
    ):
        resolve_binding = getattr(adapter, "_resolve_binding", None)
        if callable(resolve_binding):
            exact_binding = resolve_binding(
                normalized_session_id,
                normalized_turn_id,
            )
    onebot_evidence = (
        caller is not None
        or context_binding is not None
        or _CURRENT_ONEBOT_CONTEXT.get()
        or _event_declares_onebot_turn(_CURRENT_EVENT.get())
        or exact_binding is not None
    )
    if platform_value and platform_value != _PLATFORM_NAME:
        if not onebot_evidence:
            return None
        if not delegated_child:
            return {"context": "OneBot11 platform 与当前 turn binding 冲突；所有 OneBot11 工具必须拒绝。"}
    if not platform_value and not onebot_evidence:
        return None
    if adapter is None:
        return {"context": "OneBot11 adapter unavailable; all OneBot11 tools must be denied."}
    if (
        not delegated_child
        and context_binding is not None
        and normalized_session_id
        and normalized_turn_id
        and (
            context_binding.session_id != normalized_session_id
            or context_binding.turn_id != normalized_turn_id
        )
    ):
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}
    if context_binding is not None and exact_binding is not None:
        if context_binding != exact_binding:
            return {"context": "OneBot11 ContextVar 与显式 turn binding 冲突；所有工具必须拒绝。"}
    if caller is None and exact_binding is not None:
        caller = exact_binding.caller
    if caller is None and context_binding is not None:
        caller = context_binding.caller
    if caller is None:
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}
    if exact_binding is not None and exact_binding.caller != caller:
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller 与显式 turn binding 冲突；所有工具必须拒绝。"}
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
    if not delegated_child and (not normalized_session_id or not normalized_turn_id):
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller binding unavailable; all OneBot11 tools must be denied."}
    if delegated_child:
        if context_binding is None:
            return {"context": "OneBot11 delegated child lacks parent turn binding; all tools must be denied."}
        # delegated child 可能只带自己的 session/turn 坐标，没有原始 event
        # 的 ContextVar；确认父 binding 后显式保留 OneBot lineage。
        _CURRENT_ONEBOT_CONTEXT.set(True)
        return {
            "context": role_prompt(
                caller,
                adapter.role_tools,
                main_agent_read_only=adapter.policy_snapshot.main_agent_read_only,
                delegated_child=True,
            )
        }
    binding = exact_binding or TurnBinding(
        normalized_session_id,
        normalized_turn_id,
        caller,
        caller.lease_id,
    )
    try:
        adapter._bindings.bind(binding)
    except ValueError:
        _clear_current_turn_binding(adapter)
        return {"context": "OneBot11 caller turn binding conflict; all OneBot11 tools must be denied."}
    # worker thread 可能只带精确 session/turn 坐标，没有原始 event 的
    # ContextVar；绑定成功后显式保留 OneBot lineage，供后续 delegated child
    # 继续经过父 turn 的 binding/lease 门禁。
    _CURRENT_ONEBOT_CONTEXT.set(True)
    _CURRENT_BINDING.set(binding)
    current_event = _CURRENT_EVENT.get()
    if current_event is not None:
        metadata = dict(getattr(current_event, "metadata", None) or {})
        metadata["onebot11_binding_key"] = {
            "session_id": normalized_session_id,
            "turn_id": normalized_turn_id,
        }
        current_event.metadata = metadata
    return {
        "context": role_prompt(
            caller,
            adapter.role_tools,
            main_agent_read_only=adapter.policy_snapshot.main_agent_read_only,
        )
    }


def _safe_audit(adapter: Any, action: str, fields: Mapping[str, Any]) -> None:
    """审计失败时只写日志，绝不改变权限 hook 的 fail-closed 结果。"""
    try:
        audit = getattr(adapter, "_audit", None)
        record = getattr(audit, "record", None)
        if callable(record):
            record(action, fields)
    except Exception:
        logger.warning("OneBot11 audit hook failed: action=%s", action, exc_info=True)


def _event_declares_onebot_turn(event: Any) -> bool:
    """判断当前 Hermes hook 是否带有 OneBot turn 身份。"""
    source = getattr(event, "source", None)
    if _platform_value(getattr(source, "platform", None)) == _PLATFORM_NAME:
        return True
    metadata = getattr(event, "metadata", None) or {}
    return isinstance(metadata, Mapping) and any(
        key in metadata
        for key in (
            "onebot11_managed_context",
            "onebot11_caller_context",
            "onebot11_binding_key",
            "onebot11_lease_id",
        )
    )


def _is_delegated_child_turn(kwargs: Mapping[str, Any] | None) -> bool:
    """判断 Hermes 当前工具调用是否来自 delegate_task 子代理。

    Hermes 0.20 在子代理执行 ContextVar 中设置 delegated-child 标记；
    platform 字段只用于提示和路由，不能单独作为提权证据。标记缺失时按
    主 agent 处理，这样旧 Hermes 或伪造 metadata 不会意外获得更高权限。
    """
    values = kwargs or {}
    del values
    try:
        from agent.delegation_context import is_delegated_child_context

        return bool(is_delegated_child_context())
    except (ImportError, AttributeError):
        return False


def _event_binding_conflicts(
    event: Any,
    session_id: str,
    turn_id: str,
) -> bool:
    """拒绝显式 turn 坐标借用当前事件声明的另一个 binding。"""
    if not session_id or not turn_id:
        return False
    metadata = getattr(event, "metadata", None) or {}
    declared = _binding_key_from_metadata(metadata)
    return declared is not None and declared != (session_id, turn_id)


def _pre_tool_call_hook(
    tool_name: str = "",
    session_id: str = "",
    turn_id: str = "",
    args: dict | None = None,
    **kwargs: Any,
) -> dict[str, str] | None:
    """在 Hermes 任意工具执行前硬拦截 OneBot turn 的越权调用。"""
    normalized_tool_name = str(tool_name or "").strip()
    explicit_platform = _platform_value(kwargs.get("platform"))
    current_event = _CURRENT_EVENT.get()
    normalized_session_id = str(session_id or "").strip()
    normalized_turn_id = str(turn_id or "").strip()
    adapter = _get_live_adapter()
    delegated_child = _is_delegated_child_turn(kwargs)
    if _event_binding_conflicts(
        current_event,
        normalized_session_id,
        normalized_turn_id,
    ) and not delegated_child:
        return {
            "action": "block",
            "message": "OneBot11 event metadata 与显式 turn binding 冲突",
        }
    explicit_onebot = explicit_platform == _PLATFORM_NAME
    context_evidence = (
        _CURRENT_CALLER.get() is not None
        or _CURRENT_BINDING.get() is not None
        or _CURRENT_ONEBOT_CONTEXT.get()
        or _event_declares_onebot_turn(current_event)
    )
    exact_binding = None
    if (
        adapter is not None
        and normalized_session_id
        and normalized_turn_id
    ):
        exact_binding = adapter._resolve_binding(
            normalized_session_id,
            normalized_turn_id,
        )
    binding_evidence = exact_binding is not None
    if (
        explicit_platform
        and explicit_platform != _PLATFORM_NAME
        and not delegated_child
        and (context_evidence or binding_evidence)
    ):
        return {
            "action": "block",
            "message": "OneBot11 platform 与 binding 冲突",
        }
    delegated_onebot_context = delegated_child and (context_evidence or binding_evidence)
    onebot_context = (
        explicit_onebot
        or context_evidence
        or binding_evidence
        or delegated_onebot_context
        or normalized_tool_name.startswith("qq_")
    )
    if not onebot_context:
        # 这是全局 Hermes hook；没有 OneBot caller 时必须让其他平台继续
        # 使用自己的工具策略，不能因为 OneBot adapter 正在运行而拦截。
        return None
    if adapter is None:
        return {"action": "block", "message": "OneBot11 adapter unavailable"}
    if getattr(adapter, "_closed", True):
        return {"action": "block", "message": "OneBot11 adapter 已关闭"}
    if normalized_tool_name.startswith("qq_") and normalized_tool_name not in ALL_TOOLS:
        return {"action": "block", "message": "权限错误: 未知 OneBot11 工具"}
    if normalized_tool_name in FORBIDDEN_TOOL_NAMES:
        return {
            "action": "block",
            "message": f"权限错误: OneBot11 当前禁止调用 {normalized_tool_name}",
        }
    try:
        binding = exact_binding or adapter._resolve_binding(
            normalized_session_id,
            normalized_turn_id,
        )
        if binding is None and delegated_child:
            # Hermes delegate_task 子代理使用自己的 session/turn 坐标，但
            # 仍运行在父 OneBot turn 的 context 中；继承父 binding 后再
            # 执行一次 delegated-child 的工具限制，不能把子代理坐标
            # 当成新的 OneBot 身份。
            binding = _CURRENT_BINDING.get()
        if binding is None:
            return {"action": "block", "message": "OneBot11 current turn binding unavailable"}
        context_binding = _CURRENT_BINDING.get()
        current_caller = _CURRENT_CALLER.get()
        if context_binding is not None and context_binding != binding:
            return {
                "action": "block",
                "message": "OneBot11 ContextVar 与显式 turn binding 冲突",
            }
        if current_caller is not None and current_caller != binding.caller:
            return {
                "action": "block",
                "message": "OneBot11 ContextVar caller 与显式 turn binding 冲突",
            }
        # Hermes 的 file 工具对 config.yaml 有写保护，但 terminal 可以绕过；
        # 这里对 OneBot turn 的 terminal 命令做统一兜底，禁止写安全敏感配置。
        if normalized_tool_name == "terminal":
            raw_command = (args or {}).get("command")
            if isinstance(raw_command, str):
                config_error = terminal_writes_sensitive_config(raw_command)
                if config_error:
                    _safe_audit(
                        adapter,
                        "permission_denied",
                        {
                            "tool": normalized_tool_name,
                            "user_id": binding.caller.user_id,
                            "chat_type": binding.caller.chat_type,
                            "chat_id": binding.caller.chat_id,
                            "reason": config_error,
                        },
                    )
                    return {
                        "action": "block",
                        "message": f"权限错误: {config_error}",
                    }
        if normalized_tool_name in {"write_file", "patch"}:
            # Hermes file 工具只保护 config.yaml；roles.yaml 是插件自己的
            # 白名单文件，必须在这里同样 fail-closed，防止 write_file/patch
            # 绕过 terminal 兜底。
            raw_path = (args or {}).get("path")
            config_error = (
                file_tool_writes_sensitive_config(str(raw_path))
                if isinstance(raw_path, str)
                else None
            )
            if config_error:
                _safe_audit(
                    adapter,
                    "permission_denied",
                    {
                        "tool": normalized_tool_name,
                        "user_id": binding.caller.user_id,
                        "chat_type": binding.caller.chat_type,
                        "chat_id": binding.caller.chat_id,
                        "reason": config_error,
                    },
                )
                return {
                    "action": "block",
                    "message": f"权限错误: {config_error}",
                }
        if normalized_tool_name == "read_file" and not delegated_child:
            # 主 agent 只读模式下也不能把 .env/auth.json 里的凭据读进
            # 上下文；config.yaml/roles.yaml 仍可读。子代理拥有 shell，
            # 属于受信执行环境，不在此处追加读保护。
            raw_path = (args or {}).get("path")
            read_error = (
                file_tool_reads_sensitive_config(str(raw_path))
                if isinstance(raw_path, str)
                else None
            )
            if read_error:
                _safe_audit(
                    adapter,
                    "permission_denied",
                    {
                        "tool": normalized_tool_name,
                        "user_id": binding.caller.user_id,
                        "chat_type": binding.caller.chat_type,
                        "chat_id": binding.caller.chat_id,
                        "reason": read_error,
                    },
                )
                return {
                    "action": "block",
                    "message": f"权限错误: {read_error}",
                }
        if (
            binding.caller.adapter_epoch is not None
            and binding.caller.adapter_epoch != adapter._adapter_epoch
        ):
            return {"action": "block", "message": "权限错误: 当前 adapter epoch 已失效"}
        if binding.lease_id and not adapter._lease_is_current(binding.lease_id):
            _safe_audit(
                adapter,
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
            _safe_audit(
                adapter,
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
        error = validate_tool_call(
            normalized_tool_name,
            args or {},
            binding.caller,
            adapter.super_admins,
            main_agent_read_only=adapter.policy_snapshot.main_agent_read_only,
            delegated_child=delegated_child,
        )
        if error:
            _safe_audit(
                adapter,
                "permission_denied",
                {
                    "tool": normalized_tool_name,
                    "user_id": binding.caller.user_id,
                    "chat_type": binding.caller.chat_type,
                    "chat_id": binding.caller.chat_id,
                    "reason": error,
                },
            )
            return {"action": "block", "message": f"权限错误: {error}"}
        return None
    except Exception as exc:
        _safe_audit(
            adapter,
            "permission_denied",
            {
                "tool": normalized_tool_name[:128],
                "session_id": str(session_id)[:128],
                "turn_id": str(turn_id)[:128],
                "reason": f"权限 hook 内部异常: {type(exc).__name__}",
            },
        )
        return {
            "action": "block",
            "message": "OneBot11 permission hook failed closed",
        }


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
    # 独立 roles 文件存在时还需要 PyYAML；roles 文件是可选的，缺失时
    # 不强制要求 yaml，避免没有角色文件的部署被额外依赖卡住。
    try:
        roles_path = roles_file_path({}, os.environ)
        if roles_path.expanduser().resolve().is_file():
            import yaml  # noqa: F401
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
            plain_message = format_onebot_text(str(message or "")).text
            message_id = await api.send_message(
                target.chat_id,
                plain_message,
                chat_type=target.chat_type,
            )
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


def _require_hermes_hook_capabilities(ctx: Any) -> Any:
    """确认安全门禁所需的 Hermes hooks 存在，否则拒绝注册平台。"""
    register_hook = getattr(ctx, "register_hook", None)
    if not callable(register_hook):
        raise RuntimeError(
            "OneBot11 需要 Hermes pre_gateway_dispatch、pre_llm_call 和 "
            "pre_tool_call hooks；当前插件上下文没有 register_hook"
        )
    try:
        from hermes_cli.plugins import VALID_HOOKS
    except (ImportError, AttributeError) as exc:
        raise RuntimeError(
            "OneBot11 无法验证 Hermes 安全 hooks，拒绝启用以避免 fail-open"
        ) from exc
    missing = _REQUIRED_HERMES_HOOKS - set(VALID_HOOKS)
    if missing:
        raise RuntimeError(
            "OneBot11 所需 Hermes hooks 不可用，拒绝启用: "
            + ", ".join(sorted(missing))
        )
    return register_hook


def register(ctx: Any) -> None:
    """注册平台、全角色工具和权限 hooks。"""
    register_hook = _require_hermes_hook_capabilities(ctx)
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
        platform_hint=(
            "You are chatting via OneBot 11 (QQ). Group messages share one session "
            "and are prefixed with the sender nickname. Use plain text by default; "
            "do not emit Markdown tables, code fences, or Markdown images. "
            "Long-running notices are control-plane messages only when Hermes "
            "supplies explicit metadata."
        ),
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
