"""OneBot 11 运行时配置解析。

本模块不导入 Hermes。构造 adapter 和平台 ``validate_config`` 共用这里的
解析合同，避免验证阶段接受一个运行阶段会被静默夹紧或延迟失败的值。
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .http_api import is_loopback_http_url, parse_http_base_url
from .permissions import (
    AccessPolicy,
    build_access_policy,
    build_role_tools,
    parse_bool,
    parse_id_list,
)
from .triggers import TriggerConfig, build_trigger_config

_PROCESSING_REACTION_EMOJI_ID = "128064"


@dataclass(frozen=True)
class RuntimeConfig:
    """已完成类型和边界校验的 OneBot 运行时配置。"""

    extra: dict[str, Any]
    http_api: str
    self_id: str
    ws_host: str
    ws_port: int
    access_token: str
    access_policy: AccessPolicy
    role_tools: dict[str, frozenset[str]]
    trigger_config: TriggerConfig
    processing_reaction_enabled: bool
    processing_reaction_emoji_id: str
    media_allowed_hosts: frozenset[str]
    media_allowed_ports: frozenset[int]
    http_timeout_seconds: float
    query_max_retries: int
    http_max_response_bytes: int
    max_image_bytes: int
    max_image_redirects: int
    max_image_total_bytes: int
    max_images_per_message: int
    queue_db_path: str | None
    queue_max_messages: int
    queue_max_bytes: int
    queue_max_message_bytes: int
    queue_max_original_bytes: int
    queue_max_summary_bytes: int
    queue_recent_originals: int
    queue_dedupe_ttl_seconds: float
    queue_max_attempts: int
    agent_input_bytes: int
    agent_recent_originals: int
    queue_lease_seconds: float
    queue_heartbeat_seconds: float | None
    queue_recovery_poll_seconds: float
    confirm_ttl_seconds: float
    audit_path: str | None
    audit_max_bytes: int
    media_orphan_ttl_seconds: float
    ws_max_queue: int
    ws_max_inflight: int
    home_channel_type: str | None


def effective_extra(
    extra: Mapping[str, Any],
    environ: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """合并环境覆盖；显式空字符串仍然覆盖 YAML 值。"""
    if not isinstance(extra, Mapping):
        raise ValueError("OneBot11 extra 必须是 mapping")
    env = os.environ if environ is None else environ
    result = dict(extra)
    fields = {
        "HTTP_API": "http_api",
        "SELF_ID": "self_id",
        "ACCESS_TOKEN": "access_token",
        "SESSION_MODE": "session_mode",
        "GROUP_SESSIONS_PER_USER": "group_sessions_per_user",
        "WS_PORT": "ws_port",
        "WS_HOST": "ws_host",
        "DM_POLICY": "dm_policy",
        "ALLOWED_USERS": "allowed_users",
        "ALLOWED_GROUPS": "allowed_groups",
        "REQUIRE_MENTION": "require_mention",
        "SUPER_ADMINS": "super_admins",
        "ADMINS": "admins",
        "QUEUE_DB": "queue_db_path",
        "HOME_CHANNEL_TYPE": "home_channel_type",
        "PROCESSING_REACTION_ENABLED": "processing_reaction_enabled",
        "PROCESSING_REACTION_EMOJI_ID": "processing_reaction_emoji_id",
        "HTTP_TIMEOUT_SECONDS": "http_timeout_seconds",
        "QUERY_MAX_RETRIES": "query_max_retries",
        "HTTP_MAX_RESPONSE_BYTES": "http_max_response_bytes",
        "MAX_IMAGE_BYTES": "max_image_bytes",
        "MAX_IMAGE_REDIRECTS": "max_image_redirects",
        "MAX_IMAGE_TOTAL_BYTES": "max_image_total_bytes",
        "MAX_IMAGES_PER_MESSAGE": "max_images_per_message",
        "QUEUE_MAX_MESSAGES": "queue_max_messages",
        "QUEUE_MAX_BYTES": "queue_max_bytes",
        "QUEUE_MAX_MESSAGE_BYTES": "queue_max_message_bytes",
        "QUEUE_MAX_ORIGINAL_BYTES": "queue_max_original_bytes",
        "QUEUE_MAX_SUMMARY_BYTES": "queue_max_summary_bytes",
        "QUEUE_RECENT_ORIGINALS": "queue_recent_originals",
        "QUEUE_DEDUPE_TTL_SECONDS": "queue_dedupe_ttl_seconds",
        "QUEUE_MAX_ATTEMPTS": "queue_max_attempts",
        "AGENT_INPUT_BYTES": "agent_input_bytes",
        "AGENT_RECENT_ORIGINALS": "agent_recent_originals",
        "QUEUE_LEASE_SECONDS": "queue_lease_seconds",
        "QUEUE_HEARTBEAT_SECONDS": "queue_heartbeat_seconds",
        "QUEUE_RECOVERY_POLL_SECONDS": "queue_recovery_poll_seconds",
        "CONFIRM_TTL_SECONDS": "confirm_ttl_seconds",
        "AUDIT_PATH": "audit_path",
        "AUDIT_MAX_BYTES": "audit_max_bytes",
        "MEDIA_ORPHAN_TTL_SECONDS": "media_orphan_ttl_seconds",
        "WS_MAX_QUEUE": "ws_max_queue",
        "WS_MAX_INFLIGHT": "ws_max_inflight",
    }
    for suffix, field in fields.items():
        name = f"ONEBOT11_{suffix}"
        if name in env:
            result[field] = env[name]
    return result


def _string(value: Any, *, name: str, default: str = "") -> str:
    """解析非空或可为空的字符串配置。"""
    if value is None:
        value = default
    if not isinstance(value, (str, int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须是字符串")
    return str(value).strip()


def _int(
    value: Any,
    *,
    name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    """严格解析整数，拒绝 bool、NaN、Infinity 和小数。"""
    if value is None:
        value = default
    if isinstance(value, bool):
        raise ValueError(f"{name} 必须是整数")
    try:
        parsed_float = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if not math.isfinite(parsed_float) or not parsed_float.is_integer():
        raise ValueError(f"{name} 必须是整数")
    parsed = int(parsed_float)
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} 必须在 {minimum} 至 {maximum} 之间")
    return parsed


def _float(
    value: Any,
    *,
    name: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    """严格解析有限浮点数。"""
    if value is None:
        value = default
    if isinstance(value, bool):
        raise ValueError(f"{name} 必须是数字")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是数字") from exc
    if not math.isfinite(parsed) or parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} 必须在 {minimum} 至 {maximum} 之间")
    return parsed


def _optional_string(value: Any, *, name: str) -> str | None:
    """解析可选路径类字符串。"""
    if value is None or value == "":
        return None
    result = _string(value, name=name)
    return result or None


def parse_runtime_config(
    extra: Mapping[str, Any],
    environ: Mapping[str, Any] | None = None,
    *,
    require_http_api: bool = False,
) -> RuntimeConfig:
    """解析完整运行时配置；``require_http_api`` 仅用于平台启用校验。"""
    env = os.environ if environ is None else environ
    effective = effective_extra(extra, env)
    http_api = _string(effective.get("http_api"), name="http_api")
    if http_api:
        parse_http_base_url(http_api)
    elif require_http_api:
        raise ValueError("ONEBOT11_HTTP_API 未配置")
    self_id = _string(effective.get("self_id"), name="self_id")
    if not self_id:
        raise ValueError("ONEBOT11_SELF_ID 未配置")
    session_mode = _string(effective.get("session_mode", "shared"), name="session_mode").casefold()
    if session_mode != "shared":
        raise ValueError("OneBot11 群 session 只允许 session_mode=shared")
    if parse_bool(
        effective.get("group_sessions_per_user"),
        default=False,
        name="group_sessions_per_user",
    ):
        raise ValueError("OneBot11 不允许 group_sessions_per_user=true")

    ws_port = _int(effective.get("ws_port"), name="ws_port", default=18880, minimum=0, maximum=65535)
    ws_host = _string(effective.get("ws_host"), name="ws_host", default="127.0.0.1")
    if not ws_host:
        raise ValueError("ws_host 不能为空")
    access_token = _string(effective.get("access_token"), name="access_token")
    loopback_hosts = {"127.0.0.1", "::1", "localhost"}
    if ws_host not in loopback_hosts and not access_token:
        raise ValueError("WS 非 loopback 地址必须配置 ONEBOT11_ACCESS_TOKEN")
    if http_api and not is_loopback_http_url(http_api) and not access_token:
        raise ValueError("HTTP API 非本机地址必须配置 ONEBOT11_ACCESS_TOKEN")

    home_channel_type = _optional_string(
        effective.get("home_channel_type"),
        name="home_channel_type",
    )
    if home_channel_type is not None:
        home_channel_type = home_channel_type.casefold()
        if home_channel_type not in {"group", "dm"}:
            raise ValueError("home_channel_type 必须是 group 或 dm")

    access_policy = build_access_policy(effective, env)
    role_tools = build_role_tools(effective)
    trigger_config = build_trigger_config(effective)
    reaction_enabled = parse_bool(
        effective.get("processing_reaction_enabled"),
        default=True,
        name="processing_reaction_enabled",
    )
    reaction_emoji = _string(
        effective.get("processing_reaction_emoji_id"),
        name="processing_reaction_emoji_id",
        default=_PROCESSING_REACTION_EMOJI_ID,
    )
    if not reaction_emoji:
        raise ValueError("processing_reaction_emoji_id 不能为空")

    media_hosts = frozenset(
        str(host).casefold().rstrip(".")
        for host in parse_id_list(effective.get("media_allowed_hosts"))
    )
    media_port_values = parse_id_list(effective.get("media_allowed_ports"))
    media_ports = frozenset(
        _int(port, name="media_allowed_ports", default=0, minimum=1, maximum=65535)
        for port in media_port_values
    )
    queue_lease_seconds = _float(
        effective.get("queue_lease_seconds"),
        name="queue_lease_seconds",
        default=120.0,
        minimum=5.0,
        maximum=86_400.0,
    )
    heartbeat_value = effective.get("queue_heartbeat_seconds")
    heartbeat_seconds = (
        None
        if heartbeat_value is None or heartbeat_value == ""
        else _float(
            heartbeat_value,
            name="queue_heartbeat_seconds",
            default=queue_lease_seconds / 3,
            minimum=0.1,
            maximum=max(0.1, queue_lease_seconds / 2),
        )
    )
    return RuntimeConfig(
        extra=effective,
        http_api=http_api,
        self_id=self_id,
        ws_host=ws_host,
        ws_port=ws_port,
        access_token=access_token,
        access_policy=access_policy,
        role_tools=role_tools,
        trigger_config=trigger_config,
        processing_reaction_enabled=reaction_enabled,
        processing_reaction_emoji_id=reaction_emoji,
        media_allowed_hosts=media_hosts,
        media_allowed_ports=media_ports,
        http_timeout_seconds=_float(
            effective.get("http_timeout_seconds"),
            name="http_timeout_seconds",
            default=10.0,
            minimum=0.1,
            maximum=300.0,
        ),
        query_max_retries=_int(
            effective.get("query_max_retries"),
            name="query_max_retries",
            default=1,
            minimum=0,
            maximum=10,
        ),
        http_max_response_bytes=_int(
            effective.get("http_max_response_bytes"),
            name="http_max_response_bytes",
            default=1_000_000,
            minimum=1_024,
            maximum=64 * 1024 * 1024,
        ),
        max_image_bytes=_int(
            effective.get("max_image_bytes"),
            name="max_image_bytes",
            default=8_000_000,
            minimum=1_024,
            maximum=128 * 1024 * 1024,
        ),
        max_image_redirects=_int(
            effective.get("max_image_redirects"),
            name="max_image_redirects",
            default=3,
            minimum=0,
            maximum=10,
        ),
        max_image_total_bytes=_int(
            effective.get("max_image_total_bytes"),
            name="max_image_total_bytes",
            default=16_000_000,
            minimum=1_024,
            maximum=256 * 1024 * 1024,
        ),
        max_images_per_message=_int(
            effective.get("max_images_per_message"),
            name="max_images_per_message",
            default=4,
            minimum=0,
            maximum=32,
        ),
        queue_db_path=_optional_string(effective.get("queue_db_path"), name="queue_db_path"),
        queue_max_messages=_int(
            effective.get("queue_max_messages"),
            name="queue_max_messages",
            default=1000,
            minimum=1,
            maximum=100_000,
        ),
        queue_max_bytes=_int(
            effective.get("queue_max_bytes"),
            name="queue_max_bytes",
            default=2_000_000,
            minimum=1,
            maximum=1_073_741_824,
        ),
        queue_max_message_bytes=_int(
            effective.get("queue_max_message_bytes"),
            name="queue_max_message_bytes",
            default=32_000,
            minimum=256,
            maximum=64 * 1024 * 1024,
        ),
        queue_max_original_bytes=_int(
            effective.get("queue_max_original_bytes"),
            name="queue_max_original_bytes",
            default=8_000,
            minimum=0,
            maximum=64 * 1024 * 1024,
        ),
        queue_max_summary_bytes=_int(
            effective.get("queue_max_summary_bytes"),
            name="queue_max_summary_bytes",
            default=16_000,
            minimum=256,
            maximum=64 * 1024 * 1024,
        ),
        queue_recent_originals=_int(
            effective.get("queue_recent_originals"),
            name="queue_recent_originals",
            default=3,
            minimum=0,
            maximum=100,
        ),
        queue_dedupe_ttl_seconds=_float(
            effective.get("queue_dedupe_ttl_seconds"),
            name="queue_dedupe_ttl_seconds",
            default=7 * 24 * 3600,
            minimum=60.0,
            maximum=31_536_000.0,
        ),
        queue_max_attempts=_int(
            effective.get("queue_max_attempts"),
            name="queue_max_attempts",
            default=3,
            minimum=1,
            maximum=10,
        ),
        agent_input_bytes=_int(
            effective.get("agent_input_bytes"),
            name="agent_input_bytes",
            default=64 * 1024,
            minimum=4_096,
            maximum=256 * 1024,
        ),
        agent_recent_originals=_int(
            effective.get("agent_recent_originals"),
            name="agent_recent_originals",
            default=3,
            minimum=0,
            maximum=100,
        ),
        queue_lease_seconds=queue_lease_seconds,
        queue_heartbeat_seconds=heartbeat_seconds,
        queue_recovery_poll_seconds=_float(
            effective.get("queue_recovery_poll_seconds"),
            name="queue_recovery_poll_seconds",
            default=5.0,
            minimum=0.05,
            maximum=3600.0,
        ),
        confirm_ttl_seconds=_float(
            effective.get("confirm_ttl_seconds"),
            name="confirm_ttl_seconds",
            default=60.0,
            minimum=1.0,
            maximum=86_400.0,
        ),
        audit_path=_optional_string(effective.get("audit_path"), name="audit_path"),
        audit_max_bytes=_int(
            effective.get("audit_max_bytes"),
            name="audit_max_bytes",
            default=2_000_000,
            minimum=1_024,
            maximum=256 * 1024 * 1024,
        ),
        media_orphan_ttl_seconds=_float(
            effective.get("media_orphan_ttl_seconds"),
            name="media_orphan_ttl_seconds",
            default=24 * 3600,
            minimum=60.0,
            maximum=31_536_000.0,
        ),
        ws_max_queue=_int(
            effective.get("ws_max_queue"),
            name="ws_max_queue",
            default=256,
            minimum=1,
            maximum=100_000,
        ),
        ws_max_inflight=_int(
            effective.get("ws_max_inflight"),
            name="ws_max_inflight",
            default=32,
            minimum=1,
            maximum=10_000,
        ),
        home_channel_type=home_channel_type,
    )
