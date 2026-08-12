"""OneBot 11 运行时配置解析。

本模块不导入 Hermes。构造 adapter 和平台 ``validate_config`` 共用这里的
解析合同，避免验证阶段接受一个运行阶段会被静默夹紧或延迟失败的值。
"""

from __future__ import annotations

import ipaddress
import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from types import MappingProxyType
from typing import Any

from .http_api import is_loopback_http_url, parse_http_base_url
from .permissions import (
    AccessPolicy,
    build_access_policy,
    build_role_tools,
    build_trusted_users,
    parse_admin_list,
    parse_bool,
    parse_id_list,
    parse_string_list,
)
from .triggers import TriggerConfig, build_trigger_config

_PROCESSING_REACTION_EMOJI_ID = "128172"  # LLBot 的 QQ Emoji「💬」ID，表示正在回复


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
    super_admins: frozenset[str]
    trusted_users: frozenset[str]
    role_tools: dict[str, frozenset[str]]
    main_agent_read_only: bool
    trigger_config: TriggerConfig
    processing_reaction_enabled: bool
    processing_reaction_emoji_id: str
    plain_text_enabled: bool
    long_running_notice_seconds: float
    show_interim_group: bool
    show_interim_dm: bool
    media_allowed_hosts: frozenset[str]
    media_allowed_ports: frozenset[int]
    media_source_roots: tuple[str, ...]
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
    home_channel: str | None
    home_channel_type: str | None


@dataclass(frozen=True)
class RuntimePolicySnapshot:
    """运行时可热替换的 OneBot 权限、触发和显示策略。"""

    version: int
    loaded_at: float
    access_policy: AccessPolicy
    super_admins: frozenset[str]
    trusted_users: frozenset[str]
    role_tools: Mapping[str, frozenset[str]]
    main_agent_read_only: bool
    trigger_config: TriggerConfig
    processing_reaction_enabled: bool
    processing_reaction_emoji_id: str
    plain_text_enabled: bool = True
    long_running_notice_seconds: float = 60.0
    show_interim_group: bool = False
    show_interim_dm: bool = True

    def __post_init__(self) -> None:
        """冻结角色工具 mapping，避免调用方绕过 snapshot 修改权限。"""
        object.__setattr__(
            self,
            "role_tools",
            MappingProxyType(
                {
                    str(role): frozenset(tools)
                    for role, tools in self.role_tools.items()
                }
            ),
        )


def build_policy_snapshot(
    runtime: RuntimeConfig,
    *,
    version: int,
    loaded_at: float,
) -> RuntimePolicySnapshot:
    """从已校验 RuntimeConfig 构造不可变热更新 snapshot。"""
    return RuntimePolicySnapshot(
        version=int(version),
        loaded_at=float(loaded_at),
        access_policy=runtime.access_policy,
        super_admins=frozenset(runtime.super_admins),
        trusted_users=frozenset(runtime.trusted_users),
        role_tools=runtime.role_tools,
        main_agent_read_only=runtime.main_agent_read_only,
        trigger_config=runtime.trigger_config,
        processing_reaction_enabled=runtime.processing_reaction_enabled,
        processing_reaction_emoji_id=runtime.processing_reaction_emoji_id,
        plain_text_enabled=runtime.plain_text_enabled,
        long_running_notice_seconds=runtime.long_running_notice_seconds,
        show_interim_group=runtime.show_interim_group,
        show_interim_dm=runtime.show_interim_dm,
    )


def runtime_static_fingerprint(runtime: RuntimeConfig) -> tuple[tuple[str, str], ...]:
    """返回 reload 不允许改变的连接、队列和协议配置摘要。"""
    hot_fields = {
        "access_policy",
        "super_admins",
        "trusted_users",
        "role_tools",
        "main_agent_read_only",
        "trigger_config",
        "processing_reaction_enabled",
        "processing_reaction_emoji_id",
        "plain_text_enabled",
        "long_running_notice_seconds",
        "show_interim_group",
        "show_interim_dm",
        "extra",
    }
    return tuple(
        (field.name, repr(getattr(runtime, field.name)))
        for field in fields(RuntimeConfig)
        if field.name not in hot_fields
    )


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
        "PLAIN_TEXT_ENABLED": "plain_text_enabled",
        "LONG_RUNNING_NOTICE_SECONDS": "long_running_notice_seconds",
        "HTTP_TIMEOUT_SECONDS": "http_timeout_seconds",
        "QUERY_MAX_RETRIES": "query_max_retries",
        "HTTP_MAX_RESPONSE_BYTES": "http_max_response_bytes",
        "MAX_IMAGE_BYTES": "max_image_bytes",
        "MAX_IMAGE_REDIRECTS": "max_image_redirects",
        "MAX_IMAGE_TOTAL_BYTES": "max_image_total_bytes",
        "MAX_IMAGES_PER_MESSAGE": "max_images_per_message",
        "MEDIA_SOURCE_ROOTS": "media_source_roots",
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
        "HOME_CHANNEL": "home_channel",
        "LLM_TRIGGER_PROVIDER": "llm_trigger_provider",
        "LLM_TRIGGER_MODEL": "llm_trigger_model",
        "LLM_TRIGGER_BASE_URL": "llm_trigger_base_url",
        "LLM_TRIGGER_API_KEY_ENV": "llm_trigger_api_key_env",
        "LLM_TRIGGER_ENABLED": "llm_trigger_enabled",
        "LLM_TRIGGER_GROUPS": "llm_trigger_groups",
        "MAIN_AGENT_READ_ONLY": "main_agent_read_only",
    }
    for suffix, field in fields.items():
        name = f"ONEBOT11_{suffix}"
        if name in env:
            result[field] = env[name]
    return result


def roles_file_path(
    extra: Mapping[str, Any],
    env: Mapping[str, Any],
) -> Path:
    """解析独立 roles 文件路径；未配置时使用 Hermes home 默认位置。"""
    raw = extra.get("roles_file")
    if raw is None:
        raw = env.get("ONEBOT11_ROLES_FILE")
    if raw is not None:
        raw_text = str(raw).strip()
        if raw_text:
            return Path(raw_text).expanduser()
    configured_home = str(env.get("HERMES_HOME") or "").strip()
    if configured_home:
        base = Path(configured_home).expanduser()
    elif os.name == "nt":
        local_appdata = str(env.get("LOCALAPPDATA") or "").strip()
        base = Path(local_appdata) if local_appdata else Path.home() / "AppData" / "Local"
        base = base / "hermes"
    else:
        base = Path.home() / ".hermes"
    return base / "onebot11" / "roles.yaml"


def _load_roles_overrides(path: Path) -> dict[str, Any]:
    """读取独立 roles 文件；文件不存在返回空 dict，非法结构直接抛错。"""
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        return {}
    try:
        import yaml  # noqa: PLC0415
    except ImportError as exc:
        raise ValueError(
            "读取 OneBot roles 文件需要 PyYAML，请安装后重试"
        ) from exc
    try:
        raw = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"OneBot roles 文件解析失败: {resolved}") from exc
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("OneBot roles 文件必须是 YAML mapping")
    allowed_keys = {"roles", "super_admins", "admins", "main_agent_read_only"}
    unknown = set(raw) - allowed_keys
    if unknown:
        raise ValueError(
            "OneBot roles 文件包含未知键: " + ", ".join(sorted(unknown))
        )
    return dict(raw)


def apply_roles_overrides(
    extra: Mapping[str, Any],
    environ: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """把独立 roles 文件合并进 extra；文件存在时作为该键的事实来源。"""
    env = os.environ if environ is None else environ
    overrides = _load_roles_overrides(roles_file_path(extra, env))
    if not overrides:
        return dict(extra)
    merged = dict(extra)
    for key in ("roles", "super_admins", "admins", "main_agent_read_only"):
        if key in overrides:
            merged[key] = overrides[key]
    return merged


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


def _numeric_id(value: Any, *, name: str, required: bool = False) -> str | None:
    """解析单个 OneBot QQ/群号，拒绝 URL、浮点和任意对象。"""
    if value is None or value == "":
        if required:
            raise ValueError(f"{name} 未配置")
        return None
    values = parse_id_list([value])
    if len(values) != 1:
        raise ValueError(f"{name} 必须是一个 QQ/群号")
    return next(iter(values))


def _host(value: Any, *, name: str) -> str:
    """严格解析 WS 监听地址，拒绝 URL、端口、路径和空白。"""
    if not isinstance(value, str):
        raise ValueError(f"{name} 必须是 hostname 或 IP 字符串")
    if value != value.strip() or not value or any(char.isspace() for char in value):
        raise ValueError(f"{name} 必须是 hostname 或 IP，不能包含空白")
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        pass
    if "/" in value or ":" in value or "://" in value:
        raise ValueError(f"{name} 不能包含 URL、端口或路径")
    if not re.fullmatch(
        r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*",
        value,
    ):
        raise ValueError(f"{name} 不是合法 hostname 或 IP")
    return value


def _is_loopback_host(value: str) -> bool:
    """判断 WS host 是否为本机回环地址。"""
    if value.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def parse_runtime_config(
    extra: Mapping[str, Any],
    environ: Mapping[str, Any] | None = None,
    *,
    require_http_api: bool = False,
) -> RuntimeConfig:
    """解析完整运行时配置；``require_http_api`` 仅用于平台启用校验。"""
    env = os.environ if environ is None else environ
    effective = effective_extra(extra, env)
    # 独立 roles 文件存在时作为 super_admins/roles 的事实来源，优先于
    # config.yaml 和环境变量，避免安全敏感配置被代理工具直接改写。
    effective = apply_roles_overrides(effective, env)
    http_api = _string(effective.get("http_api"), name="http_api")
    if http_api:
        parse_http_base_url(http_api)
    elif require_http_api:
        raise ValueError("ONEBOT11_HTTP_API 未配置")
    self_id = _numeric_id(effective.get("self_id"), name="self_id", required=True)
    assert self_id is not None
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
    ws_host = _host(effective.get("ws_host", "127.0.0.1"), name="ws_host")
    access_token = _string(effective.get("access_token"), name="access_token")
    if not _is_loopback_host(ws_host) and not access_token:
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
    home_channel = _numeric_id(
        effective.get("home_channel"),
        name="home_channel",
    )
    if home_channel is not None and home_channel_type is None:
        raise ValueError("配置 home_channel 时必须同时配置 home_channel_type=group|dm")

    access_policy = build_access_policy(effective, env)
    raw_admins = effective.get("super_admins")
    if raw_admins is None:
        raw_admins = effective.get("admins")
    super_admins = frozenset(parse_admin_list(raw_admins))
    trusted_users = build_trusted_users(effective)
    role_tools = build_role_tools(effective)
    main_agent_read_only = parse_bool(
        effective.get("main_agent_read_only"),
        default=False,
        name="main_agent_read_only",
    )
    trigger_config = build_trigger_config(effective)
    if trigger_config.llm_enabled and not trigger_config.llm_allowed_groups:
        raise ValueError("启用 LLM trigger 时必须配置非空 llm_trigger_groups")
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
    plain_text_enabled = parse_bool(
        effective.get("plain_text_enabled"),
        default=True,
        name="plain_text_enabled",
    )
    long_running_notice_seconds = _float(
        effective.get("long_running_notice_seconds"),
        name="long_running_notice_seconds",
        default=60.0,
        minimum=0.0,
        maximum=86_400.0,
    )
    show_interim_group = parse_bool(
        effective.get("show_interim_group"),
        default=False,
        name="show_interim_group",
    )
    show_interim_dm = parse_bool(
        effective.get("show_interim_dm"),
        default=True,
        name="show_interim_dm",
    )

    media_hosts = frozenset(
        host.casefold().rstrip(".")
        for host in parse_string_list(
            effective.get("media_allowed_hosts"),
            name="media_allowed_hosts",
        )
    )
    media_port_values = parse_id_list(effective.get("media_allowed_ports"))
    media_ports = frozenset(
        _int(port, name="media_allowed_ports", default=0, minimum=1, maximum=65535)
        for port in media_port_values
    )
    media_source_roots: list[str] = []
    for raw_root in parse_string_list(
        effective.get("media_source_roots"),
        name="media_source_roots",
    ):
        root = Path(raw_root).expanduser()
        if not root.is_absolute():
            raise ValueError("media_source_roots 必须使用绝对路径")
        media_source_roots.append(str(root.resolve(strict=False)))
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
        super_admins=super_admins,
        trusted_users=trusted_users,
        role_tools=role_tools,
        main_agent_read_only=main_agent_read_only,
        trigger_config=trigger_config,
        processing_reaction_enabled=reaction_enabled,
        processing_reaction_emoji_id=reaction_emoji,
        plain_text_enabled=plain_text_enabled,
        long_running_notice_seconds=long_running_notice_seconds,
        show_interim_group=show_interim_group,
        show_interim_dm=show_interim_dm,
        media_allowed_hosts=media_hosts,
        media_allowed_ports=media_ports,
        media_source_roots=tuple(dict.fromkeys(media_source_roots)),
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
        home_channel=home_channel,
        home_channel_type=home_channel_type,
    )
