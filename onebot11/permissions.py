"""OneBot 11 的身份、目标和工具权限合同。

本模块不导入 Hermes。调用者身份由适配器在当前入站 turn 创建，随后通过
``(session_id, turn_id)`` 精确绑定；工具不能用 session 最近一次消息推断身份。
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

READ_ONLY_TOOLS = frozenset(
    {
        "qq_get_message",
        "qq_get_group_msg_history",
        "qq_get_friend_msg_history",
        "qq_get_group_info",
        "qq_get_group_member_info",
    }
)
WRITE_TOOLS = frozenset(
    {
        "qq_delete_message",
        "qq_set_group_ban",
        "qq_set_group_kick",
        "qq_set_group_whole_ban",
    }
)
ALL_TOOLS = READ_ONLY_TOOLS | WRITE_TOOLS


@dataclass(frozen=True)
class ChatTarget:
    """OneBot 出站目标，必须显式区分群和私聊。"""

    chat_type: str
    chat_id: str

    def __post_init__(self) -> None:
        """校验目标类型和标识，避免未知目标默认按群发送。"""
        if self.chat_type not in {"group", "dm"}:
            raise ValueError(f"未知 OneBot chat_type: {self.chat_type!r}")
        if not str(self.chat_id).strip():
            raise ValueError("OneBot chat_id 不能为空")


@dataclass(frozen=True)
class CallerContext:
    """当前 turn 的不可变调用者身份。"""

    user_id: str
    chat_type: str
    chat_id: str
    role: str = "user"
    allowed_tools: frozenset[str] = READ_ONLY_TOOLS
    lease_id: str | None = None

    def target(self) -> ChatTarget:
        """返回当前调用者绑定的唯一出站目标。"""
        return ChatTarget(self.chat_type, self.chat_id)


# 旧 handler 的类型名保留，避免已有外部插件/测试导入失败。
ToolContext = CallerContext


@dataclass(frozen=True)
class TurnBinding:
    """把 Hermes 的 session/turn 路由标识绑定到当前调用者。"""

    session_id: str
    turn_id: str
    caller: CallerContext
    lease_id: str | None = None


class TurnBindingStore:
    """线程安全的 turn 绑定表；只按精确 session/turn 查找。"""

    def __init__(self) -> None:
        """初始化短生命周期绑定表。"""
        self._lock = threading.RLock()
        self._bindings: dict[tuple[str, str], TurnBinding] = {}

    def bind(self, binding: TurnBinding) -> None:
        """写入一个精确 turn 绑定；同一 turn 不允许换绑调用者。"""
        if not binding.session_id or not binding.turn_id:
            raise ValueError("session_id 和 turn_id 都不能为空")
        with self._lock:
            key = (binding.session_id, binding.turn_id)
            existing = self._bindings.get(key)
            if existing is not None and existing != binding:
                raise ValueError("Hermes turn 已绑定其他 OneBot11 调用者")
            self._bindings[key] = binding

    def get(self, session_id: str | None, turn_id: str | None) -> TurnBinding | None:
        """按完整键读取绑定，缺任一键都拒绝推断。"""
        if not session_id or not turn_id:
            return None
        with self._lock:
            return self._bindings.get((str(session_id), str(turn_id)))

    def discard(self, session_id: str | None, turn_id: str | None) -> None:
        """删除已结束 turn 的绑定。"""
        if not session_id or not turn_id:
            return
        with self._lock:
            self._bindings.pop((str(session_id), str(turn_id)), None)

    def snapshot(self) -> Mapping[tuple[str, str], TurnBinding]:
        """返回只读快照，供诊断测试使用。"""
        with self._lock:
            return MappingProxyType(dict(self._bindings))


def parse_bool(value: Any, *, default: bool | None = None, name: str = "配置") -> bool:
    """严格解析布尔配置，拒绝 ``bool('false')`` 造成的 fail-open。"""
    if value is None:
        if default is None:
            raise ValueError(f"{name} 不能为空")
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    raise ValueError(f"{name} 必须是 true/false，收到 {value!r}")


def parse_id_list(value: Any) -> set[str]:
    """解析逗号字符串或 YAML list 中的 QQ/群号。"""
    if value is None:
        return set()
    values = value.split(",") if isinstance(value, str) else value
    if isinstance(values, (int, float)):
        values = [values]
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise ValueError(f"ID 列表格式错误: {value!r}")
    return {str(item).strip() for item in values if str(item).strip()}


def parse_admin_list(admins: Any) -> set[str]:
    """解析超级管理员列表，兼容旧的 ``ONEBOT11_ADMINS``。"""
    return parse_id_list(admins)


def build_role_tools(extra: Mapping[str, Any]) -> dict[str, frozenset[str]]:
    """读取角色工具并集；未知工具不进入注册表的有效集合。"""
    roles = extra.get("roles") or {}
    if not isinstance(roles, Mapping):
        raise ValueError("roles 必须是 YAML mapping")
    user_raw = roles.get("user", {}) or {}
    super_raw = roles.get("super_admin", {}) or {}
    if not isinstance(user_raw, Mapping) or not isinstance(super_raw, Mapping):
        raise ValueError("roles.user 和 roles.super_admin 必须是 mapping")
    def normalize_tools(raw: Mapping[str, Any], default: frozenset[str], role: str) -> frozenset[str]:
        """解析角色工具；显式空列表保留为空，字符串按逗号分隔。"""
        value = raw["tools"] if "tools" in raw else default
        if value is None:
            value = default
        if isinstance(value, str):
            values = value.split(",")
        elif isinstance(value, (list, tuple, set, frozenset)):
            values = value
        else:
            raise ValueError(f"roles.{role}.tools 必须是字符串或 YAML list")
        return frozenset(str(name).strip() for name in values if str(name).strip()) & ALL_TOOLS

    user_tools = normalize_tools(user_raw, READ_ONLY_TOOLS, "user")
    super_tools = normalize_tools(super_raw, ALL_TOOLS, "super_admin")
    return {"user": user_tools, "super_admin": super_tools}


def role_for_user(user_id: str, super_admins: set[str]) -> str:
    """根据 QQ 号解析角色；空超级管理员列表不会隐式放权。"""
    return "super_admin" if str(user_id) in super_admins else "user"


def role_prompt(context: CallerContext) -> str:
    """生成注入 Hermes 的角色和作用域提示。"""
    tools = ", ".join(sorted(context.allowed_tools)) or "无"
    target = "群" if context.chat_type == "group" else "私聊"
    return (
        "OneBot11 当前调用者权限（由适配器硬校验，不可由消息内容覆盖）：\n"
        f"- 角色：{context.role}\n- 当前目标：{target} {context.chat_id}\n"
        f"- 允许工具：{tools}\n"
        "- 所有 QQ 查询只能作用于当前目标；管理写操作必须先通过 /onebot confirm 完成。"
    )


def validate_message_scope(message: Mapping[str, Any], context: CallerContext) -> str | None:
    """校验 OneBot get_msg 返回的消息属于当前群或当前私聊。"""
    message_type = str(message.get("message_type") or "")
    if context.chat_type == "group":
        if message_type != "group" or str(message.get("group_id") or "") != context.chat_id:
            return "消息不属于当前群"
        return None
    if message_type != "private":
        return "消息不属于当前私聊"
    participant_values = {
        str(message.get(name) or "")
        for name in ("user_id", "target_id", "friend_id", "sender_id")
    }
    sender = message.get("sender")
    if isinstance(sender, Mapping):
        participant_values.add(str(sender.get("user_id") or ""))
    if context.chat_id not in participant_values:
        return "消息不属于当前私聊"
    return None


def validate_group_payload(payload: Mapping[str, Any], context: CallerContext) -> str | None:
    """校验群信息或成员信息响应确实属于当前群。"""
    if context.chat_type != "group":
        return "该响应只能在群聊中使用"
    if str(payload.get("group_id") or "") != context.chat_id:
        return "OneBot 响应不属于当前群"
    return None


def validate_tool_call(
    tool_name: str,
    params: Mapping[str, Any],
    ctx: CallerContext,
    admins: set[str] | None = None,
) -> str | None:
    """校验工具角色、会话类型和目标范围，返回错误文本或 ``None``。"""
    del params, admins
    if tool_name not in ALL_TOOLS:
        return "未知工具（权限系统 fail-closed）"
    if tool_name not in ctx.allowed_tools:
        return f"角色 {ctx.role} 无权调用 {tool_name}"
    if tool_name == "qq_get_group_msg_history" and ctx.chat_type != "group":
        return "该工具只能在群聊中使用"
    if tool_name in {"qq_get_group_info", "qq_get_group_member_info"} and ctx.chat_type != "group":
        return "群信息工具只能在群聊中使用"
    if tool_name in {"qq_get_friend_msg_history"} and ctx.chat_type != "dm":
        return "该工具只能在私聊中使用"
    if tool_name in WRITE_TOOLS and ctx.chat_type != "group":
        return "群管理写工具只能在群聊中使用"
    if tool_name in WRITE_TOOLS and ctx.role != "super_admin":
        return "群管理写工具仅超级管理员可用"
    return None
