"""OneBot 11 查询和群管理工具定义。

工具参数不包含 group_id/user_id 这样的会话边界字段；执行时由 adapter 注入
不可伪造的 CallerContext，并在 HTTP 返回后再次校验消息作用域。
"""

from __future__ import annotations

from typing import Any

from .http_api import OneBotHttpApi
from .permissions import (
    CallerContext,
    parse_bool,
    validate_group_payload,
    validate_message_scope,
)

TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "qq_get_message": {
        "type": "object",
        "properties": {"message_id": {"type": "string", "description": "要查询的消息 ID"}},
        "required": ["message_id"],
    },
    "qq_get_group_msg_history": {
        "type": "object",
        "properties": {"count": {"type": "integer", "description": "条数，最大 50"}},
    },
    "qq_get_friend_msg_history": {
        "type": "object",
        "properties": {"count": {"type": "integer", "description": "条数，最大 50"}},
    },
    "qq_get_group_info": {"type": "object", "properties": {}},
    "qq_get_group_member_info": {
        "type": "object",
        "properties": {"user_id": {"type": "string", "description": "当前群成员 QQ 号"}},
        "required": ["user_id"],
    },
    "qq_delete_message": {
        "type": "object",
        "properties": {"message_id": {"type": "string", "description": "当前群消息 ID"}},
        "required": ["message_id"],
    },
    "qq_set_group_ban": {
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "description": "当前群成员 QQ 号"},
            "duration": {"type": "integer", "description": "禁言秒数，0 表示解除"},
        },
        "required": ["user_id", "duration"],
    },
    "qq_set_group_kick": {
        "type": "object",
        "properties": {"user_id": {"type": "string", "description": "当前群成员 QQ 号"}, "reject_add_request": {"type": "boolean"}},
        "required": ["user_id"],
    },
    "qq_set_group_whole_ban": {
        "type": "object",
        "properties": {"enable": {"type": "boolean", "description": "是否开启全员禁言"}},
        "required": ["enable"],
    },
    "onebot_get_permissions": {
        "type": "object",
        "properties": {},
    },
    "onebot_set_role_tools": {
        "type": "object",
        "properties": {
            "role": {
                "type": "string",
                "enum": ["user", "trusted_user", "super_admin"],
            },
            "tools": {
                "type": "array",
                "items": {"type": "string"},
                "description": "精确工具名列表，不支持 wildcard 或 toolset 名",
            },
        },
        "required": ["role", "tools"],
    },
    "onebot_set_trusted_users": {
        "type": "object",
        "properties": {
            "users": {
                "type": "array",
                "items": {"type": "string"},
                "description": "trusted_user 的 QQ 号列表",
            },
        },
        "required": ["users"],
    },
}

READ_TOOL_NAMES = frozenset(
    {
        "qq_get_message",
        "qq_get_group_msg_history",
        "qq_get_friend_msg_history",
        "qq_get_group_info",
        "qq_get_group_member_info",
        "onebot_get_permissions",
    }
)
WRITE_TOOL_NAMES = frozenset(set(TOOL_SCHEMAS) - set(READ_TOOL_NAMES))


def _count(params: dict[str, Any]) -> int:
    """规范化查询条数。"""
    try:
        value = int(params.get("count", 20))
    except (TypeError, ValueError) as exc:
        raise ValueError("count 必须是整数") from exc
    return max(1, min(value, 50))


async def handle_get_message(api: OneBotHttpApi, params: dict[str, Any], ctx: CallerContext) -> dict[str, Any]:
    """查询当前群或当前私聊中的单条消息。"""
    message = await api.get_message(str(params["message_id"]))
    error = validate_message_scope(message, ctx)
    if error:
        return {"status": "permission_error", "error": error}
    return {"status": "ok", "message": message}


async def handle_get_group_msg_history(api: OneBotHttpApi, params: dict[str, Any], ctx: CallerContext) -> dict[str, Any]:
    """查询当前群最近消息。"""
    messages = await api.get_group_msg_history(ctx.chat_id, _count(params))
    for message in messages:
        error = validate_message_scope(message, ctx)
        if error:
            return {"status": "permission_error", "group_id": ctx.chat_id, "error": error}
    return {"status": "ok", "group_id": ctx.chat_id, "messages": messages}


async def handle_get_friend_msg_history(api: OneBotHttpApi, params: dict[str, Any], ctx: CallerContext) -> dict[str, Any]:
    """查询当前私聊最近消息。"""
    messages = await api.get_friend_msg_history(ctx.user_id, _count(params))
    for message in messages:
        error = validate_message_scope(message, ctx)
        if error:
            return {"status": "permission_error", "user_id": ctx.user_id, "error": error}
    return {"status": "ok", "user_id": ctx.user_id, "messages": messages}


async def handle_get_group_info(api: OneBotHttpApi, params: dict[str, Any], ctx: CallerContext) -> dict[str, Any]:
    """查询当前群基本信息。"""
    del params
    group = await api.call_action("get_group_info", {"group_id": int(ctx.chat_id)})
    error = validate_group_payload(group, ctx)
    if error:
        return {"status": "permission_error", "error": error}
    return {"status": "ok", "group_id": ctx.chat_id, "group": group}


async def handle_get_group_member_info(api: OneBotHttpApi, params: dict[str, Any], ctx: CallerContext) -> dict[str, Any]:
    """查询当前群指定成员信息。"""
    member = await api.call_action(
        "get_group_member_info",
        {"group_id": int(ctx.chat_id), "user_id": int(str(params["user_id"]))},
    )
    error = validate_group_payload(member, ctx)
    if error:
        return {"status": "permission_error", "error": error}
    return {"status": "ok", "group_id": ctx.chat_id, "member": member}


async def handle_write_action(api: OneBotHttpApi, tool_name: str, params: dict[str, Any], ctx: CallerContext) -> dict[str, Any]:
    """执行已经通过确认的群管理动作；非幂等未知结果原样抛给 adapter。"""
    if ctx.chat_type != "group":
        return {"status": "permission_error", "error": "只能作用于当前群"}
    if tool_name == "qq_delete_message":
        target = await api.get_message(str(params["message_id"]))
        error = validate_message_scope(target, ctx)
        if error:
            return {"status": "permission_error", "error": error}
        data = await api.call_action("delete_msg", {"message_id": int(str(params["message_id"]))}, retryable=False)
    elif tool_name == "qq_set_group_ban":
        data = await api.call_action(
            "set_group_ban",
            {"group_id": int(ctx.chat_id), "user_id": int(str(params["user_id"])), "duration": max(0, int(params["duration"]))},
            retryable=False,
        )
    elif tool_name == "qq_set_group_kick":
        data = await api.call_action(
            "set_group_kick",
            {
                "group_id": int(ctx.chat_id),
                "user_id": int(str(params["user_id"])),
                "reject_add_request": parse_bool(
                    params.get("reject_add_request"), default=False, name="reject_add_request"
                ),
            },
            retryable=False,
        )
    elif tool_name == "qq_set_group_whole_ban":
        data = await api.call_action(
            "set_group_whole_ban",
            {
                "group_id": int(ctx.chat_id),
                "enable": parse_bool(params.get("enable"), name="enable"),
            },
            retryable=False,
        )
    else:
        return {"status": "error", "error": "未知写工具"}
    return {"status": "ok", "tool": tool_name, "data": data}
