"""OneBot 11 平台工具（消息查询）。

handler 签名统一为 (api, params, ctx) -> dict,零 Hermes 依赖。
由根目录 adapter.py 包装成 ctx.register_tool 的 handler,并注入
ToolContext（user_id / chat_type / chat_id）。

安全设计：群号 / QQ 号一律从会话上下文注入,LLM 传的同类参数被忽略——
工具只能作用于发起会话自身。
"""

from __future__ import annotations

from .http_api import OneBotHttpApi
from .permissions import ToolContext

# 工具 schema（JSON Schema,供 LLM 调用;不含群号/QQ 号——由会话注入）
TOOL_SCHEMAS: dict[str, dict] = {
    "qq_get_message": {
        "type": "object",
        "properties": {
            "message_id": {"type": "string", "description": "要查询的消息 ID"},
        },
        "required": ["message_id"],
    },
    "qq_get_group_msg_history": {
        "type": "object",
        "properties": {
            "count": {"type": "integer", "description": "拉取条数,默认 20,最大 50"},
        },
        "required": [],
    },
    "qq_get_friend_msg_history": {
        "type": "object",
        "properties": {
            "count": {"type": "integer", "description": "拉取条数,默认 20,最大 50"},
        },
        "required": [],
    },
}


async def handle_get_message(api: OneBotHttpApi, params: dict, ctx: ToolContext) -> dict:
    """按 message_id 查单条消息。"""
    message = await api.get_message(str(params["message_id"]))
    return {"message": message}


async def handle_get_group_msg_history(
    api: OneBotHttpApi, params: dict, ctx: ToolContext
) -> dict:
    """查当前群最近消息（群号取自会话）。"""
    count = min(int(params.get("count", 20)), 50)
    messages = await api.get_group_msg_history(ctx.chat_id, count=count)
    return {"group_id": ctx.chat_id, "messages": messages}


async def handle_get_friend_msg_history(
    api: OneBotHttpApi, params: dict, ctx: ToolContext
) -> dict:
    """查自己与机器人的私聊最近消息（QQ 取自会话）。"""
    count = min(int(params.get("count", 20)), 50)
    messages = await api.get_friend_msg_history(ctx.user_id, count=count)
    return {"user_id": ctx.user_id, "messages": messages}
