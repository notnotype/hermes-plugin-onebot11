"""OneBot 11 事件 → 内部归一化事件。

本模块零 Hermes 依赖：返回自定义 InboundEvent,由根目录 adapter.py 再转换为
Hermes 的 MessageEvent / SessionSource。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from onebot11.message import ParsedMessage, parse_message_segments


@dataclass
class InboundEvent:
    """OneBot 11 消息事件归一化结果。

    - chat_id: 群聊为 group_id,私聊为 user_id（字符串）
    - chat_type: "group" | "dm"
    - user_name: 群昵称(card)优先,其次 nickname;私聊为 nickname
    - images: 图片 file/url 列表
    - reply_to_message_id: 引用的消息 id（reply 段）,无则为 None
    - mentioned_self: 是否 @ 了机器人自己
    """

    text: str
    chat_id: str
    chat_type: str
    user_id: str
    user_name: str
    message_id: str
    images: list[str] = field(default_factory=list)
    reply_to_message_id: str | None = None
    mentioned_self: bool = False


def build_inbound_event(raw: dict, self_id: str | None) -> InboundEvent | None:
    """把 OneBot 11 上报事件转换为 InboundEvent。

    只处理 post_type=message;meta_event(heartbeat)/notice/request 返回 None
    （v1 不进会话）。缺少 message 字段或 sender 缺失时返回 None。
    """
    if raw.get("post_type") != "message":
        return None

    message_type = raw.get("message_type")
    message_id = str(raw.get("message_id", ""))
    user_id = str(raw.get("user_id", ""))
    sender = raw.get("sender") or {}
    parsed: ParsedMessage = parse_message_segments(raw.get("message") or [], self_id=self_id)

    if message_type == "group":
        chat_id = str(raw.get("group_id", ""))
        chat_type = "group"
        user_name = sender.get("card") or sender.get("nickname") or user_id
    elif message_type == "private":
        chat_id = user_id
        chat_type = "dm"
        user_name = sender.get("nickname") or user_id
    else:
        return None

    if not user_id or not chat_id:
        return None

    message_segments = raw.get("message")
    if message_segments is None:
        # 缺少 message 字段的畸形事件直接丢弃
        return None

    reply_to_id: str | None = None
    for seg in message_segments or []:
        if seg.get("type") == "reply":
            reply_to_id = str(seg.get("data", {}).get("id", "")) or None
            break

    return InboundEvent(
        text=parsed.text,
        chat_id=chat_id,
        chat_type=chat_type,
        user_id=user_id,
        user_name=user_name,
        message_id=message_id,
        images=parsed.images,
        reply_to_message_id=reply_to_id,
        mentioned_self=parsed.mentioned_self,
    )
