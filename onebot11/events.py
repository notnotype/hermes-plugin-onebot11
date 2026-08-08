"""OneBot 11 事件归一化，零 Hermes 依赖。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .message import ParsedMessage, parse_message_segments


@dataclass
class InboundEvent:
    """可进入消息队列的 OneBot message 事件。"""

    text: str
    chat_id: str
    chat_type: str
    user_id: str
    user_name: str
    message_id: str
    message_key: str = ""
    images: list[str] = field(default_factory=list)
    image_urls: list[str] = field(default_factory=list)
    image_files: list[str] = field(default_factory=list)
    reply_to_message_id: str | None = None
    mentioned_self: bool = False
    markers: list[str] = field(default_factory=list)
    segments: list[dict[str, Any]] = field(default_factory=list)
    raw_metadata: dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""


@dataclass(frozen=True)
class AuxiliaryEvent:
    """不进入 Agent session 的 notice/request/lifecycle 事件摘要。"""

    post_type: str
    event_type: str
    chat_id: str | None
    user_id: str | None
    summary: str


def _bounded_raw_metadata(raw: Mapping[str, Any], limit: int = 4096) -> dict[str, Any]:
    """保存有限、可序列化的调试元数据，不保留完整 raw payload。"""
    selected = {
        key: raw.get(key)
        for key in (
            "post_type",
            "message_type",
            "message_id",
            "group_id",
            "user_id",
            "notice_type",
            "request_type",
            "sub_type",
            "time",
        )
        if key in raw
    }
    try:
        encoded = json.dumps(selected, ensure_ascii=False, default=str, separators=(",", ":"))
        if len(encoded.encode("utf-8")) <= limit:
            return json.loads(encoded)
        bounded: dict[str, Any] = {"truncated": True}
        for key, value in selected.items():
            value_text = str(value)
            remaining = max(0, limit - len(json.dumps(bounded, ensure_ascii=False).encode("utf-8")) - 32)
            bounded[key] = value if len(value_text.encode("utf-8")) <= remaining else value_text.encode("utf-8")[:remaining].decode("utf-8", errors="ignore")
        return bounded
    except (TypeError, ValueError, json.JSONDecodeError):
        return {"post_type": str(raw.get("post_type") or "unknown")}


def _normalized_event_for_hash(raw: Mapping[str, Any]) -> dict[str, Any]:
    """提取不含时间戳的消息字段，生成可重放的稳定去重输入。"""
    fields = (
        "post_type",
        "message_type",
        "sub_type",
        "group_id",
        "user_id",
        "message",
        "raw_message",
    )
    return {key: raw[key] for key in fields if key in raw}


def normalize_auxiliary_event(raw: Mapping[str, Any], limit: int = 512) -> AuxiliaryEvent | None:
    """归一化 notice/request/lifecycle，供日志和统计而非 Agent 使用。"""
    post_type = str(raw.get("post_type") or "")
    if post_type not in {"notice", "request", "meta_event", "lifecycle"}:
        return None
    event_type = str(raw.get(f"{post_type}_type") or raw.get("sub_type") or "unknown")
    chat_id = raw.get("group_id") or raw.get("user_id")
    user_id = raw.get("user_id")
    summary = json.dumps(_bounded_raw_metadata(raw, limit), ensure_ascii=False, default=str)
    bounded_summary = summary.encode("utf-8")[:limit].decode("utf-8", errors="ignore")
    return AuxiliaryEvent(
        post_type,
        event_type,
        str(chat_id) if chat_id is not None else None,
        str(user_id) if user_id is not None else None,
        bounded_summary,
    )


def build_inbound_event(raw: Mapping[str, Any], self_id: str | None) -> InboundEvent | None:
    """把 OneBot message 事件转换为队列可用的内部事件。"""
    if str(raw.get("post_type") or "") != "message":
        return None
    message_type = raw.get("message_type")
    message_id = str(raw.get("message_id") or "")
    message_key = message_id
    if not message_key:
        canonical = _normalized_event_for_hash(raw)
        encoded = json.dumps(
            canonical,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")
        message_key = "hash:" + hashlib.sha256(encoded).hexdigest()
    user_id = str(raw.get("user_id") or "")
    if self_id and user_id == str(self_id):
        return None
    sender = raw.get("sender") or {}
    if not isinstance(sender, dict):
        sender = {}
    message_segments = raw.get("message")
    if message_segments is None:
        return None
    parsed: ParsedMessage = parse_message_segments(message_segments, self_id=self_id)
    if message_type == "group":
        chat_id = str(raw.get("group_id") or "")
        chat_type = "group"
        user_name = str(sender.get("card") or sender.get("nickname") or user_id)
    elif message_type == "private":
        chat_id = user_id
        chat_type = "dm"
        user_name = str(sender.get("nickname") or user_id)
    else:
        return None
    if not user_id or not chat_id:
        return None
    return InboundEvent(
        text=parsed.text,
        chat_id=chat_id,
        chat_type=chat_type,
        user_id=user_id,
        user_name=user_name,
        message_id=message_id,
        message_key=message_key,
        images=parsed.images,
        image_urls=parsed.image_urls,
        image_files=parsed.image_files,
        reply_to_message_id=parsed.reply_to_message_id,
        mentioned_self=parsed.mentioned_self,
        markers=parsed.markers,
        segments=parsed.segments,
        raw_metadata=_bounded_raw_metadata(raw),
        raw_text=(
            str(raw.get("raw_message"))
            if isinstance(raw.get("raw_message"), str)
            else str(raw.get("message"))
            if isinstance(raw.get("message"), str)
            else parsed.text
        ),
    )
