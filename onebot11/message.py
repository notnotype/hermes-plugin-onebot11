"""OneBot 11 消息段解析（array 和 CQ 字符串）。

解析结果保留可展示的媒体/reply/未知段标记；原始 payload 由 adapter 另外限长保存，
不把任意 OneBot JSON 直接拼进 Hermes prompt。
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Any

_CQ_RE = re.compile(r"\[CQ:([a-zA-Z0-9_]+)((?:,[^\]]*)?)\]")


@dataclass
class ParsedMessage:
    """从消息段中提取正文、媒体和作用域相关标记。"""

    text: str = ""
    images: list[str] = field(default_factory=list)
    image_urls: list[str] = field(default_factory=list)
    image_files: list[str] = field(default_factory=list)
    mentioned_qq: list[str] = field(default_factory=list)
    mentioned_self: bool = False
    reply_to_message_id: str | None = None
    markers: list[str] = field(default_factory=list)
    segments: list[dict[str, Any]] = field(default_factory=list)


def _unescape_cq(value: str) -> str:
    """还原 CQ 字符串中的转义字符。"""
    return html.unescape(value.replace("&#44;", ",").replace("&#91;", "[").replace("&#93;", "]"))


def parse_cq_string(value: str, self_id: str | None = None) -> list[dict[str, Any]]:
    """把 CQ 字符串转换为与 array 相同的消息段列表。"""
    segments: list[dict[str, Any]] = []
    cursor = 0
    for match in _CQ_RE.finditer(value):
        if match.start() > cursor:
            segments.append({"type": "text", "data": {"text": _unescape_cq(value[cursor:match.start()])}})
        params: dict[str, str] = {}
        raw_params = match.group(2).lstrip(",")
        for item in raw_params.split(",") if raw_params else []:
            if "=" in item:
                key, raw = item.split("=", 1)
                params[key] = _unescape_cq(raw)
        segments.append({"type": match.group(1), "data": params})
        cursor = match.end()
    if cursor < len(value):
        segments.append({"type": "text", "data": {"text": _unescape_cq(value[cursor:])}})
    if not segments and value:
        segments.append({"type": "text", "data": {"text": value}})
    return segments


def parse_message_segments(segments: list[dict[str, Any]] | str, self_id: str | None = None) -> ParsedMessage:
    """解析 OneBot array/CQ 消息，未知段也保留有限标记。"""
    if isinstance(segments, str):
        segments = parse_cq_string(segments, self_id=self_id)
    if not isinstance(segments, list):
        raise ValueError("OneBot message 必须是 array 或 CQ 字符串")

    result = ParsedMessage()
    for raw_segment in segments:
        if not isinstance(raw_segment, dict):
            result.markers.append("[onebot:invalid-segment]")
            continue
        seg_type = str(raw_segment.get("type") or "unknown")
        data = raw_segment.get("data") or {}
        if not isinstance(data, dict):
            data = {}
        result.segments.append(
            {
                "type": seg_type,
                "data": {
                    str(k): str(v)[:512]
                    for k, v in data.items()
                    if k in {"file", "url", "id", "name", "text", "qq", "time"}
                },
            }
        )
        if seg_type == "text":
            result.text += str(data.get("text") or "")
        elif seg_type == "at":
            qq = str(data.get("qq") or "")
            if qq == "all":
                result.markers.append("[@all]")
            elif self_id is not None and qq == str(self_id):
                result.mentioned_self = True
            elif qq:
                result.mentioned_qq.append(qq)
        elif seg_type == "image":
            url = str(data.get("url") or "").strip()
            file_id = str(data.get("file") or "").strip()
            if url:
                result.images.append(url)
                result.image_urls.append(url)
            elif file_id:
                result.images.append(file_id)
            if file_id:
                result.image_files.append(file_id)
            if url or file_id:
                result.markers.append("[image]")
        elif seg_type == "reply":
            reply_id = str(data.get("id") or "")
            if reply_id:
                result.reply_to_message_id = reply_id
                result.markers.append(f"[reply:{reply_id[:128]}]")
        elif seg_type in {"file", "record", "video", "forward"}:
            label = str(data.get("name") or data.get("file") or data.get("id") or "")[:128]
            result.markers.append(f"[{seg_type}:{label}]" if label else f"[{seg_type}]")
        elif seg_type != "unknown":
            result.markers.append(f"[{seg_type}]")
        else:
            result.markers.append("[onebot:unknown]")
    return result
