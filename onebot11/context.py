"""OneBot11 群 turn 的确定性上下文拼接。"""

from __future__ import annotations

from collections.abc import Iterable

from .queue import QueueMessage


def _truncate_utf8(value: str, limit: int) -> str:
    """按 UTF-8 字节保留前缀，避免截断半个字符。"""
    if limit <= 0:
        return ""
    return str(value).encode("utf-8", errors="replace")[:limit].decode(
        "utf-8", errors="ignore"
    )


def _truncate_utf8_tail(value: str, limit: int) -> str:
    """按 UTF-8 字节保留末尾，优先保留摘要中的新内容。"""
    if limit <= 0:
        return ""
    return str(value).encode("utf-8", errors="replace")[-limit:].decode(
        "utf-8", errors="ignore"
    )


def _message_line(message: QueueMessage, *, include_original: bool) -> str:
    """生成一条受限队列消息上下文。"""
    markers = " ".join(
        str(item)[:128] for item in (message.metadata.get("onebot11_markers") or [])
    )
    line = f"#{message.seq or '?'} [{message.user_name}] {message.text}"
    if include_original and message.raw_text and message.raw_text != message.text:
        line += f" [原文: {message.raw_text}]"
    if markers:
        line += f" {markers}"
    return line


def build_agent_context(
    summary: str,
    messages: Iterable[QueueMessage],
    max_bytes: int = 64 * 1024,
    recent_originals: int = 3,
) -> str:
    """按字节预算拼接历史摘要、最新消息和省略计数。"""
    budget = max(512, int(max_bytes))
    items = tuple(messages)
    lines = ["[OneBot11 群消息上下文]"]
    if summary:
        summary_budget = min(
            len(str(summary).encode("utf-8")), max(0, budget // 2)
        )
        summary_text = _truncate_utf8_tail(str(summary), summary_budget)
        lines.extend(["历史摘要：", summary_text])
    lines.append("本次待处理消息：")

    selected: list[str] = []
    remaining = max(0, budget - len("\n".join(lines).encode("utf-8")) - 1)
    for index, message in reversed(list(enumerate(items))):
        line = _message_line(
            message,
            include_original=index >= max(0, len(items) - max(0, int(recent_originals))),
        )
        line_bytes = len(line.encode("utf-8")) + (1 if selected else 0)
        if line_bytes > remaining and not selected:
            line = _truncate_utf8(line, max(1, remaining))
            line_bytes = len(line.encode("utf-8"))
        if line_bytes > remaining:
            break
        selected.append(line)
        remaining -= line_bytes

    omitted = len(items) - len(selected)
    if omitted:
        marker = f"[省略 {omitted} 条较早队列消息]"
        marker_bytes = len(marker.encode("utf-8")) + 1
        while selected and marker_bytes > remaining:
            remaining += len(selected.pop(0).encode("utf-8")) + (1 if selected else 0)
        if marker_bytes <= remaining:
            selected.append(marker)
            remaining -= marker_bytes
    lines.extend(reversed(selected))
    result = "\n".join(lines)
    if len(result.encode("utf-8")) <= budget:
        return result
    return _truncate_utf8_tail(result, budget)
