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
            len(str(summary).encode("utf-8")), max(0, budget // 3)
        )
        summary_text = _truncate_utf8_tail(str(summary), summary_budget)
        lines.extend(["历史摘要：", summary_text])
    lines.append("本次待处理消息：")

    selected_newest_first: list[str] = []
    remaining = max(0, budget - len("\n".join(lines).encode("utf-8")) - 1)
    for index, message in reversed(list(enumerate(items))):
        line = _message_line(
            message,
            include_original=index >= max(0, len(items) - max(0, int(recent_originals))),
        )
        line_bytes = len(line.encode("utf-8")) + (1 if selected_newest_first else 0)
        if line_bytes > remaining and not selected_newest_first:
            # 最新消息即使很长也保留其尾部；不能因为预算不足而丢掉整批最新输入。
            line = _truncate_utf8(line, max(1, remaining))
            line_bytes = len(line.encode("utf-8"))
        if line_bytes > remaining:
            break
        selected_newest_first.append(line)
        remaining -= line_bytes

    omitted = len(items) - len(selected_newest_first)
    if omitted:
        marker = f"[省略 {omitted} 条较早队列消息]"
        marker_bytes = len(marker.encode("utf-8")) + 1
        while len(selected_newest_first) > 1 and marker_bytes > remaining:
            # selected 是“最新到最早”，只能从尾部丢弃旧消息，保留最新消息。
            removed = selected_newest_first.pop()
            remaining += len(removed.encode("utf-8")) + 1
        if marker_bytes <= remaining:
            selected_newest_first.append(marker)
            remaining -= marker_bytes
    lines.extend(reversed(selected_newest_first))
    result = "\n".join(lines)
    if len(result.encode("utf-8")) <= budget:
        return result
    return _truncate_utf8_tail(result, budget)
