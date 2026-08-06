"""OneBot11 当前队列 batch 的确定性上下文构造。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from .queue import QueueMessage


def _truncate_utf8(value: str, limit: int) -> str:
    """按 UTF-8 字节保留前缀，避免截断半个字符。"""
    if limit <= 0:
        return ""
    return str(value).encode("utf-8", errors="replace")[:limit].decode(
        "utf-8", errors="ignore"
    )


def _truncate_utf8_tail(value: str, limit: int) -> str:
    """按 UTF-8 字节保留末尾，优先保留较新的摘要内容。"""
    if limit <= 0:
        return ""
    return str(value).encode("utf-8", errors="replace")[-limit:].decode(
        "utf-8", errors="ignore"
    )


def _message_line(message: QueueMessage, *, include_original: bool) -> str:
    """生成一条受限的队列消息上下文。"""
    markers = " ".join(
        str(item)[:128] for item in (message.metadata.get("onebot11_markers") or [])
    )
    line = f"#{message.seq or '?'} [{message.user_name}] {message.text}"
    if include_original and message.raw_text and message.raw_text != message.text:
        line += f" [原文: {message.raw_text}]"
    if markers:
        line += f" {markers}"
    return line


def build_queue_batch_summary(
    messages: Iterable[QueueMessage],
    max_bytes: int = 32 * 1024,
) -> str:
    """把当前 batch 的早期消息压成确定性摘要，不读取跨轮历史。"""
    budget = max(256, int(max_bytes))
    lines = [_message_line(message, include_original=False) for message in messages]
    if not lines:
        return ""
    result = "\n".join(lines)
    if len(result.encode("utf-8")) <= budget:
        return result
    marker = "[本次队列摘要较大，较早消息已裁剪]"
    marker_bytes = len(marker.encode("utf-8")) + 1
    tail = _truncate_utf8_tail(result, max(0, budget - marker_bytes))
    return f"{marker}\n{tail}" if tail else marker[:budget]


def build_agent_context(
    summary: str,
    messages: Iterable[QueueMessage],
    max_bytes: int = 64 * 1024,
    recent_originals: int = 3,
) -> str:
    """构造当前 user turn，避免把已写入 session 的旧 batch 再注入。"""
    budget = max(512, int(max_bytes))
    items = tuple(messages)
    recent_count = max(0, min(len(items), int(recent_originals)))
    older_items = items[:-recent_count] if recent_count else items
    batch_summary = str(summary or "")
    if not batch_summary and older_items:
        batch_summary = build_queue_batch_summary(older_items, max_bytes=budget // 2)

    header = "[OneBot11 当前群消息 batch]"
    sections = [header]
    if batch_summary:
        summary_budget = min(
            len(batch_summary.encode("utf-8")),
            max(0, budget // 2),
        )
        sections.extend(
            [
                "队列摘要（本次 batch 的较早消息，已物化进本轮历史）：",
                _truncate_utf8_tail(batch_summary, summary_budget),
            ]
        )
    if recent_count:
        sections.append("最近消息（本次 batch 的原文，将物化进本轮历史）：")
    else:
        sections.append("本次 batch 没有保留最近原文。")

    prefix = "\n".join(sections)
    remaining = max(0, budget - len(prefix.encode("utf-8")) - 1)
    selected: list[str] = []
    recent_items = items[-recent_count:] if recent_count else ()
    for message in reversed(recent_items):
        line = _message_line(message, include_original=True)
        line_bytes = len(line.encode("utf-8")) + (1 if selected else 0)
        if line_bytes > remaining and not selected:
            line = _truncate_utf8(line, max(1, remaining))
            line_bytes = len(line.encode("utf-8"))
        if line_bytes > remaining:
            break
        selected.append(line)
        remaining -= line_bytes
    if len(selected) < len(recent_items):
        omitted = len(recent_items) - len(selected)
        marker = f"[最近消息另有 {omitted} 条因预算省略]"
        marker_bytes = len(marker.encode("utf-8")) + 1
        if marker_bytes <= remaining:
            selected.append(marker)
            remaining -= marker_bytes
    sections.extend(reversed(selected))
    result = "\n".join(sections)
    if len(result.encode("utf-8")) <= budget:
        return result
    return _truncate_utf8_tail(result, budget)


def build_dynamic_context(
    fields: Mapping[str, object],
    max_bytes: int = 8 * 1024,
) -> str:
    """构造 request-only 动态上下文；调用方不得把它写入 Hermes transcript。"""
    budget = max(256, int(max_bytes))
    lines = ["[OneBot11 动态上下文：仅当前 provider request，不写入会话历史]"]
    for key, value in fields.items():
        name = str(key).strip()
        if not name:
            continue
        lines.append(f"{name}: {value}")
    return _truncate_utf8("\n".join(lines), budget)
