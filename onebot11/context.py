"""队列 → 上下文文本的拼接与压缩。

零 Hermes 依赖,可独立测试。规则:
- 最近 keep_raw 条保留原文(仍受单条长度限制)
- 更早的消息:优先经 summarizer 回调压缩成一句摘要;无回调则丢弃
- 总长超过 max_chars 截断,防巨型上下文占满预算
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from .queue import GroupMessageQueue

# summarizer 可以是同步函数或协程,统一 await
Summarizer = Callable[[str], Awaitable[str] | str]


async def build_group_context(
    queue: GroupMessageQueue,
    chat_id: str,
    *,
    keep_raw: int = 5,
    max_chars: int = 1500,
    summarizer: Summarizer | None = None,
) -> str:
    """构建"自上次触发以来的群聊摘要";队列为空返回空串。"""
    msgs = queue.snapshot(chat_id)
    if not msgs:
        return ""
    parts: list[str] = []
    older, raw = msgs[:-keep_raw], msgs[-keep_raw:]
    if older and summarizer is not None:
        blob = " | ".join(f"[{m.user_name}] {m.text}" for m in older)
        summary = summarizer(blob)
        if isinstance(summary, Awaitable):
            summary = await summary
        if summary:
            parts.append(f"[此前摘要] {summary}")
    for m in raw:
        parts.append(f"[{m.user_name}] {m.text}")
    joined = "\n".join(parts)
    if len(joined) > max_chars:
        joined = joined[:max_chars] + "…"
    return joined
