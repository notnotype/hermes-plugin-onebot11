"""群聊消息队列:监听所有消息,按群分桶,触发时取出摘要。

零 Hermes 依赖,可独立测试。内存队列,网关重启即清空(可后续落 DB)。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class QueuedMessage:
    """队列中的一条消息。"""

    text: str
    user_id: str
    user_name: str
    ts: float


class GroupMessageQueue:
    """每个群的环形消息队列。

    - max_entries: 每群最多保留多少条(超出丢最旧)
    - max_chars_per_entry: 单条消息超过此长度截断(防巨型消息占满预算)
    """

    def __init__(self, max_entries: int = 100, max_chars_per_entry: int = 2000) -> None:
        self.max_entries = max_entries
        self.max_chars_per_entry = max_chars_per_entry
        self._buckets: dict[str, deque[QueuedMessage]] = {}

    def push(self, chat_id: str, text: str, user_id: str, user_name: str, ts: float) -> None:
        """入队;超长消息按 max_chars_per_entry 截断。"""
        if len(text) > self.max_chars_per_entry:
            text = text[: self.max_chars_per_entry] + "…"
        bucket = self._buckets.setdefault(chat_id, deque(maxlen=self.max_entries))
        bucket.append(QueuedMessage(text=text, user_id=user_id, user_name=user_name, ts=ts))

    def snapshot(self, chat_id: str) -> list[QueuedMessage]:
        """返回该群队列快照(旧→新)。"""
        return list(self._buckets.get(chat_id, deque()))

    def clear(self, chat_id: str) -> None:
        """清空该群队列(触发后消费)。"""
        self._buckets.pop(chat_id, None)
