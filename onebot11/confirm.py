"""群管理写操作的短期、单次消费确认令牌。"""

from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Confirmation:
    """待确认动作；令牌本身不进入审计日志。"""

    token: str
    tool_name: str
    params: Mapping[str, Any]
    user_id: str
    chat_type: str
    chat_id: str
    expires_at: float


class ConfirmationStore:
    """进程内令牌存储，重启后全部失效是安全默认值。"""

    def __init__(self, ttl_seconds: float = 60.0) -> None:
        """初始化带 TTL 的令牌表。"""
        self.ttl_seconds = max(1.0, float(ttl_seconds))
        self._lock = threading.RLock()
        self._items: dict[str, Confirmation] = {}

    def issue(
        self,
        tool_name: str,
        params: Mapping[str, Any],
        *,
        user_id: str,
        chat_type: str,
        chat_id: str,
    ) -> Confirmation:
        """创建一次性确认令牌。"""
        now = time.time()
        confirmation = Confirmation(
            token=secrets.token_urlsafe(18),
            tool_name=str(tool_name),
            params=dict(params),
            user_id=str(user_id),
            chat_type=str(chat_type),
            chat_id=str(chat_id),
            expires_at=now + self.ttl_seconds,
        )
        with self._lock:
            self._purge(now)
            self._items[confirmation.token] = confirmation
        return confirmation

    def consume(self, token: str, *, user_id: str, chat_type: str, chat_id: str) -> Confirmation | None:
        """验证同一管理员/同一目标并单次消费令牌。"""
        with self._lock:
            now = time.time()
            token_key = str(token).strip()
            item = self._items.get(token_key)
            if item is None or item.expires_at <= now:
                self._items.pop(token_key, None)
                return None
            if item.user_id != str(user_id) or item.chat_type != str(chat_type) or item.chat_id != str(chat_id):
                return None
            self._items.pop(token_key, None)
            return item

    def consume_any(self, token: str) -> Confirmation | None:
        """兼容管理命令测试的无上下文消费接口，不用于生产执行。"""
        with self._lock:
            item = self._items.pop(str(token).strip(), None)
            if item is None or item.expires_at <= time.time():
                return None
            return item

    def clear(self) -> None:
        """清空 reload 前签发的所有确认令牌。"""
        with self._lock:
            self._items.clear()

    def _purge(self, now: float) -> None:
        """删除过期令牌。"""
        for token, item in list(self._items.items()):
            if item.expires_at <= now:
                self._items.pop(token, None)
