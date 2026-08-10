"""OneBot11 管理动作的限长 JSONL 审计记录。"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class AuditLog:
    """仅记录摘要，不记录确认 token 或完整敏感消息。"""

    def __init__(self, path: str | Path | None, *, max_bytes: int = 2_000_000) -> None:
        """初始化可选审计文件。``None`` 表示只保留调用方日志。"""
        self.path = Path(path) if path else None
        self.max_bytes = max(1024, int(max_bytes))
        self._lock = threading.Lock()
        if self.path is not None:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                logger.warning(
                    "OneBot11 audit directory is unavailable; continuing without audit file",
                    exc_info=True,
                )
                self.path = None

    def record(self, action: str, fields: Mapping[str, Any]) -> None:
        """追加一条结构化审计事件并在超过上限时轮转。

        审计是旁路观测能力，不能因为目录只读、磁盘满或轮转失败而阻塞
        入站消息、权限拒绝或 queue completion。
        """
        if self.path is None:
            return
        try:
            entry = {"ts": time.time(), "action": str(action), **dict(fields)}
            entry = self._redact(entry)
            line = json.dumps(entry, ensure_ascii=False, sort_keys=True, default=str) + "\n"
            with self._lock:
                if self.path.exists() and self.path.stat().st_size + len(line.encode("utf-8")) > self.max_bytes:
                    rotated = self.path.with_suffix(self.path.suffix + ".1")
                    if rotated.exists():
                        rotated.unlink()
                    self.path.replace(rotated)
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(line)
        except Exception:
            logger.warning("OneBot11 audit write failed; continuing without audit record", exc_info=True)

    def _redact(self, value: Any, key: str = "") -> Any:
        """递归删除 token 等确认凭据，防止嵌套参数绕过审计边界。"""
        if any(secret in key.casefold() for secret in ("token", "secret", "password")):
            return "[redacted]"
        if isinstance(value, Mapping):
            return {str(name): self._redact(item, str(name)) for name, item in value.items()}
        if isinstance(value, list):
            return [self._redact(item, key) for item in value[:32]]
        if isinstance(value, tuple):
            return [self._redact(item, key) for item in value[:32]]
        return value
