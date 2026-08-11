"""OneBot 11 出站媒体去重的纯协议逻辑。

这里不认识 Hermes。适配器只在一个明确的 turn scope 内使用
``MediaDeliveryScope``，不跨 session、重启或不同群永久拦截媒体。
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit


def _short_hash(value: str) -> str:
    """为路径或 URL 生成不暴露原文的受限 fingerprint。"""
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()[:32]


def normalize_media_source(source: str) -> str:
    """规范化本地路径或 HTTP(S) URL，供同一 turn 内比较。"""
    raw = unquote(str(source or "").strip())
    if raw.casefold().startswith("file://"):
        path = unquote(raw[7:])
        # ``file:///C:/x.png`` 在 Windows 上会多一个根路径斜杠。
        if os.name == "nt" and len(path) >= 3 and path[0] == "/" and path[2] == ":":
            path = path[1:]
        try:
            normalized = str(Path(path).expanduser().resolve(strict=False))
        except OSError:
            normalized = path
        return "path:" + os.path.normcase(os.path.normpath(normalized))

    parsed = urlsplit(raw)
    if parsed.scheme.casefold() in {"http", "https"} and parsed.netloc:
        try:
            host = (parsed.hostname or "").casefold().rstrip(".")
            port = parsed.port
        except ValueError:
            host = ""
            port = None
        if host:
            default_port = (parsed.scheme.casefold() == "http" and port == 80) or (
                parsed.scheme.casefold() == "https" and port == 443
            )
            netloc = host if port is None or default_port else f"{host}:{port}"
            return urlunsplit(
                (
                    parsed.scheme.casefold(),
                    netloc,
                    parsed.path or "/",
                    parsed.query,
                    "",
                )
            )

    try:
        normalized = str(Path(raw).expanduser().resolve(strict=False))
    except OSError:
        normalized = raw
    return "path:" + os.path.normcase(os.path.normpath(normalized))


class MediaDeliveryScope:
    """记录一个 turn 内已经尝试过的媒体来源和内容摘要。"""

    def __init__(self, scope_id: str) -> None:
        """创建一个短生命周期 scope。"""
        self.scope_id = str(scope_id)
        self._source_fingerprints: set[str] = set()
        self._content_fingerprints: set[str] = set()

    def source_fingerprint(self, source: str) -> str:
        """返回不包含完整路径或 URL 的来源 fingerprint。"""
        return _short_hash(normalize_media_source(source))

    def content_fingerprint(self, content: bytes) -> str:
        """返回当前 turn 内图片内容的 SHA-256 fingerprint。"""
        return hashlib.sha256(bytes(content)).hexdigest()[:32]

    def would_duplicate(self, source: str, content: bytes | None = None) -> bool:
        """判断来源或已读取内容是否已在当前 scope 中出现。"""
        if self.source_fingerprint(source) in self._source_fingerprints:
            return True
        if content is not None and self.content_fingerprint(content) in self._content_fingerprints:
            return True
        return False

    def remember(self, source: str, content: bytes | None = None) -> str:
        """登记一次媒体尝试，并返回受限 fingerprint。"""
        source_fingerprint = self.source_fingerprint(source)
        self._source_fingerprints.add(source_fingerprint)
        if content is not None:
            content_fingerprint = self.content_fingerprint(content)
            self._content_fingerprints.add(content_fingerprint)
            return content_fingerprint
        return source_fingerprint

    def claim(self, source: str, content: bytes | None = None) -> tuple[bool, str]:
        """原子式地判断并登记媒体，返回 ``(是否新媒体, fingerprint)``。"""
        fingerprint = (
            self.content_fingerprint(content)
            if content is not None
            else self.source_fingerprint(source)
        )
        if self.would_duplicate(source, content):
            return False, fingerprint
        self.remember(source, content)
        return True, fingerprint

    def clear(self) -> None:
        """清空 scope，供 turn 完成或 adapter shutdown 时回收。"""
        self._source_fingerprints.clear()
        self._content_fingerprints.clear()
