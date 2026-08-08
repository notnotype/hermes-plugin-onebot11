"""OneBot 11 HTTP API 客户端。

查询动作可以有限重试；发送、撤回、禁言、踢人和全员禁言均不自动重试。
非幂等请求的连接/响应未知会以 ``unknown_outcome`` 暴露给上层。
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import uuid
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import aiohttp

logger = logging.getLogger(__name__)

QUERY_ACTIONS = frozenset(
    {
        "get_msg",
        "get_image",
        "get_group_msg_history",
        "get_friend_msg_history",
        "get_group_info",
        "get_group_member_info",
        "get_group_list",
    }
)
WRITE_ACTIONS = frozenset(
    {
        "send_group_msg",
        "send_private_msg",
        "delete_msg",
        "set_group_ban",
        "set_group_kick",
        "set_group_whole_ban",
        "set_msg_emoji_like",
        "unset_msg_emoji_like",
    }
)


def _parse_retcode(payload: dict) -> tuple[int, bool]:
    """解析 OneBot retcode，并区分缺失/非法值。"""
    raw = payload.get("retcode")
    if raw is None:
        return -1, False
    if isinstance(raw, bool):
        return -1, False
    try:
        return int(raw), True
    except (TypeError, ValueError):
        return -1, False


def parse_http_base_url(value: str) -> tuple[str, str, int | None]:
    """严格解析 OneBot HTTP 基地址，拒绝凭据、查询串和非法端口。"""
    raw = str(value or "").strip()
    if not raw or any(char.isspace() or ord(char) < 32 for char in raw):
        raise ValueError("OneBot HTTP API 地址不能为空且不能包含空白或控制字符")
    try:
        parsed = urlsplit(raw)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise ValueError("OneBot HTTP API 地址格式错误") from exc
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("OneBot HTTP API 地址必须使用 http 或 https")
    if not hostname:
        raise ValueError("OneBot HTTP API 地址缺少 host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("OneBot HTTP API 地址不能内嵌用户名或密码")
    if parsed.query or parsed.fragment:
        raise ValueError("OneBot HTTP API 地址不能包含 query 或 fragment")
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("OneBot HTTP API 端口必须在 1-65535 范围内")
    return parsed.scheme, hostname.casefold().rstrip("."), port


def is_loopback_http_url(value: str) -> bool:
    """严格解析 HTTP 地址后判断其 host 是否为本机回环。"""
    try:
        _scheme, host, _port = parse_http_base_url(value)
    except ValueError:
        return False
    try:
        address = ipaddress.ip_address(host)
        if address.is_loopback:
            return True
        mapped = getattr(address, "ipv4_mapped", None)
        return bool(mapped and mapped.is_loopback)
    except ValueError:
        return host == "localhost"


class OneBotApiError(Exception):
    """OneBot 动作调用失败。"""

    def __init__(
        self,
        action: str,
        status: str,
        retcode: int,
        *,
        unknown_outcome: bool = False,
        error_kind: str = "failed",
    ) -> None:
        """保存机器可读错误分类。"""
        self.action = action
        self.status = status
        self.retcode = int(retcode)
        self.unknown_outcome = bool(unknown_outcome)
        self.error_kind = error_kind
        super().__init__(f"OneBot 动作 {action} 失败: status={status} retcode={retcode}")


def chunk_text(text: str, limit: int) -> list[str]:
    """按 limit 切分长文本，优先在空格处断，保证内容不丢。"""
    if not text:
        return []
    if limit <= 0 or len(text) <= limit:
        return [text]
    chunks: list[str] = []
    rest = text
    while len(rest) > limit:
        cut = rest.rfind(" ", 0, limit + 1)
        if cut <= 0:
            cut = limit
        chunks.append(rest[:cut])
        rest = rest[cut:].lstrip()
    if rest:
        chunks.append(rest)
    return chunks


def is_numeric_message_id(value: str) -> bool:
    """判断 OneBot message_id 是否为可传给 API 的有符号整数。"""
    normalized = str(value or "").strip()
    return normalized.isdigit() or (
        normalized.startswith("-") and normalized[1:].isdigit()
    )


def _matches_image_magic(data: bytes, content_type: str, url: str) -> bool:
    """同时校验常见图片魔数和响应类型/后缀，拒绝伪装文件。"""
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        detected = "image/png"
    elif data.startswith(b"\xff\xd8\xff"):
        detected = "image/jpeg"
    elif data.startswith((b"GIF87a", b"GIF89a")):
        detected = "image/gif"
    elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        detected = "image/webp"
    else:
        detected = ""
    if not detected:
        return False
    normalized = (content_type or "").split(";", 1)[0].strip().lower()
    if normalized in {"application/octet-stream", ""} or normalized == "image/*":
        return True
    if normalized.startswith("image/"):
        return normalized == detected
    suffix = Path(urlsplit(url).path).suffix.lower()
    return suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp"} and normalized == ""


def is_valid_image_bytes(data: bytes, content_type: str = "", url: str = "") -> bool:
    """公开图片魔数校验，供本地 ``get_image`` 文件复制复用。"""
    return _matches_image_magic(data, content_type, url)


def image_suffix(data: bytes) -> str:
    """根据图片魔数返回安全扩展名。"""
    if data.startswith(b"\x89PNG"):
        return ".png"
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return ".gif"
    if data.startswith(b"RIFF"):
        return ".webp"
    return ".bin"


class OneBotHttpApi:
    """OneBot HTTP 客户端，按动作类型区分重试合同。"""

    def __init__(
        self,
        base_url: str,
        token: str = "",
        timeout: float = 10.0,
        max_retries: int = 1,
        *,
        max_response_bytes: int = 1_000_000,
        allowed_media_hosts: set[str] | None = None,
        allowed_media_ports: set[int] | None = None,
        max_media_bytes: int = 8_000_000,
        max_redirects: int = 3,
    ) -> None:
        """初始化 HTTP/API 与图片下载安全边界。"""
        self._base = str(base_url or "").strip().rstrip("/")
        if self._base:
            base_scheme, base_host, base_port = parse_http_base_url(self._base)
            self._base_host = base_host
            self._base_port = base_port or (443 if base_scheme == "https" else 80)
        else:
            self._base_host = ""
            self._base_port = None
        self._token = token or ""
        self._timeout = max(0.1, float(timeout))
        self._max_retries = max(0, int(max_retries))
        self.max_response_bytes = max(1024, int(max_response_bytes))
        explicit_media_hosts = {
            str(host).casefold().rstrip(".")
            for host in (allowed_media_hosts or set())
            if str(host).strip()
        }
        self.allowed_media_hosts = explicit_media_hosts
        explicit_media_ports = {int(port) for port in (allowed_media_ports or set())}
        if any(port < 1 or port > 65535 for port in explicit_media_ports):
            raise ValueError("allowed_media_ports 必须全部在 1-65535 范围内")
        self.allowed_media_ports = explicit_media_ports
        self.max_media_bytes = max(1024, int(max_media_bytes))
        self.max_redirects = max(0, int(max_redirects))
        self._session: aiohttp.ClientSession | None = None

    async def _session_or_create(self) -> aiohttp.ClientSession:
        """惰性创建共享 aiohttp 会话。"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """关闭共享会话。"""
        if self._session is not None and not self._session.closed:
            await self._session.close()

    def _headers(self) -> dict[str, str]:
        """构造鉴权头。"""
        return {"Authorization": f"Bearer {self._token}"} if self._token else {}

    async def _read_response_bytes(self, resp: aiohttp.ClientResponse, *, limit: int) -> bytes:
        """按 Content-Length 和实际读取双重限制响应体。"""
        raw_length = resp.headers.get("Content-Length")
        if raw_length is not None:
            try:
                length = int(raw_length)
            except (TypeError, ValueError) as exc:
                raise OneBotApiError("http", "malformed_content_length", -1, error_kind="protocol") from exc
            if length < 0 or length > limit:
                raise OneBotApiError("http", "response_too_large", -1, error_kind="too_large")
        body = await resp.content.read(limit + 1)
        if len(body) > limit:
            raise OneBotApiError("http", "response_too_large", -1, error_kind="too_large")
        return body

    async def call_action(
        self,
        action: str,
        params: dict,
        *,
        retryable: bool | None = None,
    ) -> dict:
        """调用 OneBot 动作，查询有限重试，非幂等动作绝不自动重试。"""
        session = await self._session_or_create()
        action = str(action)
        may_retry = action in QUERY_ACTIONS and (retryable is not False)
        attempts = self._max_retries if may_retry else 0
        url = f"{self._base}/{action}?echo={uuid.uuid4().hex}"
        for attempt in range(attempts + 1):
            try:
                async with session.post(url, json=params, headers=self._headers()) as resp:
                    try:
                        body = await self._read_response_bytes(resp, limit=self.max_response_bytes)
                    except OneBotApiError as exc:
                        if action in WRITE_ACTIONS:
                            raise OneBotApiError(
                                action,
                                exc.status,
                                exc.retcode,
                                unknown_outcome=True,
                                error_kind="unknown",
                            ) from exc
                        raise
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        if may_retry and resp.status in {429, 500, 502, 503, 504} and attempt < attempts:
                            await self._sleep_before_retry(resp, attempt)
                            continue
                        raise OneBotApiError(
                            action,
                            f"http_{resp.status}_non_json",
                            -1,
                            unknown_outcome=action in WRITE_ACTIONS,
                            error_kind="unknown" if action in WRITE_ACTIONS else "protocol",
                        ) from exc
                    if not isinstance(payload, dict):
                        raise OneBotApiError(
                            action,
                            "invalid_json",
                            -1,
                            unknown_outcome=action in WRITE_ACTIONS,
                            error_kind="unknown" if action in WRITE_ACTIONS else "protocol",
                        )
                    retcode, valid_retcode = _parse_retcode(payload)
                    if resp.status == 429:
                        if may_retry and attempt < attempts:
                            await self._sleep_before_retry(resp, attempt)
                            continue
                        raise OneBotApiError(
                            action,
                            "429",
                            retcode,
                            error_kind="rate_limited",
                        )
                    if resp.status >= 500:
                        if may_retry and attempt < attempts:
                            await self._sleep_before_retry(resp, attempt)
                            continue
                        raise OneBotApiError(
                            action,
                            str(resp.status),
                            retcode,
                            unknown_outcome=action in WRITE_ACTIONS,
                            error_kind="unknown" if action in WRITE_ACTIONS else "failed",
                        )
                    if resp.status >= 400:
                        raise OneBotApiError(
                            action,
                            str(resp.status),
                            retcode,
                            error_kind="failed",
                        )
                    if not valid_retcode:
                        raise OneBotApiError(
                            action,
                            "invalid_retcode",
                            retcode,
                            unknown_outcome=action in WRITE_ACTIONS,
                            error_kind="unknown" if action in WRITE_ACTIONS else "protocol",
                        )
                    if retcode != 0:
                        raise OneBotApiError(action, str(payload.get("status")), retcode)
                    data = payload.get("data")
                    return data if isinstance(data, dict) else {}
            except OneBotApiError:
                raise
            except (TimeoutError, aiohttp.ClientError, OSError) as exc:
                if may_retry and attempt < attempts:
                    await asyncio.sleep(0.3 * (attempt + 1))
                    continue
                raise OneBotApiError(
                    action,
                    f"network: {exc}",
                    -1,
                    unknown_outcome=action in WRITE_ACTIONS,
                    error_kind="unknown" if action in WRITE_ACTIONS else "network",
                ) from exc
        raise OneBotApiError(action, "unknown", -1, error_kind="unknown")

    async def _sleep_before_retry(self, resp: aiohttp.ClientResponse, attempt: int) -> None:
        """按 Retry-After 或有界退避等待查询重试。"""
        raw = resp.headers.get("Retry-After", "")
        try:
            delay = min(5.0, max(0.0, float(raw))) if raw else 0.3 * (attempt + 1)
        except (TypeError, ValueError):
            delay = 0.3 * (attempt + 1)
        await asyncio.sleep(delay)

    async def send_message(
        self, chat_id: str, text: str, *, chat_type: str, reply_to: str | None = None
    ) -> str:
        """发送一条文本消息，不重试未知出站请求。"""
        if chat_type not in {"group", "dm"}:
            raise ValueError(f"未知 OneBot chat_type: {chat_type!r}")
        message: list[dict] = []
        reply_id = str(reply_to or "").strip()
        if is_numeric_message_id(reply_id):
            message.append({"type": "reply", "data": {"id": reply_id}})
        message.append({"type": "text", "data": {"text": text}})
        action = "send_group_msg" if chat_type == "group" else "send_private_msg"
        key = "group_id" if chat_type == "group" else "user_id"
        data = await self.call_action(action, {key: int(chat_id), "message": message}, retryable=False)
        return str(data.get("message_id", ""))

    async def set_message_emoji_like(
        self, message_id: str, emoji_id: str, *, enabled: bool
    ) -> None:
        """给群消息添加或取消表情回应；该非幂等请求不自动重试。"""
        normalized_message_id = str(message_id or "").strip()
        if not is_numeric_message_id(normalized_message_id):
            raise ValueError("reaction 只能作用于真实 OneBot message_id")
        normalized_emoji_id = str(emoji_id or "").strip()
        if not normalized_emoji_id:
            raise ValueError("emoji_id 不能为空")
        await self.call_action(
            "set_msg_emoji_like",
            {
                "message_id": int(normalized_message_id),
                "emoji_id": normalized_emoji_id,
                "set": bool(enabled),
            },
            retryable=False,
        )

    async def download_to_temp(self, url: str, dest_dir: str) -> str | None:
        """经过 SSRF、重定向、大小、类型和魔数校验后下载图片。"""
        current = url
        session = await self._session_or_create()
        try:
            for _ in range(self.max_redirects + 1):
                self._validate_media_url(current)
                async with session.get(current, headers={}, allow_redirects=False) as resp:
                    if resp.status in {301, 302, 303, 307, 308}:
                        location = resp.headers.get("Location")
                        if not location:
                            return None
                        current = urljoin(current, location)
                        continue
                    if resp.status != 200:
                        return None
                    try:
                        data = await self._read_response_bytes(resp, limit=self.max_media_bytes)
                    except OneBotApiError:
                        return None
                    content_type = resp.headers.get("Content-Type", "")
                    if not _matches_image_magic(data, content_type, current):
                        return None
                    suffix = self._image_suffix(data, content_type)
                    path = Path(dest_dir) / f"{uuid.uuid4().hex}{suffix}"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        path.write_bytes(data)
                    except OSError:
                        path.unlink(missing_ok=True)
                        return None
                    return str(path)
            return None
        except (TimeoutError, aiohttp.ClientError, OSError, ValueError):
            return None

    def _validate_media_url(self, url: str) -> None:
        """校验图片 URL 协议、host、端口和 IP，阻断 SSRF。"""
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password or not parsed.hostname:
            raise ValueError("不允许的图片 URL")
        host = parsed.hostname.casefold()
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if not self.allowed_media_hosts or host.rstrip(".") not in self.allowed_media_hosts:
            raise ValueError("图片 host 不在 allowlist")
        if self.allowed_media_ports:
            if port not in self.allowed_media_ports:
                raise ValueError("图片 port 不在 allowlist")
        elif host.rstrip(".") != self._base_host or port != self._base_port:
            raise ValueError("非 HTTP API host/port 必须显式配置 media_allowed_ports")
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            # host 不是 literal IP；DNS rebinding 防护需要部署侧 allowlist，
            # 不在插件里偷偷解析并缓存地址。
            if not host.replace(".", "").replace("-", "").isalnum():
                raise ValueError("图片 host 格式错误") from None
        else:
            base_host = urlsplit(self._base).hostname
            if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified) and host != (base_host or "").casefold():
                raise ValueError("禁止访问本机/内网/保留地址")

    def _image_suffix(self, data: bytes, content_type: str) -> str:
        """依据魔数选择稳定扩展名。"""
        del content_type
        return image_suffix(data)

    async def get_message(self, message_id: str) -> dict:
        """查询单条消息。"""
        return await self.call_action("get_msg", {"message_id": int(message_id)})

    async def get_image(self, file_id: str) -> dict:
        """把 OneBot image file 标识解析为受控的本地路径或 URL。"""
        normalized = str(file_id or "").strip()
        if not normalized:
            raise ValueError("图片 file 不能为空")
        data = await self.call_action("get_image", {"file": normalized})
        returned_url = data.get("url")
        if returned_url is not None:
            if not isinstance(returned_url, str) or not returned_url.strip():
                raise ValueError("OneBot get_image 返回了无效 URL")
            self._validate_media_url(returned_url)
        return data

    async def get_group_msg_history(self, group_id: str, count: int = 20) -> list[dict]:
        """查询群消息历史。"""
        data = await self.call_action("get_group_msg_history", {"group_id": int(group_id), "message_seq": 0, "count": count})
        return data.get("messages") or []

    async def get_friend_msg_history(self, user_id: str, count: int = 20) -> list[dict]:
        """查询私聊消息历史。"""
        data = await self.call_action("get_friend_msg_history", {"user_id": int(user_id), "message_seq": 0, "count": count})
        return data.get("messages") or []
