"""OneBot 11 HTTP API 客户端。

发送消息与动作调用都走 LLBot/NapCat 的 ob11 HTTP 服务
（POST /{action},params 为请求体,echo 走查询参数）。
本模块零 Hermes 依赖,可独立测试。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)


class OneBotApiError(Exception):
    """OneBot 动作调用失败（HTTP 非 2xx 或 retcode != 0）。"""

    def __init__(self, action: str, status: str, retcode: int) -> None:
        self.action = action
        self.status = status
        self.retcode = retcode
        super().__init__(f"OneBot 动作 {action} 失败: status={status} retcode={retcode}")


def chunk_text(text: str, limit: int) -> list[str]:
    """按 limit 切分长文本,优先在空格处断,保证内容不丢。"""
    if not text:
        return []
    if len(text) <= limit:
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


class OneBotHttpApi:
    """ob11 HTTP API 客户端。

    - base_url: HTTP 服务地址,如 http://127.0.0.1:3000
    - token: Bearer token,为空则不带 Authorization 头
    - max_retries: 网络错误时的重试次数（HTTP 5xx 也触发）
    """

    def __init__(
        self, base_url: str, token: str = "", timeout: float = 10.0, max_retries: int = 1
    ) -> None:
        self._base = base_url.rstrip("/")
        self._token = token or ""
        self._timeout = timeout
        self._max_retries = max_retries
        self._session: aiohttp.ClientSession | None = None

    async def _session_or_create(self) -> aiohttp.ClientSession:
        """惰性创建共享会话。"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        """关闭共享会话。"""
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def call_action(self, action: str, params: dict) -> dict:
        """调用一个 OneBot 11 动作,返回响应 data。失败抛 OneBotApiError。"""
        session = await self._session_or_create()
        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        url = f"{self._base}/{action}?echo={uuid.uuid4().hex}"

        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                async with session.post(url, json=params, headers=headers) as resp:
                    if resp.status >= 500 and attempt < self._max_retries:
                        # 服务端 5xx：短暂等待后重试
                        await asyncio.sleep(0.3 * (attempt + 1))
                        continue
                    payload = await resp.json()
                    if payload.get("retcode") != 0:
                        raise OneBotApiError(action, str(payload.get("status")), payload.get("retcode"))
                    return payload.get("data") or {}
            except aiohttp.ClientError as exc:
                last_error = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(0.3 * (attempt + 1))
                    continue
                raise OneBotApiError(action, f"network: {exc}", -1) from exc
        raise OneBotApiError(action, f"unknown: {last_error}", -1)

    async def send_message(
        self, chat_id: str, text: str, *, chat_type: str, reply_to: str | None = None
    ) -> str:
        """发送一条文本消息,返回 message_id。

        - chat_type: "group" → send_group_msg,"dm" → send_private_msg
        - reply_to: 引用消息 id,转成 reply 段放在最前
        """
        message: list[dict] = []
        if reply_to:
            message.append({"type": "reply", "data": {"id": str(reply_to)}})
        message.append({"type": "text", "data": {"text": text}})

        if chat_type == "group":
            data = await self.call_action("send_group_msg", {"group_id": int(chat_id), "message": message})
        else:
            data = await self.call_action("send_private_msg", {"user_id": int(chat_id), "message": message})
        return str(data.get("message_id", ""))

    async def download_to_temp(self, url: str, dest_dir: str) -> str | None:
        """下载 url 到 dest_dir,返回本地路径;失败返回 None。"""
        session = await self._session_or_create()
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.read()
        except aiohttp.ClientError:
            return None
        suffix = Path(url).suffix or ".bin"
        path = Path(dest_dir) / f"{uuid.uuid4().hex}{suffix}"
        path.write_bytes(data)
        return str(path)

    async def get_message(self, message_id: str) -> dict:
        """按 message_id 查单条消息。"""
        return await self.call_action("get_msg", {"message_id": int(message_id)})

    async def get_group_msg_history(self, group_id: str, count: int = 20) -> list[dict]:
        """查群消息历史（message_seq=0 表示最近的消息）。"""
        data = await self.call_action(
            "get_group_msg_history", {"group_id": int(group_id), "message_seq": 0, "count": count}
        )
        return data.get("messages") or []

    async def get_friend_msg_history(self, user_id: str, count: int = 20) -> list[dict]:
        """查私聊消息历史。"""
        data = await self.call_action(
            "get_friend_msg_history", {"user_id": int(user_id), "message_seq": 0, "count": count}
        )
        return data.get("messages") or []
