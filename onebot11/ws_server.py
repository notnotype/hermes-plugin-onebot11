"""OneBot 11 反向 WebSocket 服务端。

LLBot/NapCat 的 ob11 ws-reverse 作为 WS 客户端主动拨入本服务。
收到 message 事件后回调 on_event(raw)；meta_event(heartbeat) 直接忽略。
本模块零 Hermes 依赖,可独立测试。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from aiohttp import WSMsgType, web

logger = logging.getLogger(__name__)

# 事件回调签名：收到 message 事件的原始 dict
OnEvent = Callable[[dict], Awaitable[None]]


class ReverseWsServer:
    """反向 WS 服务端。

    - port: 监听端口（0 = 随机临时端口,启动后读 self.port）
    - token: Bearer token;为空则接受所有连接
    - on_event: 收到 message 事件时异步回调
    """

    def __init__(self, port: int, token: str, on_event: OnEvent) -> None:
        self._port = port
        self._token = token or ""
        self._on_event = on_event
        self._app = web.Application()
        self._app.router.add_get("/", self._ws_handler)
        self._runner: web.AppRunner | None = None
        self.port: int = port  # 启动后为实际绑定端口（port=0 时为随机端口）

    async def start(self) -> None:
        """启动 HTTP/WS 服务。"""
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()
        sockets = self._runner.addresses  # type: ignore[attr-defined]
        if sockets:
            self.port = int(sockets[0][1])

    async def stop(self) -> None:
        """停止服务并清理。"""
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _ws_handler(self, request: web.Request) -> web.StreamResponse:
        """WS 握手：校验 Authorization: Bearer <token>。"""
        if self._token:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {self._token}":
                return web.Response(status=401, text="unauthorized")
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    raw = json.loads(msg.data)
                except (json.JSONDecodeError, TypeError):
                    logger.debug("忽略非 JSON 帧")
                    continue
                if raw.get("post_type") == "meta_event":
                    # heartbeat/lifecycle 不进会话
                    continue
                try:
                    await self._on_event(raw)
                except Exception:
                    logger.exception("事件回调失败")
        finally:
            await asyncio.shield(ws.close())
        return ws
