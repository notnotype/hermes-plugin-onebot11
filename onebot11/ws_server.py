"""OneBot 11 反向 WebSocket 服务端。

接收队列有界；事件进入队列前不提交内存去重，处理失败会撤销内存去重，
让上游重连/重放仍有机会恢复。持久去重事实由 QueueStore 负责。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable

from aiohttp import WSMsgType, web

logger = logging.getLogger(__name__)
OnEvent = Callable[[dict], Awaitable[None]]
OnEventFailure = Callable[[], Awaitable[None]]


def _is_loopback(host: str) -> bool:
    """判断监听地址是否为本机回环地址。"""
    return host in {"127.0.0.1", "::1", "localhost"}


class ReverseWsServer:
    """带鉴权、边界和并发限制的反向 WS 服务端。"""

    def __init__(
        self,
        port: int,
        token: str,
        on_event: OnEvent,
        *,
        host: str = "127.0.0.1",
        max_queue: int = 256,
        max_inflight: int = 32,
    ) -> None:
        """初始化监听参数；非回环地址必须有 token。"""
        if not _is_loopback(host) and not str(token or "").strip():
            raise ValueError("OneBot11 WS 监听非 loopback 地址时必须配置 token")
        self._host = host
        self._port = int(port)
        self._token = token or ""
        self._on_event = on_event
        self._max_queue = max(1, int(max_queue))
        self._queue: asyncio.Queue[tuple[str, str, dict, OnEventFailure | None]] = asyncio.Queue(maxsize=self._max_queue)
        self._max_inflight = max(1, int(max_inflight))
        self._consumer: asyncio.Task[None] | None = None
        self._chat_tasks: dict[str, asyncio.Task[None]] = {}
        self._chat_buffers: dict[str, deque[tuple[str, dict, OnEventFailure | None]]] = defaultdict(deque)
        self._inflight = asyncio.Semaphore(self._max_inflight)
        self._admitted = 0
        self._dedupe: set[str] = set()
        self._dedupe_order: deque[str] = deque()
        self._dedupe_lock = asyncio.Lock()
        self._app = web.Application()
        self._app.router.add_get("/", self._ws_handler)
        self._runner: web.AppRunner | None = None
        self.port: int = self._port

    async def start(self) -> None:
        """启动 HTTP/WS 服务和有界事件消费者。"""
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        sockets = self._runner.addresses
        if sockets:
            self.port = int(sockets[0][1])
        self._consumer = asyncio.create_task(self._consume())

    async def stop(self) -> None:
        """停止服务并等待消费者退出。"""
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        consumer = self._consumer
        self._consumer = None
        if consumer is not None:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        chat_tasks = list(self._chat_tasks.values())
        self._chat_tasks.clear()
        for task in chat_tasks:
            task.cancel()
        if chat_tasks:
            await asyncio.gather(*chat_tasks, return_exceptions=True)
        async with self._dedupe_lock:
            self._queue = asyncio.Queue(maxsize=self._max_queue)
            self._chat_buffers.clear()
            self._dedupe.clear()
            self._dedupe_order.clear()
            self._admitted = 0

    def _event_key(self, raw: dict) -> str:
        """构造消息事件内存去重键。"""
        message_id = raw.get("message_id")
        self_id = raw.get("self_id") or ""
        if message_id not in (None, ""):
            chat_id = raw.get("group_id") or raw.get("user_id") or ""
            return f"{self_id}:{raw.get('message_type', '')}:{chat_id}:{message_id}"
        encoded = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        return "hash:" + hashlib.sha256(encoded).hexdigest()

    def _chat_key(self, raw: dict) -> str:
        """按消息目标划分顺序 lane，避免同号群和私聊互相串线。"""
        self_id = raw.get("self_id") or ""
        if raw.get("group_id") not in (None, ""):
            return f"{self_id}:group:{raw.get('group_id')}"
        if raw.get("user_id") not in (None, ""):
            return f"{self_id}:dm:{raw.get('user_id')}"
        return f"{self_id}:event:global"

    async def _admit(self, raw: dict, on_failure: OnEventFailure | None = None) -> bool:
        """在进入有界队列前完成轻量去重和背压检查。"""
        key = self._event_key(raw)
        async with self._dedupe_lock:
            if key in self._dedupe:
                return False
            if self._admitted >= self._max_queue:
                raise RuntimeError("OneBot11 WS 接收队列已满") from None
            try:
                self._queue.put_nowait((key, self._chat_key(raw), raw, on_failure))
            except asyncio.QueueFull:
                raise RuntimeError("OneBot11 WS 接收队列已满") from None
            self._dedupe.add(key)
            self._dedupe_order.append(key)
            while len(self._dedupe_order) > max(1024, self._max_queue * 8):
                self._dedupe.discard(self._dedupe_order.popleft())
            self._admitted += 1
            return True

    async def _consume(self) -> None:
        """消费事件并在处理失败时撤销内存去重。"""
        while True:
            key, chat_key, raw, on_failure = await self._queue.get()
            buffer = self._chat_buffers[chat_key]
            buffer.append((key, raw, on_failure))
            if chat_key not in self._chat_tasks:
                self._chat_tasks[chat_key] = asyncio.create_task(self._consume_chat(chat_key))
            self._queue.task_done()

    async def _consume_chat(self, chat_key: str) -> None:
        """按入站顺序处理一个 chat lane，同时受全局并发上限约束。"""
        async with self._inflight:
            while self._chat_buffers.get(chat_key):
                key, raw, on_failure = self._chat_buffers[chat_key].popleft()
                try:
                    await self._on_event(raw)
                except asyncio.CancelledError:
                    async with self._dedupe_lock:
                        self._dedupe.discard(key)
                    raise
                except Exception:
                    async with self._dedupe_lock:
                        self._dedupe.discard(key)
                    logger.exception("OneBot11 事件处理失败，允许上游重放: %s", key)
                    if on_failure is not None:
                        try:
                            await on_failure()
                        except Exception:
                            logger.debug("关闭失败事件所在 WS 连接时出错", exc_info=True)
                finally:
                    async with self._dedupe_lock:
                        self._admitted = max(0, self._admitted - 1)
            self._chat_buffers.pop(chat_key, None)
        current = asyncio.current_task()
        if self._chat_tasks.get(chat_key) is current:
            self._chat_tasks.pop(chat_key, None)

    async def _ws_handler(self, request: web.Request) -> web.StreamResponse:
        """校验 Bearer token 并把事件送入有界接收队列。"""
        if self._token and request.headers.get("Authorization", "") != f"Bearer {self._token}":
            return web.Response(status=401, text="unauthorized")
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async def close_after_processing_failure() -> None:
            """处理失败时关闭当前连接，让 OneBot 端进入重连/重放路径。"""
            if not ws.closed:
                await ws.close(code=1011, message=b"event-processing-failed")

        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    raw = json.loads(msg.data)
                except (json.JSONDecodeError, TypeError):
                    logger.debug("忽略非 JSON 帧")
                    continue
                if not isinstance(raw, dict):
                    continue
                if raw.get("post_type") == "meta_event" and raw.get("meta_event_type") == "heartbeat":
                    continue
                try:
                    await self._admit(raw, close_after_processing_failure)
                except RuntimeError:
                    logger.warning("OneBot11 WS 接收队列已满，关闭连接触发上游恢复")
                    await ws.close(code=1013, message=b"backpressure")
                    break
        finally:
            await asyncio.shield(ws.close())
        return ws
