"""反向 WebSocket 服务端测试。

LLBot/NapCat 作为 WS 客户端拨入本服务;token 校验 + 事件回调。
"""

import json

import aiohttp
import pytest

from onebot11.ws_server import ReverseWsServer


@pytest.fixture
async def server():
    """启动一个监听临时端口的服务端,事件收集到 received 列表。"""
    received: list[dict] = []

    async def on_event(raw: dict) -> None:
        received.append(raw)

    srv = ReverseWsServer(port=0, token="", on_event=on_event)
    await srv.start()
    yield srv, received
    await srv.stop()


async def _send_json(ws: aiohttp.ClientWebSocketResponse, payload: dict) -> None:
    await ws.send_str(json.dumps(payload))


async def test_无token时连接成功(server):
    """token 为空时,任何连接都接受。"""
    srv, _ = server
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"http://127.0.0.1:{srv.port}/") as ws:
            assert not ws.closed


async def test_错误token被拒绝():
    """配置 token 后,错误 token 握手被拒（401）。"""
    srv = ReverseWsServer(port=0, token="secret", on_event=lambda raw: None)
    await srv.start()
    try:
        async with aiohttp.ClientSession() as session:
            with pytest.raises(aiohttp.WSServerHandshakeError):
                async with session.ws_connect(
                    f"http://127.0.0.1:{srv.port}/", headers={"Authorization": "Bearer wrong"}
                ):
                    pass
    finally:
        await srv.stop()


async def test_正确token连接成功():
    """配置 token 后,正确 Bearer 握手成功。"""
    srv = ReverseWsServer(port=0, token="secret", on_event=lambda raw: None)
    await srv.start()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                f"http://127.0.0.1:{srv.port}/", headers={"Authorization": "Bearer secret"}
            ) as ws:
                assert not ws.closed
    finally:
        await srv.stop()


async def test_message事件触发回调(server):
    """收到 message 事件,on_event 收到原始 dict。"""
    srv, received = server
    raw = {
        "post_type": "message",
        "message_type": "group",
        "message_id": 1,
        "group_id": 888,
        "user_id": 123,
        "message": [{"type": "text", "data": {"text": "hi"}}],
    }
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"http://127.0.0.1:{srv.port}/") as ws:
            await _send_json(ws, raw)
            # 等待回调异步执行
            for _ in range(50):
                if received:
                    break
                await asyncio_sleep()
    assert received == [raw]


async def test_heartbeat不触发回调(server):
    """meta_event(heartbeat) 不触发 on_event。"""
    srv, received = server
    heartbeat = {"post_type": "meta_event", "meta_event_type": "heartbeat", "time": 1700000000}
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"http://127.0.0.1:{srv.port}/") as ws:
            await _send_json(ws, heartbeat)
            await asyncio_sleep()
    assert received == []


async def test_畸形JSON不崩(server):
    """非 JSON 帧被忽略,连接保持。"""
    srv, received = server
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"http://127.0.0.1:{srv.port}/") as ws:
            await ws.send_str("not-json{{{")
            await asyncio_sleep()
            assert not ws.closed
    assert received == []


async def test_事件处理失败关闭连接允许上游重放():
    """事件未能持久化时关闭来源连接，避免消息静默丢失。"""
    async def fail(_raw: dict) -> None:
        raise RuntimeError("queue unavailable")

    srv = ReverseWsServer(port=0, token="", on_event=fail)
    await srv.start()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{srv.port}/") as ws:
                await _send_json(ws, {"post_type": "message", "message_id": 99, "user_id": 7})
                message = await ws.receive(timeout=3)
                assert message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }
    finally:
        await srv.stop()


async def asyncio_sleep() -> None:
    import asyncio

    await asyncio.sleep(0.05)
