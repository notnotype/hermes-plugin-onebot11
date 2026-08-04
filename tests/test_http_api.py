"""HTTP API 发送与动作调用测试。

用真实 aiohttp 测试服务端模拟 LLBot 的 ob11 HTTP 服务,验证组包/鉴权/重试/分块。
"""

import pytest
from aiohttp import web

from onebot11.http_api import OneBotApiError, OneBotHttpApi, chunk_text


@pytest.fixture
async def fake_server():
    """模拟 OneBot 11 HTTP 服务端,记录请求并返回固定响应。"""
    calls: list[dict] = []
    fail_first = {"send_group_msg": 0}  # 每个 action 首次可注入 500

    async def handler(request: web.Request) -> web.Response:
        body = await request.json()
        calls.append(
            {
                "path": request.path,
                "params": body,
                "echo": request.query.get("echo"),
                "auth": request.headers.get("Authorization", ""),
            }
        )
        action = request.path.lstrip("/")
        if fail_first.get(action, 0) > 0:
            fail_first[action] -= 1
            return web.Response(status=500, text="boom")
        if action == "get_msg":
            data = {"message_id": body.get("message_id"), "message": [{"type": "text", "data": {"text": "原消息"}}]}
        elif action == "get_group_msg_history":
            data = {"messages": [{"message_id": 1, "message": [{"type": "text", "data": {"text": "历史1"}}]}]}
        elif action == "get_friend_msg_history":
            data = {"messages": [{"message_id": 2, "message": [{"type": "text", "data": {"text": "私聊历史"}}]}]}
        else:
            data = {"message_id": 42}
        return web.json_response({"status": "ok", "retcode": 0, "data": data})

    app = web.Application()
    app.router.add_post("/{action}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    yield f"http://127.0.0.1:{port}", calls, fail_first
    await runner.cleanup()


async def test_私聊发送组包(fake_server):
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base, token="tok")
    mid = await api.send_message("123456789", "你好", chat_type="dm")
    assert mid == "42"
    call = calls[0]
    assert call["path"] == "/send_private_msg"
    assert call["params"]["user_id"] == 123456789
    assert call["params"]["message"] == [{"type": "text", "data": {"text": "你好"}}]
    assert call["auth"] == "Bearer tok"
    assert call["echo"]


async def test_群聊发送带reply段(fake_server):
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    await api.send_message("88888888", "收到", chat_type="group", reply_to="1001")
    call = calls[0]
    assert call["path"] == "/send_group_msg"
    assert call["params"]["group_id"] == 88888888
    assert call["params"]["message"] == [
        {"type": "reply", "data": {"id": "1001"}},
        {"type": "text", "data": {"text": "收到"}},
    ]


async def test_失败重试一次成功(fake_server):
    base, calls, fail_first = fake_server
    fail_first["send_group_msg"] = 1
    api = OneBotHttpApi(base_url=base, max_retries=1)
    mid = await api.send_message("88888888", "重试", chat_type="group")
    assert mid == "42"
    assert len(calls) == 2  # 第一次 500,第二次成功


async def test_retcode非零抛错():
    """HTTP 200 但 retcode != 0 时抛 OneBotApiError。"""

    async def handler(request):
        return web.json_response({"status": "failed", "retcode": 100, "data": None})

    app = web.Application()
    app.router.add_post("/{action}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    try:
        api = OneBotHttpApi(base_url=base)
        with pytest.raises(OneBotApiError):
            await api.send_message("1", "x", chat_type="dm")
    finally:
        await runner.cleanup()


async def test_查询群消息历史(fake_server):
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    messages = await api.get_group_msg_history("88888888", count=20)
    assert messages[0]["message_id"] == 1
    call = calls[0]
    assert call["path"] == "/get_group_msg_history"
    assert call["params"]["group_id"] == 88888888
    assert call["params"]["count"] == 20


async def test_查询私聊历史(fake_server):
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    messages = await api.get_friend_msg_history("123456789", count=10)
    assert messages[0]["message_id"] == 2
    assert calls[0]["path"] == "/get_friend_msg_history"
    assert calls[0]["params"]["user_id"] == 123456789


def test_长文本分块():
    """超过限制的文本按行/字符切成多块,不截断单词。"""
    text = "一" * 30 + " " + "二" * 30
    chunks = chunk_text(text, limit=25)
    assert len(chunks) > 1
    assert "".join(chunks).replace(" ", "") == text.replace(" ", "")  # 内容不丢
    assert all(len(c) <= 25 for c in chunks)


def test_短文本不分块():
    assert chunk_text("你好", limit=100) == ["你好"]


def test_空文本返回空列表():
    assert chunk_text("", limit=100) == []
