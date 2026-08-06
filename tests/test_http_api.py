"""HTTP API 发送与动作调用测试。

用真实 aiohttp 测试服务端模拟 LLBot 的 ob11 HTTP 服务,验证组包/鉴权/重试/分块。
"""

import pytest
from aiohttp import web

from onebot11.http_api import (
    OneBotApiError,
    OneBotHttpApi,
    chunk_text,
    is_loopback_http_url,
    parse_http_base_url,
)


@pytest.mark.parametrize(
    "value",
    [
        "ftp://127.0.0.1:3000",
        "http://",
        "http://user:password@127.0.0.1:3000",
        "http://127.0.0.1:not-a-port",
        "http://127.0.0.1:0",
        "http://127.0.0.1:3000?echo=bad",
    ],
)
def test_HTTP地址严格校验(value):
    """scheme、host、凭据和端口错误不能被当作 loopback 安全地址。"""
    with pytest.raises(ValueError):
        parse_http_base_url(value)
    assert not is_loopback_http_url(value)


def test_HTTP地址接受合法回环():
    """合法回环地址用于本机 OneBot HTTP 服务。"""
    assert parse_http_base_url("http://127.0.0.1:3000") == ("http", "127.0.0.1", 3000)
    assert is_loopback_http_url("http://[::1]:3000")


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
    await api.close()


async def test_发送拒绝未知目标类型(fake_server):
    """协议客户端也不把未知类型猜成私聊。"""
    base, _calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    with pytest.raises(ValueError):
        await api.send_message("1", "x", chat_type="unknown")


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
    await api.close()


async def test_群消息reaction使用LLBot扩展且不自动重试(fake_server):
    """处理指示器使用真实消息 ID 和 LLBot 的 set 参数。"""
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base, max_retries=3)
    await api.set_message_emoji_like("-1001", "128064", enabled=True)
    await api.set_message_emoji_like("-1001", "128064", enabled=False)
    assert [call["path"] for call in calls] == ["/set_msg_emoji_like", "/set_msg_emoji_like"]
    assert calls[0]["params"] == {"message_id": -1001, "emoji_id": "128064", "set": True}
    assert calls[1]["params"] == {"message_id": -1001, "emoji_id": "128064", "set": False}
    await api.close()


async def test_reaction拒绝内部hash消息ID(fake_server):
    """没有真实 OneBot message_id 时不能发出无法定位目标的 reaction 请求。"""
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    with pytest.raises(ValueError):
        await api.set_message_emoji_like("hash:abc", "128064", enabled=True)
    assert calls == []
    await api.close()


async def test_reaction未知结果不自动重试(fake_server):
    """reaction 是非幂等扩展动作，网络结果未知时只发一次请求。"""
    base, calls, fail_first = fake_server
    fail_first["set_msg_emoji_like"] = 1
    api = OneBotHttpApi(base_url=base, max_retries=3)
    try:
        with pytest.raises(OneBotApiError) as exc_info:
            await api.set_message_emoji_like("-1001", "128064", enabled=True)
        assert exc_info.value.unknown_outcome
        assert len(calls) == 1
    finally:
        await api.close()


async def test_非数字reply_id不进入OneBot请求(fake_server):
    """缺少真实 OneBot message_id 时不能把内部 hash 当作 reply ID。"""
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    await api.send_message("88888888", "收到", chat_type="group", reply_to="hash:abc")
    assert calls[0]["params"]["message"] == [{"type": "text", "data": {"text": "收到"}}]
    await api.close()


async def test_负数message_id可以作为reply(fake_server):
    """LLBot 可能返回负数 message_id，仍应保留 OneBot reply 段。"""
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    await api.send_message("88888888", "收到", chat_type="group", reply_to="-1001")
    assert calls[0]["params"]["message"] == [
        {"type": "reply", "data": {"id": "-1001"}},
        {"type": "text", "data": {"text": "收到"}},
    ]
    await api.close()


async def test_非幂等发送失败不自动重试(fake_server):
    base, calls, fail_first = fake_server
    fail_first["send_group_msg"] = 1
    api = OneBotHttpApi(base_url=base, max_retries=1)
    try:
        with pytest.raises(OneBotApiError) as exc_info:
            await api.send_message("88888888", "不要重试", chat_type="group")
        assert exc_info.value.unknown_outcome
        assert len(calls) == 1
    finally:
        await api.close()


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
    api = None
    try:
        api = OneBotHttpApi(base_url=base)
        with pytest.raises(OneBotApiError):
            await api.send_message("1", "x", chat_type="dm")
    finally:
        if api is not None:
            await api.close()
            await runner.cleanup()


async def test_非法retcode的非幂等响应按未知处理():
    """无法解释 OneBot 成功/失败结果时不能把写请求当作确定失败。"""
    async def handler(request):
        return web.json_response({"status": "ok", "retcode": "not-an-int", "data": {}})

    app = web.Application()
    app.router.add_post("/{action}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    api = OneBotHttpApi(base_url=base)
    try:
        with pytest.raises(OneBotApiError) as exc_info:
            await api.send_message("1", "x", chat_type="dm")
        assert exc_info.value.unknown_outcome
    finally:
        await api.close()
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
    await api.close()


async def test_查询私聊历史(fake_server):
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    messages = await api.get_friend_msg_history("123456789", count=10)
    assert messages[0]["message_id"] == 2
    assert calls[0]["path"] == "/get_friend_msg_history"
    assert calls[0]["params"]["user_id"] == 123456789
    await api.close()


async def test_get_image使用file字段查询(fake_server):
    """非 URL 图片先通过 OneBot get_image 解析，不能把 file 直接当 URL。"""
    base, calls, _ = fake_server
    api = OneBotHttpApi(base_url=base)
    result = await api.get_image("abc.jpg")
    assert result["message_id"] == 42
    assert calls[0]["path"] == "/get_image"
    assert calls[0]["params"] == {"file": "abc.jpg"}
    await api.close()


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
