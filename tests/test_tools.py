"""平台工具 handler 测试：会话上下文注入（群号/QQ 号从会话取,不由 LLM 传）。"""

import pytest
from aiohttp import web

from onebot11.http_api import OneBotHttpApi
from onebot11.permissions import ToolContext
from onebot11.tools import (
    TOOL_SCHEMAS,
    handle_get_friend_msg_history,
    handle_get_group_msg_history,
    handle_get_message,
)


@pytest.fixture
async def fake_server():
    """记录请求路径与参数,返回固定数据。"""
    calls: list[dict] = []

    async def handler(request: web.Request) -> web.Response:
        body = await request.json()
        calls.append({"path": request.path, "params": body})
        action = request.path.lstrip("/")
        if action == "get_msg":
            data = {
                "message_id": body.get("message_id"),
                "message_type": "group",
                "group_id": 888,
                "message": [{"type": "text", "data": {"text": "原消息"}}],
            }
        elif action == "get_group_msg_history":
            data = {"messages": [{"message_id": 1, "message": [{"type": "text", "data": {"text": "历史1"}}]}]}
        else:
            data = {"messages": [{"message_id": 2, "message": [{"type": "text", "data": {"text": "私聊历史"}}]}]}
        return web.json_response({"status": "ok", "retcode": 0, "data": data})

    app = web.Application()
    app.router.add_post("/{action}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    base = f"http://127.0.0.1:{runner.addresses[0][1]}"
    yield base, calls
    await runner.cleanup()


async def test_查单条消息(fake_server):
    base, calls = fake_server
    api = OneBotHttpApi(base_url=base)
    ctx = ToolContext(user_id="123", chat_type="group", chat_id="888")
    result = await handle_get_message(api, {"message_id": "1001"}, ctx)
    assert result["status"] == "ok"
    assert result["message"]["message_id"] == 1001
    assert calls[0]["path"] == "/get_msg"
    assert calls[0]["params"]["message_id"] == 1001
    await api.close()


async def test_hash_message_key不会进入get_msg():
    """没有真实 OneBot message_id 时返回结构化错误，不调用远端查询。"""
    calls: list[str] = []

    class StubApi:
        """记录是否错误访问了 OneBot。"""

        async def get_message(self, _message_id: str) -> dict:
            calls.append("get_msg")
            return {}

    result = await handle_get_message(
        StubApi(),
        {"message_id": "hash:abc"},
        ToolContext(user_id="123", chat_type="group", chat_id="888"),
    )
    assert result["status"] == "invalid_message_id"
    assert calls == []


async def test_负数真实message_id可以进入get_msg(fake_server):
    """真实 OneBot 有符号 message_id 不能被误判成 hash key。"""
    base, calls = fake_server
    api = OneBotHttpApi(base_url=base)
    result = await handle_get_message(
        api,
        {"message_id": "-1001"},
        ToolContext(user_id="123", chat_type="group", chat_id="888"),
    )
    assert result["status"] == "ok"
    assert calls[0]["params"]["message_id"] == -1001
    await api.close()


async def test_群历史群号从会话注入(fake_server):
    """group_id 取自会话,LLM 传了也会被忽略。"""
    base, calls = fake_server
    api = OneBotHttpApi(base_url=base)
    ctx = ToolContext(user_id="123", chat_type="group", chat_id="888")
    result = await handle_get_group_msg_history(api, {"count": 5, "group_id": "999"}, ctx)
    assert result["group_id"] == "888"
    assert calls[0]["path"] == "/get_group_msg_history"
    assert calls[0]["params"]["group_id"] == 888  # 注入的是会话群号
    assert calls[0]["params"]["count"] == 5
    await api.close()


async def test_群历史默认条数20(fake_server):
    base, calls = fake_server
    api = OneBotHttpApi(base_url=base)
    ctx = ToolContext(user_id="123", chat_type="group", chat_id="888")
    await handle_get_group_msg_history(api, {}, ctx)
    assert calls[0]["params"]["count"] == 20
    await api.close()


async def test_私聊历史QQ从会话注入(fake_server):
    base, calls = fake_server
    api = OneBotHttpApi(base_url=base)
    ctx = ToolContext(user_id="999", chat_type="dm", chat_id="999")
    result = await handle_get_friend_msg_history(api, {"count": 10}, ctx)
    assert result["user_id"] == "999"
    assert calls[0]["path"] == "/get_friend_msg_history"
    assert calls[0]["params"]["user_id"] == 999  # 注入的是会话本人
    await api.close()


async def test_群历史任一条越界时整次拒绝():
    """历史接口不能只校验查询参数，返回列表也必须逐条属于当前群。"""
    class StubApi:
        """返回一条合法和一条跨群消息的最小 API stub。"""

        async def get_group_msg_history(self, _group_id: str, _count: int) -> list[dict]:
            return [
                {"message_type": "group", "group_id": 888, "message_id": 1},
                {"message_type": "group", "group_id": 999, "message_id": 2},
            ]

    result = await handle_get_group_msg_history(
        StubApi(), {}, ToolContext(user_id="123", chat_type="group", chat_id="888")
    )
    assert result["status"] == "permission_error"


async def test_私聊历史任一条不含当前参与者时整次拒绝():
    """私聊历史必须逐条证明当前 QQ 号是消息参与者。"""
    class StubApi:
        """返回一条不属于当前私聊的最小 API stub。"""

        async def get_friend_msg_history(self, _user_id: str, _count: int) -> list[dict]:
            return [{"message_type": "private", "user_id": 999, "target_id": 998}]

    result = await handle_get_friend_msg_history(
        StubApi(), {}, ToolContext(user_id="123", chat_type="dm", chat_id="123")
    )
    assert result["status"] == "permission_error"


def test_查询和群管理工具schema齐全():
    assert set(TOOL_SCHEMAS.keys()) == {
        "qq_get_message",
        "qq_get_group_msg_history",
        "qq_get_friend_msg_history",
        "qq_get_group_info",
        "qq_get_group_member_info",
        "qq_delete_message",
        "qq_set_group_ban",
        "qq_set_group_kick",
        "qq_set_group_whole_ban",
    }
    for schema in TOOL_SCHEMAS.values():
        assert schema["type"] == "object"
