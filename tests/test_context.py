"""上下文拼接器测试:原文保留、摘要回调、总长上限。"""
import asyncio

from onebot11.context import build_group_context
from onebot11.queue import GroupMessageQueue


def _fill(q: GroupMessageQueue, chat: str, n: int) -> None:
    for i in range(n):
        q.push(chat, f"msg{i}", "1", "小明", float(i))


def test_无消息返回空():
    q = GroupMessageQueue()
    assert asyncio.run(build_group_context(q, "g1")) == ""


def test_只保留最近keep_raw条原文():
    q = GroupMessageQueue()
    _fill(q, "g1", 8)
    ctx = asyncio.run(build_group_context(q, "g1", keep_raw=3))
    assert "msg5" in ctx and "msg7" in ctx
    assert "msg0" not in ctx  # 更早的被丢弃(无摘要器时)


def test_摘要回调覆盖更早消息():
    q = GroupMessageQueue()
    _fill(q, "g1", 8)

    async def summarizer(blob: str) -> str:
        return f"摘要:{len(blob)}字"

    ctx = asyncio.run(build_group_context(q, "g1", keep_raw=2, summarizer=summarizer))
    assert "此前摘要" in ctx and "摘要:" in ctx
    assert "msg7" in ctx  # 最近原文仍在


def test_总长超限截断():
    q = GroupMessageQueue()
    _fill(q, "g1", 3)
    ctx = asyncio.run(build_group_context(q, "g1", max_chars=20))
    assert len(ctx) <= 21
