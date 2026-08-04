"""触发判定器测试:mention/关键词/LLM 回调。"""
import asyncio

from onebot11.events import InboundEvent
from onebot11.queue import GroupMessageQueue
from onebot11.triggers import TriggerPolicy


def _ev(text: str, mentioned: bool = False) -> InboundEvent:
    return InboundEvent(
        text=text, chat_id="888", chat_type="group", user_id="1",
        user_name="小明", message_id="1", mentioned_self=mentioned,
    )


def test_mention恒触发():
    policy = TriggerPolicy()
    assert asyncio.run(policy.decide(_ev("在吗", mentioned=True), GroupMessageQueue())) is True


def test_无触发源不触发():
    policy = TriggerPolicy()
    assert asyncio.run(policy.decide(_ev("在吗"), GroupMessageQueue())) is False


def test_关键词触发():
    policy = TriggerPolicy(keywords=["机器人", "查询"])
    q = GroupMessageQueue()
    assert asyncio.run(policy.decide(_ev("机器人帮我查一下"), q)) is True
    assert asyncio.run(policy.decide(_ev("今天天气不错"), q)) is False


def test_llm回调判定():
    async def judge(chat_id: str, snapshot: str, current: str) -> bool:
        assert chat_id == "888"
        return current.startswith("帮我")

    policy = TriggerPolicy(llm_judge=judge)
    q = GroupMessageQueue()
    assert asyncio.run(policy.decide(_ev("帮我查一下"), q)) is True
    assert asyncio.run(policy.decide(_ev("哈哈"), q)) is False
