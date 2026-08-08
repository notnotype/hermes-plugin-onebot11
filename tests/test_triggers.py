"""确定性触发器和自动锚点选择合同测试。"""

import pytest

from onebot11.triggers import (
    AnchorDecision,
    TriggerConfig,
    TriggerEvaluator,
    TriggerMessageSnapshot,
    TriggerSnapshot,
    build_anchor_selector_prompt,
    build_trigger_config,
    build_trigger_snapshot,
    parse_anchor_decision,
    selector_schedule_reason,
    should_trigger,
)


def test_mention和关键词触发():
    """@ 和 Unicode casefold 关键词都能触发群 turn。"""
    config = TriggerConfig(require_mention=True, keywords=("请回答",))
    mention = should_trigger(
        chat_type="group", text="在吗", mentioned_self=True, config=config
    )
    keyword = should_trigger(
        chat_type="group", text="请回答这个", mentioned_self=False, config=config
    )
    assert (mention.triggered, mention.reason, mention.explicit) == (True, "mention", True)
    assert (keyword.triggered, keyword.reason, keyword.explicit) == (True, "keyword", True)


def test_mention和关键词绕过冷却而兼容策略仍受冷却():
    """明确点名不能被旧 cooldown 静默压制，always 兼容策略保持旧节流行为。"""
    config = TriggerConfig(
        require_mention=False,
        keywords=("bot",),
        cooldown_seconds=60,
    )
    mention = should_trigger(
        chat_type="group",
        text="在吗",
        mentioned_self=True,
        config=config,
        last_trigger_at=99,
        now=100,
    )
    keyword = should_trigger(
        chat_type="group",
        text="bot 帮我查一下",
        mentioned_self=False,
        config=config,
        last_trigger_at=99,
        now=100,
    )
    compatible = should_trigger(
        chat_type="group",
        text="普通消息",
        mentioned_self=False,
        config=config,
        last_trigger_at=99,
        now=100,
    )
    assert mention.triggered and mention.reason == "mention" and mention.explicit
    assert keyword.triggered and keyword.reason == "keyword" and keyword.explicit
    assert not compatible.triggered
    assert compatible.reason == "cooldown"
    assert not compatible.explicit


def test_always和兼容模式触发():
    """always 兼容模式只能调度自动选择器，不能直接继承发送者 authority。"""
    always = should_trigger(
        chat_type="group", text="普通消息", mentioned_self=False,
        config=TriggerConfig(always=True),
    )
    compatible = should_trigger(
        chat_type="group", text="普通消息", mentioned_self=False,
        config=TriggerConfig(require_mention=False),
    )
    assert always.triggered and not always.explicit
    assert not always.creates_message_anchor
    assert compatible.reason == "always" and not compatible.explicit


def test_私聊直接触发且冷却生效():
    """私聊不需要 @；冷却只抑制新的 turn，不影响入队。"""
    config = TriggerConfig(cooldown_seconds=10)
    first = should_trigger(
        chat_type="dm", text="hi", mentioned_self=False, config=config, now=20
    )
    assert first.triggered and not first.explicit
    decision = should_trigger(
        chat_type="dm", text="again", mentioned_self=False,
        config=config, last_trigger_at=15, now=20,
    )
    assert not decision.triggered
    assert decision.reason == "cooldown"
    assert not decision.explicit


def test_默认cooldown使用持久化wall_clock(monkeypatch):
    """未显式传入 now 时，默认时间源必须与 SQLite 时间戳同为 wall-clock。"""
    from onebot11 import triggers as trigger_module

    monkeypatch.setattr(trigger_module, "wall_clock", lambda: 100.0)
    decision = should_trigger(
        chat_type="group",
        text="普通消息",
        mentioned_self=False,
        config=TriggerConfig(require_mention=False, cooldown_seconds=60),
        last_trigger_at=99.0,
    )
    assert not decision.triggered
    assert decision.reason == "cooldown"


def test_队列消息投影不包含角色或工具配置():
    """自动选择器只能看消息事实，authority 必须等选定锚点后再解析。"""
    from onebot11.queue import QueueMessage

    snapshot = build_trigger_snapshot(
        "100",
        (
            QueueMessage(
                chat_id="100",
                chat_type="group",
                message_id="9",
                user_id="200",
                user_name="小明",
                text="帮我查一下",
                seq=7,
                metadata={
                    "onebot11_reply_to": "8",
                    "onebot11_markers": ["reply", "image"],
                    "role": "super_admin",
                    "allowed_tools": ["terminal"],
                },
            ),
        ),
    )
    assert snapshot.messages[0] == TriggerMessageSnapshot(
        7,
        "200",
        "小明",
        "帮我查一下",
        reply_to_message_id="8",
        markers=("reply", "image"),
    )


def test_触发配置严格解析():
    """拒绝 bool('false') 和非 list/string 的关键词配置。"""
    config = build_trigger_config({"require_mention": "false", "trigger_keywords": ["你好"]})
    assert not config.require_mention
    assert config.keywords == ("你好",)
    with pytest.raises(ValueError):
        build_trigger_config({"require_mention": "not-a-bool"})
    with pytest.raises(ValueError):
        build_trigger_config({"trigger_keywords": {"nested": "no"}})


@pytest.mark.asyncio
async def test_最小TriggerEvaluator合同():
    """Evaluator 只接收脱敏快照并返回锚点决定。"""

    class FirstMessageEvaluator:
        """用于验证 Protocol 形状的最小 evaluator。"""

        async def evaluate(self, snapshot: TriggerSnapshot) -> AnchorDecision:
            return AnchorDecision(snapshot.messages[0].seq, "automatic_request")

    evaluator: TriggerEvaluator = FirstMessageEvaluator()
    snapshot = TriggerSnapshot(
        chat_id="100",
        messages=(TriggerMessageSnapshot(1, "200", "小明", "bot，帮我查一下"),),
    )
    assert await evaluator.evaluate(snapshot) == AnchorDecision(1, "automatic_request")


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            '{"anchor_seq": 7, "reason_code": "automatic_request"}',
            AnchorDecision(7, "automatic_request"),
        ),
        (
            '{"anchor_seq": null, "reason_code": "no_request"}',
            AnchorDecision(None, "no_request"),
        ),
    ],
)
def test_严格解析自动锚点决定(payload: str, expected: AnchorDecision):
    """只接受 anchor_seq 与固定 reason_code 的精确 JSON object。"""
    assert parse_anchor_decision(payload) == expected


@pytest.mark.parametrize(
    "payload",
    [
        "true",
        "```json\n{\"anchor_seq\": 1, \"reason_code\": \"automatic_request\"}\n```",
        '{"anchor_seq": true, "reason_code": "automatic_request"}',
        '{"anchor_seq": 0, "reason_code": "automatic_request"}',
        '{"anchor_seq": 1, "reason_code": "no_request"}',
        '{"anchor_seq": null, "reason_code": "automatic_request"}',
        '{"anchor_seq": 1, "reason_code": "other"}',
        '{"anchor_seq": 1, "reason_code": "automatic_request", "role": "admin"}',
        '{"anchor_seq": 1, "anchor_seq": 2, "reason_code": "automatic_request"}',
    ],
)
def test_拒绝宽松或越权的自动锚点输出(payload: str):
    """模型不能返回旧布尔值、额外权限字段、重复键或不匹配状态。"""
    with pytest.raises(ValueError):
        parse_anchor_decision(payload)


def test_selector_prompt有界且不暴露角色和工具字段():
    """Prompt 只物化选择锚点所需字段，并优先保留最早消息。"""
    snapshot = TriggerSnapshot(
        chat_id="100",
        messages=(
            TriggerMessageSnapshot(
                3,
                "201",
                "甲",
                "bot，帮我查询" + "很长" * 500,
                reply_to_message_id="9001",
                markers=("reply", "image"),
            ),
            TriggerMessageSnapshot(4, "202", "乙", "第二个请求"),
        ),
    )
    result = build_anchor_selector_prompt(snapshot, max_bytes=700)
    prompt = result.text
    assert len(prompt.encode("utf-8")) <= 700
    assert result.visible_max_seq == 3
    assert '"seq":3' in prompt
    assert '"user_id":"201"' in prompt
    assert '"reply_to_message_id":"9001"' in prompt
    assert '"markers":["reply","image"]' in prompt
    assert "role" not in prompt.casefold()
    assert "tools" not in prompt.casefold()
    assert "因字节预算截断" in prompt


def test_候选和活跃窗口只返回调度信号():
    """question/reply/active window 只安排 evaluator，不直接创建 anchor。"""
    assert selector_schedule_reason(text="怎么配置？") == "question"
    assert selector_schedule_reason(text="接着说", reply_to_message_id="9") == "reply"
    assert selector_schedule_reason(text="继续", active_window=True) == "active_window"
    assert selector_schedule_reason(text="普通闲聊") is None
