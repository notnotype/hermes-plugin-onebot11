"""确定性触发器矩阵测试。"""

from dataclasses import replace

import pytest

from onebot11.queue import QueueMessage
from onebot11.triggers import (
    LayeredTriggerState,
    TriggerConfig,
    build_llm_trigger_input,
    build_trigger_config,
    is_question,
    memory_matches,
    parse_llm_decision,
    should_trigger,
)


def test_mention和关键词触发():
    """@ 和 Unicode casefold 关键词都能触发群 turn。"""
    config = TriggerConfig(require_mention=True, keywords=("请回答",))
    assert should_trigger(
        chat_type="group", text="在吗", mentioned_self=True, config=config
    ).reason == "mention"
    assert should_trigger(
        chat_type="group", text="请回答这个", mentioned_self=False, config=config
    ).reason == "keyword"


def test_always和兼容模式触发():
    """always 或 require_mention=false 时普通群消息创建 trigger。"""
    assert should_trigger(
        chat_type="group", text="普通消息", mentioned_self=False,
        config=TriggerConfig(always=True),
    ).triggered
    assert should_trigger(
        chat_type="group", text="普通消息", mentioned_self=False,
        config=TriggerConfig(require_mention=False),
    ).reason == "always"


def test_私聊直接触发且冷却生效():
    """私聊不需要 @；冷却只抑制新的 turn，不影响入队。"""
    config = TriggerConfig(cooldown_seconds=10)
    assert should_trigger(
        chat_type="dm", text="hi", mentioned_self=False, config=config, now=20
    ).triggered
    decision = should_trigger(
        chat_type="dm", text="again", mentioned_self=False,
        config=config, last_trigger_at=15, now=20,
    )
    assert not decision.triggered
    assert decision.reason == "cooldown"


def test_硬触发绕过冷却():
    """@、关键词和 always 在 cooldown 内仍必须创建硬触发。"""
    config = TriggerConfig(
        require_mention=True,
        keywords=("请回答",),
        always=False,
        cooldown_seconds=60,
    )
    mention = should_trigger(
        chat_type="group",
        text="在吗",
        mentioned_self=True,
        config=config,
        last_trigger_at=100,
        now=101,
    )
    keyword = should_trigger(
        chat_type="group",
        text="请回答这个",
        mentioned_self=False,
        config=config,
        last_trigger_at=100,
        now=101,
    )
    always = should_trigger(
        chat_type="group",
        text="普通消息",
        mentioned_self=False,
        config=replace(config, always=True),
        last_trigger_at=100,
        now=101,
    )
    assert (mention.triggered, mention.reason) == (True, "mention")
    assert (keyword.triggered, keyword.reason) == (True, "keyword")
    assert (always.triggered, always.reason) == (True, "always")


def test_触发配置严格解析():
    """拒绝 bool('false') 和非 list/string 的关键词配置。"""
    config = build_trigger_config({"require_mention": "false", "trigger_keywords": ["你好"]})
    assert not config.require_mention
    assert config.keywords == ("你好",)
    with pytest.raises(ValueError):
        build_trigger_config({"require_mention": "not-a-bool"})
    with pytest.raises(ValueError):
        build_trigger_config({"trigger_keywords": {"nested": "no"}})


def test_触发词列表只接受字符串():
    """关键词和记忆词不能把数字静默转换成可触发文本。"""
    with pytest.raises(ValueError):
        build_trigger_config({"trigger_keywords": [123]})
    with pytest.raises(ValueError):
        build_trigger_config({"memory_trigger_words": [456]})
    with pytest.raises(ValueError):
        build_trigger_config({"trigger_keywords": [None]})


def test_触发数值配置越界或非有限值必须拒绝():
    """配置错误不能被静默夹紧，否则部署者会误以为配置已经生效。"""
    invalid_configs = (
        {"trigger_cooldown_seconds": -1},
        {"trigger_cooldown_seconds": float("nan")},
        {"llm_trigger": {"timeout": 0}},
        {"llm_trigger": {"input_bytes": 64_001}},
        {"llm_trigger": {"concurrency": 0}},
        {"llm_trigger": {"trigger_debounce_seconds": 61}},
        {"llm_trigger": {"engaged_idle_seconds": 0}},
        {"llm_trigger": {"engaged_max_seconds": 30, "engaged_idle_seconds": 60}},
        {"llm_trigger": {"engaged_max_arbitrations": -1}},
    )
    for extra in invalid_configs:
        with pytest.raises(ValueError):
            build_trigger_config(extra)


def test_分层状态机只让候选消息进入仲裁():
    """普通闲聊不消耗旁路 LLM，问句和有上下文回指才安排 debounce。"""
    config = TriggerConfig(debounce_seconds=5)
    state = LayeredTriggerState(config)
    assert state.observe_message(
        chat_type="group",
        text="大家吃饭了吗",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    ).kind == "schedule"

    state = LayeredTriggerState(config)
    assert state.observe_message(
        chat_type="group",
        text="今天天气不错",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    ).reason == "non_candidate"
    assert state.llm_calls == 0
    assert is_question("Can you help me?")
    assert memory_matches("继续刚才的话题", config.memory_words)


def test_判断期间新消息让旧结果变脏并重新debounce():
    """旧判断完成前有新 revision 时，不能直接使用旧快照触发。"""
    config = TriggerConfig(debounce_seconds=5)
    state = LayeredTriggerState(config)
    state.observe_message(
        chat_type="group",
        text="怎么处理这个问题？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    judgement = state.on_timer(now=5)
    assert judgement.kind == "judge"
    assert state.observe_message(
        chat_type="group",
        text="补充一条上下文",
        mentioned_self=False,
        has_context=True,
        revision=2,
        now=6,
    ).reason == "judging_dirty"
    rescheduled = state.on_llm_result(
        decision="trigger",
        wait_seconds=0,
        observed_revision=1,
        current_revision=2,
        now=7,
    )
    assert rescheduled.kind == "schedule"
    assert state.mode == "debounce"
    assert state.llm_calls == 1


def test_硬触发使旧LLM结果失效():
    """硬触发优先；旧旁路判断返回后不能再创建第二个触发请求。"""
    config = TriggerConfig(debounce_seconds=5)
    state = LayeredTriggerState(config)
    state.observe_message(
        chat_type="group",
        text="这个怎么处理？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    judgement = state.on_timer(now=5)
    assert judgement.kind == "judge"

    direct = state.observe_message(
        chat_type="group",
        text="@机器人马上处理",
        mentioned_self=True,
        has_context=True,
        revision=2,
        now=6,
    )
    assert direct.kind == "direct"
    stale = state.on_llm_result(
        decision="trigger",
        wait_seconds=0,
        observed_revision=1,
        current_revision=2,
        now=7,
    )
    assert stale.reason == "stale_judgement"


def test_旧generation不能污染新一轮LLM判断():
    """旧判断返回时若群已经开始新一轮判断，必须被 generation fencing 拒绝。"""
    config = TriggerConfig(debounce_seconds=1)
    state = LayeredTriggerState(config)
    state.observe_message(
        chat_type="group",
        text="第一条问题？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    first = state.on_timer(now=1)
    assert first.kind == "judge"

    state.on_llm_failure(
        now=1,
        current_revision=1,
        generation=first.generation,
    )
    state.observe_message(
        chat_type="group",
        text="第二条问题？",
        mentioned_self=False,
        has_context=False,
        revision=2,
        now=2,
    )
    second = state.on_timer(now=3)
    assert second.kind == "judge"
    assert second.generation != first.generation

    stale = state.on_llm_result(
        decision="trigger",
        wait_seconds=0,
        observed_revision=1,
        current_revision=2,
        now=4,
        generation=first.generation,
    )
    assert stale.reason == "stale_judgement"
    assert state.mode == "judging"


def test_模型失败在没有新消息时不空转():
    """同一 revision 的旁路模型连续失败不能形成无消息重试循环。"""
    config = TriggerConfig(debounce_seconds=1)
    state = LayeredTriggerState(config)
    state.observe_message(
        chat_type="group",
        text="这个怎么处理？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    assert state.on_timer(now=1).kind == "judge"
    assert state.on_llm_failure(now=2, current_revision=1).kind == "none"
    assert state.mode == "idle"
    assert state.on_timer(now=3).kind == "none"


def test_wait不创建lease且只等待新消息():
    """旁路 wait 只改变内存状态，到期后回到 idle，不会伪造 Agent turn。"""
    config = TriggerConfig(debounce_seconds=5)
    state = LayeredTriggerState(config)
    state.observe_message(
        chat_type="group",
        text="这个怎么弄？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    assert state.on_timer(now=5).kind == "judge"
    action = state.on_llm_result(
        decision="wait",
        wait_seconds=10,
        observed_revision=1,
        current_revision=1,
        now=5,
    )
    assert action.kind == "wait"
    assert state.mode == "waiting"
    assert state.on_timer(now=15).reason == "wait_expired"
    assert state.mode == "idle"
    assert state.arbitration_count == 0


def test_wait状态也受活跃窗口仲裁上限约束():
    """waiting 不应绕过 engaged 的旁路调用预算。"""
    state = LayeredTriggerState(
        TriggerConfig(debounce_seconds=1, engaged_max_arbitrations=1)
    )
    state.on_turn_complete(success=True, now=0)
    state.arbitration_count = 1
    state.mode = "waiting"
    state.wait_until = 10
    action = state.observe_message(
        chat_type="group",
        text="继续刚才的话题",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=5,
    )
    assert action.reason == "arbitration_limit"


def test_wait从engaged到期后保留剩余活跃窗口():
    """等待结束后恢复 engaged，adapter 仍可依据 engaged_until 继续计时。"""
    state = LayeredTriggerState(
        TriggerConfig(
            debounce_seconds=5,
            engaged_idle_seconds=60,
            engaged_max_seconds=300,
        )
    )
    state.mode = "waiting"
    state.wait_until = 20
    state.engaged_until = 95
    state.engaged_max_until = 300

    action = state.on_timer(now=20)

    assert action.kind == "none"
    assert action.reason == "wait_expired"
    assert state.mode == "engaged"
    assert state.engaged_until == 95


def test_engaged窗口最多三次仲裁并在成功后重新计数():
    """成功 turn 才开启活跃窗口，窗口内最多三次旁路仲裁。"""
    config = TriggerConfig(debounce_seconds=1, engaged_max_arbitrations=3)
    state = LayeredTriggerState(config)
    state.on_turn_complete(success=True, now=0)
    assert state.mode == "engaged"
    for index in range(3):
        assert state.observe_message(
            chat_type="group",
            text=f"普通追问 {index}",
            mentioned_self=False,
            has_context=True,
            revision=index + 1,
            now=index + 1,
        ).kind == "schedule"
        assert state.on_timer(now=index + 2).kind == "judge"
        state.on_llm_result(
            decision="ignore",
            wait_seconds=0,
            observed_revision=index + 1,
            current_revision=index + 1,
            now=index + 2,
        )
    assert state.observe_message(
        chat_type="group",
        text="第四条追问",
        mentioned_self=False,
        has_context=True,
        revision=4,
        now=10,
    ).reason == "arbitration_limit"
    state.on_timer(now=61)
    state.on_turn_complete(success=True, now=62)
    assert state.arbitration_count == 0


def test_完成时保留新消息建立的debounce状态():
    """旧 Agent turn 收尾不能覆盖期间新消息已经建立的候选状态。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    assert state.observe_message(
        chat_type="group",
        text="新消息怎么处理？",
        mentioned_self=False,
        has_context=True,
        revision=2,
        now=10,
    ).kind == "schedule"
    due_at = state.debounce_due
    state.on_turn_complete(
        success=True,
        now=11,
        preserve_pending=True,
        has_hard_trigger=False,
    )
    assert state.mode == "debounce"
    assert state.debounce_due == due_at


def test_失败turn即使保留pending也退出engaged():
    """失败、取消或未知结果不能把群留在成功后的连续对话窗口。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)
    assert state.mode == "engaged"
    state.on_turn_complete(
        success=False,
        now=1,
        preserve_pending=True,
        has_hard_trigger=False,
    )
    assert state.mode == "idle"
    assert state.engaged_until is None
    assert state.engaged_max_until is None


def test_llm决策严格限制字段和等待秒数():
    """非法 JSON 结构不能被宽松转换成触发。"""
    assert parse_llm_decision({"decision": "trigger", "wait_seconds": 0}) == ("trigger", 0)
    assert parse_llm_decision({"decision": "wait", "wait_seconds": 5}) == ("wait", 5)
    assert parse_llm_decision({"decision": "wait", "wait_seconds": 7}) is None
    assert parse_llm_decision({"decision": "trigger", "wait_seconds": 5}) is None
    assert parse_llm_decision({"decision": "ignore", "wait_seconds": 10}) is None
    assert parse_llm_decision({"decision": "trigger"}) is None
    assert parse_llm_decision({"decision": "trigger", "wait_seconds": False}) is None
    assert parse_llm_decision({"decision": "ignore", "wait_seconds": 0, "extra": 1}) is None


def test_llm提示词只展示合法的等待秒数合同():
    """模型看到的 JSON 示例必须与 parser 的严格合同一致。"""
    prompt = build_llm_trigger_input("", (), max_bytes=2048, candidate_type="question")
    assert '{"decision":"trigger","wait_seconds":0}' in prompt
    assert '{"decision":"wait","wait_seconds":5}' in prompt
    assert '{"decision":"ignore","wait_seconds":0}' in prompt
    assert '{"decision":"trigger|wait|ignore","wait_seconds":5}' not in prompt


def test_llm输入预算严格成立且保留最新消息():
    """旁路输入即使预算很小也不超限，并优先保留最新队列项。"""
    messages = (
        QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="old",
            user_id="1",
            user_name="旧用户",
            text="很早的上下文",
            seq=1,
        ),
        QueueMessage(
            chat_id="888",
            chat_type="group",
            message_id="latest",
            user_id="2",
            user_name="新用户",
            text="最新问题？",
            seq=2,
        ),
    )
    prompt = build_llm_trigger_input(
        "一段很长的历史摘要",
        messages,
        max_bytes=128,
        candidate_type="question",
    )
    assert len(prompt.encode("utf-8")) <= 128
    assert "最新问题？" in prompt


def test_llm输入预算小于提示词仍不超限():
    """直接调用者传入极小预算时也不能返回超限字符串。"""
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="latest",
        user_id="2",
        user_name="新用户",
        text="最新",
        seq=2,
    )
    prompt = build_llm_trigger_input("", (message,), max_bytes=8)
    assert len(prompt.encode("utf-8")) <= 8


def test_llm输入预算足够时保留完整JSON合同并裁剪最新正文():
    """正常配置预算下不能为了巨型最新消息截断严格输出合同。"""
    message = QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id="latest-large",
        user_id="2",
        user_name="新用户",
        text="最新问题？" + ("很长" * 1000),
        seq=2,
    )
    prompt = build_llm_trigger_input("", (message,), max_bytes=512)
    assert len(prompt.encode("utf-8")) <= 512
    assert '{"decision":"trigger","wait_seconds":0}' in prompt
    assert "最新问题？" in prompt
