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
    is_significant_question,
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


def test_llm触发默认超时30秒且失败上限3次():
    """selector 必须给慢模型留足超时，并在连续失败后停止自动重试。"""
    config = build_trigger_config(
        {"llm_trigger": {"enabled": True, "provider": "p", "model": "m"}}
    )
    assert config.llm_timeout_seconds == 30.0
    assert config.llm_max_failures == 3


def test_llm触发失败上限可配置且拒绝越界():
    """max_failures 使用严格整数解析，拒绝布尔和越界值。"""
    config = build_trigger_config(
        {
            "llm_trigger": {
                "enabled": True,
                "provider": "p",
                "model": "m",
                "max_failures": 5,
            }
        }
    )
    assert config.llm_max_failures == 5
    with pytest.raises(ValueError):
        build_trigger_config(
            {
                "llm_trigger": {
                    "enabled": True,
                    "provider": "p",
                    "model": "m",
                    "max_failures": True,
                }
            }
        )
    with pytest.raises(ValueError):
        build_trigger_config(
            {
                "llm_trigger": {
                    "enabled": True,
                    "provider": "p",
                    "model": "m",
                    "max_failures": 99,
                }
            }
        )
def test_selector默认客服参数与疑似问句候选():
    """客服默认使用 2 秒 debounce，疑似问法只进入 selector 候选。"""
    config = build_trigger_config({})
    assert config.debounce_seconds == 2.0
    assert config.llm_provider == "deepseek"
    assert config.llm_model == "deepseek-v4-flash"
    state = LayeredTriggerState(config)
    for index, text in enumerate(
        (
            "有没有办法处理这个？",
            "想问一下这个怎么弄",
            "请教一下项目要放哪",
            "能不能帮忙看一下",
            "这个正常吗",
        ),
        start=1,
    ):
        action = state.observe_message(
            chat_type="group",
            text=text,
            mentioned_self=False,
            has_context=False,
            revision=index,
            now=float(index),
        )
        assert action.kind == "schedule"
        state.invalidate_judgement()

def test_问句范围门控要求bot关联和兴趣词():
    """配置门控后，普通问句不进 selector，范围内问句仍保留候选。"""
    config = TriggerConfig(
        question_bot_words=("机器人", "助手"),
        question_interest_words=("项目", "配置"),
        debounce_seconds=5,
    )
    state = LayeredTriggerState(config)
    out_of_scope = state.observe_message(
        chat_type="group",
        text="今天吃什么？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    assert out_of_scope.kind == "none"
    assert out_of_scope.reason == "question_out_of_scope"
    assert state.llm_calls == 0

    in_scope = state.observe_message(
        chat_type="group",
        text="机器人，项目配置怎么改？",
        mentioned_self=False,
        has_context=False,
        revision=2,
        now=1,
    )
    assert in_scope.kind == "schedule"
    assert in_scope.candidate_type == "question"


def test_问句范围门控保留引用bot和bot提问后的同用户追问():
    """引用 bot 或回复 bot 的同一用户可绕过 bot 词，但仍命中兴趣词。"""
    config = TriggerConfig(
        question_bot_words=("机器人",),
        question_interest_words=("配置",),
        debounce_seconds=5,
    )
    state = LayeredTriggerState(config)
    quoted = state.observe_message(
        chat_type="group",
        text="配置怎么改？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
        reply_to_bot=True,
    )
    assert quoted.kind == "schedule"
    assert quoted.candidate_type == "question"

    state = LayeredTriggerState(config)
    state.on_turn_complete(
        success=True,
        now=0,
        bot_asked=True,
        anchor_user_id="u1",
    )
    follow_up = state.observe_message(
        chat_type="group",
        text="配置怎么改？",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
        user_id="u1",
    )
    assert follow_up.kind == "schedule"
    assert follow_up.candidate_type == "question"


def test_问句范围门控允许兴趣词独立配置():
    """兴趣词可独立启用，让其他成员的相关问句进入 selector；bot 词仍可收紧范围。"""
    with pytest.raises(ValueError):
        build_trigger_config({"question_bot_words": ["机器人"]})

    topic_only = build_trigger_config(
        {"question_interest_words": ["AI", "资料"]}
    )
    assert topic_only.question_bot_words == ()
    assert topic_only.question_interest_words == ("ai", "资料")

    strict = build_trigger_config(
        {
            "question_bot_words": ["机器人"],
            "question_interest_words": ["项目"],
        }
    )
    assert strict.question_bot_words == ("机器人",)
    assert strict.question_interest_words == ("项目",)

def test_无bot关联的AI和资料问句进入selector():
    """放宽 bot 关联后，其他群成员的 AI/资料问题也应成为候选。"""
    config = TriggerConfig(
        question_bot_words=(),
        question_interest_words=("AI", "资料", "搜索"),
        debounce_seconds=5,
    )
    state = LayeredTriggerState(config)

    ai_question = state.observe_message(
        chat_type="group",
        text="AI 模型怎么选？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
        user_id="other-user",
    )
    assert ai_question.kind == "schedule"
    assert ai_question.candidate_type == "question"

    state.invalidate_judgement()
    research_question = state.observe_message(
        chat_type="group",
        text="能帮我找资料和搜索文档吗？",
        mentioned_self=False,
        has_context=False,
        revision=2,
        now=1,
        user_id="another-user",
    )
    assert research_question.kind == "schedule"
    assert research_question.candidate_type == "question"

def test_有上下文的解惑请求进入selector():
    """当前句省略主题时，明确求助短语可交给 selector 结合上下文判断。"""
    config = TriggerConfig(
        question_interest_words=("AI", "资料", "技术", "项目"),
        debounce_seconds=5,
    )
    no_context = LayeredTriggerState(config).observe_message(
        chat_type="group",
        text="有没有大佬解惑一下",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
        user_id="other-user",
    )
    assert no_context.kind == "none"
    assert no_context.reason == "question_out_of_scope"

    state = LayeredTriggerState(config)
    action = state.observe_message(
        chat_type="group",
        text="有没有大佬解惑一下",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=0,
        user_id="other-user",
    )
    assert action.kind == "schedule"
    assert action.candidate_type == "question"


def test_selector提示词不会把疑问词当作必答触发():
    """疑问词只能提供候选信号，群成员闲聊仍明确默认 ignore。"""
    prompt = build_llm_trigger_input(
        "",
        (),
        4_000,
        candidate_type="question",
    )
    assert "必须 trigger" not in prompt
    assert "只代表候选" in prompt
    assert "明确向机器人求助" in prompt
    assert "AI、资料检索、文档、技术、项目" in prompt
    assert "群成员之间的互动" in prompt


def test_分层状态机只让显著问句进入仲裁():
    """普通闲聊问句不消耗旁路 LLM，明确主题问题才安排 debounce。"""
    config = TriggerConfig(debounce_seconds=5)
    state = LayeredTriggerState(config)
    weak = state.observe_message(
        chat_type="group",
        text="大家吃饭了吗",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    assert weak.kind == "none"
    assert weak.reason == "weak_question"
    assert state.llm_calls == 0

    strong = LayeredTriggerState(config).observe_message(
        chat_type="group",
        text="AI 模型怎么选？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )
    assert strong.kind == "schedule"
    assert strong.candidate_type == "question"
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
        anchor_seq=1,
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
        anchor_seq=1,
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
        anchor_seq=1,
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
    config = TriggerConfig(debounce_seconds=5, engaged_idle_seconds=10)
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
        anchor_seq=None,
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
            anchor_seq=None,
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


def test_engaged短确认词统一交给selector判断():
    """活跃对话中的短确认词没有特例，统一进入 selector 决定是否唤醒。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)

    action = state.observe_message(
        chat_type="group",
        text="可以。",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )

    assert action.kind == "schedule"
    assert action.candidate_type == "engaged"
    assert state.llm_calls == 0
    assert state.mode == "debounce"


def test_waiting短确认词和真实问句都进入selector():
    """selector 的等待状态对短确认词没有特例，一律交给 selector。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)
    state.mode = "waiting"
    state.wait_until = 30

    acknowledgement = state.observe_message(
        chat_type="group",
        text="好的",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert acknowledgement.kind == "schedule"
    assert acknowledgement.candidate_type == "wait_followup"
    assert state.llm_calls == 0


def test_engaged问句优先走question候选():
    """活跃对话中的问句（含不带问号的）走 question 候选，不走 engaged 软判断。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)

    action = state.observe_message(
        chat_type="group",
        text="可以吗？",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert action.kind == "schedule"
    assert action.candidate_type == "question"
    assert state.llm_calls == 0


def test_engaged普通无主题问句不进入selector():
    """活跃窗口也过滤普通闲聊问句，主题问题才进入 question 候选。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)
    weak = state.observe_message(
        chat_type="group",
        text="你吃饭了吗",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert weak.kind == "none"
    assert weak.reason == "weak_question"
    assert state.llm_calls == 0

    state = LayeredTriggerState(
        TriggerConfig(
            question_interest_words=("AI", "项目"),
            debounce_seconds=5,
        )
    )
    state.on_turn_complete(success=True, now=0)
    strong = state.observe_message(
        chat_type="group",
        text="AI 模型怎么选",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert strong.kind == "schedule"
    assert strong.candidate_type == "question"
    assert state.llm_calls == 0


def test_idle短确认词不直接唤醒():
    """没有成功 Agent 回复建立活跃窗口时，短确认词仍然只是普通闲聊。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))

    action = state.observe_message(
        chat_type="group",
        text="可以",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )

    assert action.kind == "none"
    assert action.reason == "non_candidate"
    assert state.llm_calls == 0


def test_judging中短确认词不打断当前selector判断():
    """selector 正在判断时短确认词只标记 dirty，等待当前结果后再仲裁。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)
    state.mode = "judging"
    state.dirty_revision = 1

    action = state.observe_message(
        chat_type="group",
        text="可以",
        mentioned_self=False,
        has_context=True,
        revision=2,
        now=1,
    )

    assert action.kind == "none"
    assert action.reason == "judging_dirty"
    assert state.dirty_revision == 2
    assert state.llm_calls == 0


def test_debounce中短确认词继续累积debounce():
    """debounce（候选等待）中活跃窗口内的短确认词继续合并等待，不打断判断。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)
    state.mode = "debounce"
    state.debounce_due = 6

    action = state.observe_message(
        chat_type="group",
        text="好的",
        mentioned_self=False,
        has_context=True,
        revision=2,
        now=3,
    )

    assert action.kind == "schedule"
    assert state.llm_calls == 0


def test_自适应debounce第一条消息立即判断():
    """本群第一条候选消息视为不活跃，立即进入判断而不是等待固定窗口。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))

    action = state.observe_message(
        chat_type="group",
        text="这个问题怎么处理？",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=10,
    )

    assert action.kind == "schedule"
    assert state.debounce_due == 10


def test_自适应debounce消息间隔超过窗口立即判断():
    """距上一条消息超过 debounce 窗口（群不活跃）时立即判断。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.observe_message(
        chat_type="group",
        text="闲聊",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )

    action = state.observe_message(
        chat_type="group",
        text="这个问题怎么处理？",
        mentioned_self=False,
        has_context=False,
        revision=2,
        now=10,
    )

    assert action.kind == "schedule"
    assert state.debounce_due == 10


def test_自适应debounce消息间隔在窗口内则补齐等待():
    """距上一条消息 3 秒（活跃）时补齐到 5 秒窗口再判断。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.observe_message(
        chat_type="group",
        text="闲聊",
        mentioned_self=False,
        has_context=False,
        revision=1,
        now=0,
    )

    action = state.observe_message(
        chat_type="group",
        text="这个问题怎么处理？",
        mentioned_self=False,
        has_context=False,
        revision=2,
        now=3,
    )

    assert action.kind == "schedule"
    assert state.debounce_due == 5


def test_失败turn即使保留pending也退出engaged():
    """失败、取消或 unknown 不能把下一条普通消息误当连续对话。"""
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


def test_llm决策严格限制真实anchor_seq合同():
    """非法 JSON 结构不能被宽松转换成触发。"""
    assert parse_llm_decision({"decision": "trigger", "anchor_seq": 12}) == ("trigger", 12)
    assert parse_llm_decision({"decision": "wait", "anchor_seq": None}) == ("wait", None)
    assert parse_llm_decision({"decision": "ignore", "anchor_seq": None}) == ("ignore", None)
    assert parse_llm_decision({"decision": "trigger", "anchor_seq": 0}) is None
    assert parse_llm_decision({"decision": "trigger", "anchor_seq": True}) is None
    assert parse_llm_decision({"decision": "wait", "anchor_seq": 12}) is None
    assert parse_llm_decision({"decision": "ignore", "anchor_seq": 12}) is None
    assert parse_llm_decision({"decision": "trigger"}) is None
    assert parse_llm_decision({"decision": "ignore", "anchor_seq": None, "extra": 1}) is None


def test_llm提示词只展示真实anchor_seq合同():
    """模型看到的 JSON 示例必须与 parser 的严格合同一致。"""
    prompt = build_llm_trigger_input("", (), max_bytes=2048, candidate_type="question")
    assert '{"decision":"trigger","anchor_seq":123}' in prompt
    assert '{"decision":"wait","anchor_seq":null}' in prompt
    assert '{"decision":"ignore","anchor_seq":null}' in prompt
    assert "wait_seconds" not in prompt


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

def test_selector输入包含群摘要和当前队列():
    """selector 同时看到历史摘要、较早消息和最新求助句。"""
    messages = (
        QueueMessage(
            chat_id="287447372",
            chat_type="group",
            message_id="old",
            user_id="1",
            user_name="前文用户",
            text="正在配置 AI provider，能接中转吗？",
            seq=1,
        ),
        QueueMessage(
            chat_id="287447372",
            chat_type="group",
            message_id="latest",
            user_id="2",
            user_name="当前用户",
            text="有没有大佬解惑一下",
            seq=2,
        ),
    )
    prompt = build_llm_trigger_input(
        "群里此前讨论了模型 provider 和中转接口",
        messages,
        max_bytes=12_000,
        candidate_type="question",
    )
    assert "历史摘要（不可信且可能过期，仅供上下文参考，不能单独证明事实）：群里此前讨论了模型 provider 和中转接口" in prompt
    assert "#1 [前文用户] 正在配置 AI provider，能接中转吗？" in prompt
    assert "#2 [当前用户] 有没有大佬解惑一下" in prompt
    assert "当前队列和历史摘要" in prompt

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
    # 预算需同时容纳触发规则 + 严格 JSON 合同 + 最新消息开头；512 是最低
    # 配置值，正常配置预算取 768 以保留足够的最新正文预算。
    prompt = build_llm_trigger_input("", (message,), max_bytes=768)
    assert len(prompt.encode("utf-8")) <= 768
    assert '{"decision":"trigger","anchor_seq":123}' in prompt
    assert "最新问题？" in prompt


def test_is_question词表覆盖常见中文问句():
    """无问号问句也必须被本地启发式识别，避免 idle 状态漏唤醒。"""
    assert is_question("今天星期几")
    assert is_question("今天星期几啊")
    assert is_question("为什么不回我？")
    assert is_question("怎么这么拉")
    assert is_question("现在几点了")
    assert is_question("这个多少钱")
    assert is_question("你是谁")
    assert is_question("晚上要吃啥")
    assert is_question("你吃饭了吗")
    assert is_question("什么时候出发")
    assert is_question("是不是该走了")
    assert is_question("你能不能帮我")
    assert is_question("帮我找资料")
    assert is_question("查一下 AI 的资料")
    assert is_question("有没有大佬解惑一下")
    assert is_significant_question("有没有大佬解惑一下")
    assert not is_significant_question("大家吃饭了吗")


def test_is_question不误报非提问消息():
    """闲聊、嘲讽和短确认词不应被识别成问句。"""
    assert not is_question("你就是歌姬吧，所以它不回你")
    assert not is_question("可以")
    assert not is_question("好的")
    assert not is_question("哈哈哈哈")
    assert not is_question("今天天气不错")


def test_selector提示词说明问句仅是候选():
    """合同明确问句只唤醒候选，模型仍需判断是否确实需要回复。"""
    prompt = build_llm_trigger_input(
        "",
        (
            QueueMessage(
                chat_id="888",
                chat_type="group",
                message_id="1",
                user_id="2",
                user_name="用户",
                text="今天星期几",
                seq=1,
            ),
        ),
        max_bytes=12_000,
        candidate_type="question",
    )
    assert "只代表候选" in prompt
    assert "明确向机器人求助" in prompt
    assert "不等于必须回复" in prompt
    assert "必须 trigger" not in prompt
    assert '{"decision":"trigger","anchor_seq":123}' in prompt


def test_selector_engaged提示词默认ignore成员互动():
    """engaged 候选仍需区分真实追问与群成员互动。"""
    prompt = build_llm_trigger_input(
        "",
        (
            QueueMessage(
                chat_id="888",
                chat_type="group",
                message_id="1",
                user_id="2",
                user_name="用户",
                text="快去配置",
                seq=1,
            ),
        ),
        max_bytes=12_000,
        candidate_type="engaged",
    )
    assert "候选类型：engaged" in prompt
    assert "只代表候选" in prompt
    assert "延续与机器人的对话" in prompt
    assert "默认 ignore" in prompt


def test_纯图片消息不进selector():
    """没有文本的图片/媒体消息不能成为 LLM 候选，但 @ 硬触发仍生效。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)

    action = state.observe_message(
        chat_type="group",
        text="",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert action.kind == "none"
    assert action.reason == "no_text"

    mentioned = state.observe_message(
        chat_type="group",
        text="",
        mentioned_self=True,
        has_context=True,
        revision=2,
        now=2,
    )
    assert mentioned.kind == "direct"
    assert mentioned.reason == "mention"


def test_bot提问后同用户回复进入deep并立即仲裁():
    """bot 上轮以问句收尾时，同用户下一条消息升 deep 且免 debounce。"""
    config = TriggerConfig(
        debounce_seconds=5,
        engaged_idle_seconds=60,
        engaged_max_arbitrations=2,
    )
    state = LayeredTriggerState(config)
    state.on_turn_complete(
        success=True,
        now=0,
        bot_asked=True,
        anchor_user_id="2056963663",
    )
    assert state.mode == "engaged"
    assert state.level == "deep"
    assert state.bot_asked is True

    action = state.observe_message(
        chat_type="group",
        text="可以",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
        user_id="2056963663",
    )
    assert action.kind == "schedule"
    assert state.mode == "debounce"
    assert state.debounce_due is not None and state.debounce_due <= 1.0


def test_bot提问后他人消息不享受deep立即仲裁():
    """deep 预算绑定同用户，避免一人提问全群升级。"""
    config = TriggerConfig(debounce_seconds=5, engaged_idle_seconds=60)
    state = LayeredTriggerState(config)
    state.on_turn_complete(
        success=True,
        now=0,
        bot_asked=True,
        anchor_user_id="2056963663",
    )
    action = state.observe_message(
        chat_type="group",
        text="可以",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
        user_id="1259901822",
    )
    assert action.kind == "schedule"
    # 他人插话回落 normal 预算，不享受 deep 档。
    assert state.level == "normal"


def test_任务词升级deep但同用户消息立即判():
    """报错/复现等任务词只升预算，仍走 selector，不直接触发。"""
    config = TriggerConfig(
        debounce_seconds=5,
        engaged_idle_seconds=60,
        task_words=("报错", "复现"),
    )
    state = LayeredTriggerState(config)
    state.on_turn_complete(success=True, now=0, anchor_user_id="2056963663")
    action = state.observe_message(
        chat_type="group",
        text="还是报错",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
        user_id="2056963663",
    )
    assert action.kind == "schedule"
    assert state.level == "deep"
    assert state.debounce_due is not None and state.debounce_due <= 1.0


def test_连续ignore降级到shallow():
    """连续两次 ignore 使预算降档，第三次同类消息进入 shallow 预算。"""
    config = TriggerConfig(
        debounce_seconds=1,
        engaged_max_arbitrations=3,
        shallow_engaged_idle_seconds=30,
        shallow_max_arbitrations=1,
    )
    state = LayeredTriggerState(config)
    state.on_turn_complete(success=True, now=0)
    for index in range(2):
        action = state.observe_message(
            chat_type="group",
            text=f"普通消息 {index}",
            mentioned_self=False,
            has_context=True,
            revision=index + 1,
            now=index + 1,
        )
        assert action.kind == "schedule"
        state.on_timer(now=index + 2)
        state.on_llm_result(
            decision="ignore",
            anchor_seq=None,
            observed_revision=index + 1,
            current_revision=index + 1,
            now=index + 2,
        )
    assert state.level == "shallow"
    assert state.ignore_streak == 2


def test_short_rule默认关闭短确认词仍进selector():
    """short_rule 默认关闭，engaged 短确认词仍统一交给 selector。"""
    state = LayeredTriggerState(TriggerConfig(debounce_seconds=5))
    state.on_turn_complete(success=True, now=0)
    action = state.observe_message(
        chat_type="group",
        text="好的",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert action.kind == "schedule"
    assert action.candidate_type == "engaged"


def test_short_rule开启时shallow短消息本地ignore():
    """short_rule 开启后，shallow 档无信号短消息不进 selector。"""
    config = TriggerConfig(
        debounce_seconds=5,
        short_rule_max_chars=20,
    )
    state = LayeredTriggerState(config)
    state.on_turn_complete(success=True, now=0)
    state.level = "shallow"
    action = state.observe_message(
        chat_type="group",
        text="哈哈",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert action.kind == "none"
    assert action.reason == "short_rule_ignore"


def test_deep_waiting攒满消息数立即仲裁():
    """deep 档 waiting 攒满 N 条新消息立即判，不等到期。"""
    config = TriggerConfig(
        debounce_seconds=5,
        deep_wait_messages=2,
    )
    state = LayeredTriggerState(config)
    state.on_turn_complete(success=True, now=0)
    state.mode = "waiting"
    state.wait_until = 100
    state.level = "deep"
    first = state.observe_message(
        chat_type="group",
        text="第一条",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
    )
    assert first.kind == "none"
    assert first.reason == "wait_collecting"
    assert state.wait_message_count == 1
    second = state.observe_message(
        chat_type="group",
        text="第二条",
        mentioned_self=False,
        has_context=True,
        revision=2,
        now=2,
    )
    assert second.kind == "schedule"
    assert state.debounce_due is not None and state.debounce_due <= 2.0


def test_reply_asks_user与消息回复bot信号():
    """bot 回复以问句/请求收尾时标记 bot_asked；回复 bot 消息升级 deep。"""
    from onebot11.triggers import TriggerConfig

    config = TriggerConfig(debounce_seconds=5, bot_asked_words=("发我", "提供"))
    state = LayeredTriggerState(config)
    # 模拟 adapter 通过 _reply_asks_user 传入 bot_asked=True。
    state.on_turn_complete(success=True, now=0, bot_asked=True)
    assert state.bot_asked is True
    assert state.level == "deep"

    reply_action = state.observe_message(
        chat_type="group",
        text="好的，马上",
        mentioned_self=False,
        has_context=True,
        revision=1,
        now=1,
        user_id="2056963663",
        reply_to_bot=True,
    )
    assert reply_action.kind == "schedule"
    assert state.level == "deep"
