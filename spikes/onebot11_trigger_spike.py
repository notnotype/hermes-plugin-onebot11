"""PROTOTYPE：OneBot 11 分层触发与活跃窗口状态机 spike。

问题：在不让每条群消息都调用 LLM 的前提下，问句、记忆命中和连续对话
能否通过一次低价 LLM 仲裁获得更高的唤醒成功率？本文件只使用虚拟时间和
内存状态，不连接 Hermes、OneBot、SQLite 或真实模型。
"""

from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class LLMResult:
    """模拟旁路 LLM 的严格结构化结果。"""

    decision: str
    wait_seconds: int = 0
    confidence: float = 0.0


@dataclass(frozen=True)
class Message:
    """模拟一条已完成规范化的群消息。"""

    at: int
    user: str
    text: str
    label: str
    mention: bool = False
    memory_score: float = 0.0
    expects_response: bool = False
    llm_result: LLMResult | None = None


@dataclass(frozen=True)
class TriggerPolicy:
    """一组可比较的触发参数；不代表生产配置。"""

    name: str = "balanced"
    keywords: tuple[str, ...] = ("hermes", "机器人")
    question_enabled: bool = True
    memory_threshold: float | None = 0.75
    active_any_message: bool = True
    engaged_idle_seconds: int = 60
    max_engaged_seconds: int = 300
    debounce_seconds: int = 5
    llm_cost_usd: float = 0.0003


@dataclass
class TriggerState:
    """状态机的可观察状态。"""

    clock: int = 0
    mode: str = "idle"
    pending: list[Message] = field(default_factory=list)
    debounce_due: int | None = None
    wait_until: int | None = None
    engaged_until: int | None = None
    engaged_max_until: int | None = None
    llm_calls: int = 0
    llm_failures: int = 0
    direct_turns: int = 0
    llm_turns: int = 0
    turns: int = 0
    false_wake_turns: int = 0
    candidate_messages: int = 0
    ignored_messages: int = 0
    wait_decisions: int = 0
    wait_expired: int = 0
    responded_labels: list[str] = field(default_factory=list)
    seen_expected_labels: list[str] = field(default_factory=list)
    decision_delays: list[int] = field(default_factory=list)
    turn_log: list[dict[str, Any]] = field(default_factory=list)


QUESTION_WORDS = (
    "吗",
    "什么",
    "怎么",
    "为什么",
    "哪个",
    "哪里",
    "如何",
    "能不能",
    "可以吗",
    "请问",
    "帮我",
)


def is_question(text: str) -> bool:
    """用低成本启发式找出常见中文/英文问句候选。"""
    normalized = (text or "").casefold().strip()
    return "?" in normalized or "？" in normalized or any(word in normalized for word in QUESTION_WORDS)


def keyword_matches(text: str, keywords: tuple[str, ...]) -> bool:
    """用 Unicode casefold 做普通子串匹配。"""
    folded = (text or "").casefold()
    return any(keyword.casefold() in folded for keyword in keywords if keyword)


class TriggerSpike:
    """可被脚本和交互壳共同驱动的最小触发状态机。"""

    def __init__(self, policy: TriggerPolicy) -> None:
        """初始化一个没有持久化、没有外部副作用的模拟会话。"""
        self.policy = policy
        self.state = TriggerState()
        self._seen: list[Message] = []

    def message(self, message: Message) -> None:
        """推进到消息时间并处理一条入站消息。"""
        self.advance(message.at)
        self._seen.append(message)
        if message.expects_response and message.label not in self.state.seen_expected_labels:
            self.state.seen_expected_labels.append(message.label)

        hard_reason = self._hard_trigger_reason(message)
        if hard_reason:
            self.state.pending.append(message)
            self._trigger(hard_reason, message.at)
            return

        if self.state.mode == "waiting":
            self.state.pending.append(message)
            self._schedule_debounce(message.at)
            return

        if self.state.mode == "debounce":
            self.state.pending.append(message)
            self._schedule_debounce(message.at)
            return

        if self.state.mode == "engaged":
            if self.policy.active_any_message:
                self.state.pending.append(message)
                self._schedule_debounce(message.at)
            else:
                self._consider_idle_candidate(message)
            return

        self._consider_idle_candidate(message)

    def advance(self, target: int) -> None:
        """推进虚拟时钟并处理到期的 debounce、wait 和活跃窗口。"""
        if target < self.state.clock:
            raise ValueError("虚拟时间必须单调递增")
        while True:
            if self.state.mode == "debounce" and self.state.debounce_due is not None:
                if self.state.debounce_due <= target:
                    self.state.clock = self.state.debounce_due
                    self._run_llm_trigger()
                    continue
            if self.state.mode == "waiting" and self.state.wait_until is not None:
                if self.state.wait_until <= target:
                    self.state.clock = self.state.wait_until
                    self.state.mode = "idle"
                    self.state.wait_until = None
                    self.state.pending.clear()
                    self.state.wait_expired += 1
                    continue
            if self.state.mode == "engaged" and self.state.engaged_until is not None:
                if self.state.engaged_until <= target:
                    self.state.clock = self.state.engaged_until
                    self.state.mode = "idle"
                    self.state.engaged_until = None
                    self.state.engaged_max_until = None
                    continue
            break
        self.state.clock = target

    def snapshot(self) -> dict[str, Any]:
        """返回每次交互后都要完整打印的状态快照。"""
        state = self.state
        expected = set(state.seen_expected_labels)
        responded = set(state.responded_labels)
        return {
            "policy": self.policy.name,
            "clock": state.clock,
            "mode": state.mode,
            "pending": [message.label for message in state.pending],
            "timers": {
                "debounce_due": state.debounce_due,
                "wait_until": state.wait_until,
                "engaged_until": state.engaged_until,
                "engaged_max_until": state.engaged_max_until,
            },
            "counters": {
                "llm_calls": state.llm_calls,
                "llm_failures": state.llm_failures,
                "estimated_cost_usd": round(state.llm_calls * self.policy.llm_cost_usd, 6),
                "direct_turns": state.direct_turns,
                "llm_turns": state.llm_turns,
                "turns": state.turns,
                "candidate_messages": state.candidate_messages,
                "ignored_messages": state.ignored_messages,
                "wait_decisions": state.wait_decisions,
                "wait_expired": state.wait_expired,
                "false_wake_turns": state.false_wake_turns,
            },
            "quality": {
                "expected": sorted(expected),
                "responded": sorted(responded),
                "missed": sorted(expected - responded),
                "continuity_rate": self._rate(len(expected & responded), len(expected)),
                "false_wake_rate": self._rate_or_zero(state.false_wake_turns, state.turns),
                "avg_llm_delay_seconds": self._average(state.decision_delays),
            },
            "turn_log": state.turn_log[-5:],
        }

    def summary(self) -> dict[str, Any]:
        """在所有定时器都结算后返回场景指标。"""
        self.advance(self.state.clock + max(self.policy.engaged_idle_seconds, 60))
        snapshot = self.snapshot()
        return {
            "policy": self.policy.name,
            "llm_calls": self.state.llm_calls,
            "estimated_cost_usd": snapshot["counters"]["estimated_cost_usd"],
            "turns": self.state.turns,
            "direct_turns": self.state.direct_turns,
            "llm_turns": self.state.llm_turns,
            "continuity_rate": snapshot["quality"]["continuity_rate"],
            "false_wake_rate": snapshot["quality"]["false_wake_rate"],
            "missed": snapshot["quality"]["missed"],
            "avg_llm_delay_seconds": snapshot["quality"]["avg_llm_delay_seconds"],
            "llm_failures": self.state.llm_failures,
            "wait_decisions": self.state.wait_decisions,
            "wait_expired": self.state.wait_expired,
        }

    def _hard_trigger_reason(self, message: Message) -> str | None:
        """判断无需 LLM 的硬触发条件。"""
        if message.mention:
            return "mention"
        if keyword_matches(message.text, self.policy.keywords):
            return "keyword"
        if message.text.strip().startswith("/onebot"):
            return "command"
        return None

    def _consider_idle_candidate(self, message: Message) -> None:
        """判断空闲状态的一条消息是否值得消耗一次 LLM 仲裁。"""
        question = self.policy.question_enabled and is_question(message.text)
        memory = self.policy.memory_threshold is not None and message.memory_score >= self.policy.memory_threshold
        if not question and not memory:
            self.state.ignored_messages += 1
            return
        self.state.candidate_messages += 1
        self.state.pending.append(message)
        self._schedule_debounce(message.at)

    def _schedule_debounce(self, at: int) -> None:
        """将候选消息合并到一个 trailing debounce 窗口。"""
        self.state.mode = "debounce"
        self.state.debounce_due = at + self.policy.debounce_seconds
        self.state.wait_until = None

    def _run_llm_trigger(self) -> None:
        """执行一次模拟仲裁，非法/失败结果统一按 ignore 处理。"""
        state = self.state
        state.llm_calls += 1
        state.debounce_due = None
        if state.pending:
            delay = state.clock - min(message.at for message in state.pending)
            state.decision_delays.append(delay)
        result = state.pending[-1].llm_result if state.pending else None
        if result is None or result.decision not in {"trigger", "wait", "ignore"}:
            state.llm_failures += 1
            state.mode = "idle"
            state.pending.clear()
            return
        if result.decision == "trigger":
            self._trigger("llm", state.clock)
            return
        if result.decision == "wait":
            if result.wait_seconds not in {5, 10, 30, 60}:
                state.llm_failures += 1
                state.mode = "idle"
                state.pending.clear()
                return
            state.mode = "waiting"
            state.wait_until = state.clock + result.wait_seconds
            state.wait_decisions += 1
            return
        state.mode = "idle"
        state.pending.clear()

    def _trigger(self, reason: str, at: int) -> None:
        """完成一次模拟 Agent turn，并打开或续期活跃窗口。"""
        state = self.state
        batch = list(state.pending)
        state.pending.clear()
        state.debounce_due = None
        state.wait_until = None
        state.turns += 1
        if reason == "llm":
            state.llm_turns += 1
        else:
            state.direct_turns += 1
        expected_in_batch = [message.label for message in batch if message.expects_response]
        state.responded_labels.extend(label for label in expected_in_batch if label not in state.responded_labels)
        if not expected_in_batch:
            state.false_wake_turns += 1
        state.turn_log.append({"at": at, "reason": reason, "messages": [message.label for message in batch]})
        if state.engaged_max_until is None:
            state.engaged_max_until = at + self.policy.max_engaged_seconds
        state.engaged_until = min(
            at + self.policy.engaged_idle_seconds,
            state.engaged_max_until,
        )
        state.mode = "engaged"

    @staticmethod
    def _rate(numerator: int, denominator: int) -> float:
        """计算带空分母保护的比例。"""
        return round(numerator / denominator, 3) if denominator else 1.0

    @staticmethod
    def _average(values: list[int]) -> float:
        """计算决策延迟平均值。"""
        return round(sum(values) / len(values), 2) if values else 0.0

    @staticmethod
    def _rate_or_zero(numerator: int, denominator: int) -> float:
        """计算事件比例；没有发生事件时误唤醒率应为零。"""
        return round(numerator / denominator, 3) if denominator else 0.0


def scenarios() -> dict[str, list[Message]]:
    """返回固定输入，保证不同策略的比较使用完全相同的事件。"""
    return {
        "普通群聊": [
            Message(0, "alice", "今天有人讨论电影", "闲聊-1"),
            Message(8, "bob", "我觉得还不错", "闲聊-2"),
        ],
        "硬触发": [
            Message(0, "alice", "@bot 帮我总结上一段", "mention", mention=True, expects_response=True),
            Message(10, "bob", "Hermes 看一下这个", "keyword", expects_response=True),
        ],
        "问句候选": [
            Message(0, "alice", "这个配置怎么改？", "question", expects_response=True, llm_result=LLMResult("trigger", confidence=0.92)),
        ],
        "非目标问句": [
            Message(0, "alice", "今晚吃什么？", "false-question", llm_result=LLMResult("trigger", confidence=0.58)),
        ],
        "记忆候选": [
            Message(0, "alice", "之前那个部署问题", "memory", memory_score=0.86, llm_result=LLMResult("ignore", confidence=0.61)),
            Message(12, "alice", "具体应该怎么改？", "memory-followup", expects_response=True, llm_result=LLMResult("trigger", confidence=0.91)),
        ],
        "wait 合并": [
            Message(0, "alice", "我有个问题？", "wait-1", llm_result=LLMResult("wait", wait_seconds=10, confidence=0.55)),
            Message(7, "alice", "是关于第二个配置的", "wait-2", expects_response=True, llm_result=LLMResult("trigger", confidence=0.89)),
        ],
        "wait 到期": [
            Message(0, "alice", "我有个问题？", "wait-expire", llm_result=LLMResult("wait", wait_seconds=10, confidence=0.55)),
        ],
        "活跃连续对话": [
            Message(0, "alice", "@bot 帮我列三个方案", "active-start", mention=True, expects_response=True),
            Message(20, "alice", "那第二个呢", "active-followup-1", expects_response=True, llm_result=LLMResult("trigger", confidence=0.83)),
            Message(22, "bob", "还有第三个吗", "active-followup-2", expects_response=True, llm_result=LLMResult("trigger", confidence=0.87)),
        ],
        "模型失败": [
            Message(0, "alice", "为什么会报错？", "llm-failure", expects_response=True, llm_result=LLMResult("invalid")),
        ],
        "活跃超时": [
            Message(0, "alice", "@bot 先回答这个", "active-timeout-start", mention=True, expects_response=True),
            Message(61, "bob", "继续闲聊", "after-timeout"),
        ],
    }


def policies(cost: float) -> dict[str, TriggerPolicy]:
    """返回成本/连续性取舍的三个可比配置。"""
    return {
        "conservative": TriggerPolicy(
            name="conservative",
            question_enabled=True,
            memory_threshold=None,
            active_any_message=False,
            llm_cost_usd=cost,
        ),
        "balanced": TriggerPolicy(name="balanced", llm_cost_usd=cost),
        "high-recall": TriggerPolicy(
            name="high-recall",
            memory_threshold=0.55,
            engaged_idle_seconds=90,
            max_engaged_seconds=300,
            llm_cost_usd=cost,
        ),
    }


def run_one(name: str, policy: TriggerPolicy, verbose: bool = True) -> dict[str, Any]:
    """运行一个场景并按输入打印状态。"""
    engine = TriggerSpike(policy)
    if verbose:
        print(f"\n=== {name} / {policy.name} ===")
    for message in scenarios()[name]:
        engine.message(message)
        if verbose:
            print(json.dumps(engine.snapshot(), ensure_ascii=False, indent=2))
    result = engine.summary()
    if verbose:
        print("--- summary ---")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def run_all(cost: float) -> None:
    """运行全部场景，再比较三个策略的总指标。"""
    for name in scenarios():
        run_one(name, policies(cost)["balanced"])
    print("\n=== policy comparison / all scenarios ===")
    for policy in policies(cost).values():
        results = [run_one(name, policy, verbose=False) for name in scenarios()]
        missed = sum(len(result["missed"]) for result in results)
        expected = sum(
            message.expects_response
            for messages in scenarios().values()
            for message in messages
        )
        turns = sum(result["turns"] for result in results)
        print(json.dumps({
            "policy": policy.name,
            "llm_calls": sum(result["llm_calls"] for result in results),
            "estimated_cost_usd": round(sum(result["estimated_cost_usd"] for result in results), 6),
            "turns": turns,
            "missed_count": missed,
            "expected_count": expected,
            "continuity_rate": round(1 - missed / expected, 3) if expected else 1.0,
            "false_wake_rate": round(
                sum(result["false_wake_rate"] * result["turns"] for result in results) / turns, 3
            ) if turns else 0.0,
        }, ensure_ascii=False))


def interactive(policy: TriggerPolicy) -> None:
    """启动最小终端交互壳，每次命令后打印完整状态。"""
    engine = TriggerSpike(policy)
    print("PROTOTYPE；输入 message <秒> <用户> <文本>，tick <秒> 或 q。")
    print(json.dumps(engine.snapshot(), ensure_ascii=False, indent=2))
    while True:
        try:
            raw = input("> ").strip()
        except EOFError:
            return
        if raw.lower() in {"q", "quit", "exit"}:
            return
        parts = shlex.split(raw)
        if not parts:
            continue
        if parts[0] == "tick" and len(parts) == 2:
            engine.advance(int(parts[1]))
        elif parts[0] == "message" and len(parts) >= 4:
            at = int(parts[1])
            user = parts[2]
            text = " ".join(parts[3:])
            engine.message(Message(
                at,
                user,
                text,
                f"interactive-{len(engine._seen) + 1}",
                mention="@bot" in text.casefold(),
                expects_response="@bot" in text.casefold() or is_question(text),
                llm_result=LLMResult("trigger") if is_question(text) else None,
            ))
        else:
            print("命令格式: message <秒> <用户> <文本> | tick <秒> | q")
            continue
        print(json.dumps(engine.snapshot(), ensure_ascii=False, indent=2))


def main() -> None:
    """解析命令行参数并运行 spike。"""
    parser = argparse.ArgumentParser(description="PROTOTYPE: OneBot11 trigger strategy spike")
    parser.add_argument("--scenario", choices=[*scenarios(), "all"], default="all")
    parser.add_argument("--interactive", action="store_true")
    parser.add_argument("--llm-cost-usd", type=float, default=0.0003)
    args = parser.parse_args()
    if args.llm_cost_usd < 0:
        raise SystemExit("--llm-cost-usd 不能为负数")
    if args.interactive:
        interactive(policies(args.llm_cost_usd)["balanced"])
    elif args.scenario == "all":
        run_all(args.llm_cost_usd)
    else:
        run_one(args.scenario, policies(args.llm_cost_usd)["balanced"])


if __name__ == "__main__":
    main()
