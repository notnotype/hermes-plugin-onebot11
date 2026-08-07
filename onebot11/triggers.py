"""OneBot 11 触发规则。

``should_trigger`` 只做确定性判断；旁路 LLM 判断由 adapter 负责调度和持久化，
这样关键词、@ 和兼容模式可以在没有 Hermes 的情况下完整测试。
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from time import monotonic
from typing import Any

from .permissions import parse_bool
from .queue import QueueMessage


@dataclass(frozen=True)
class TriggerConfig:
    """一个平台实例的确定性触发配置。"""

    require_mention: bool = True
    keywords: tuple[str, ...] = ()
    always: bool = False
    cooldown_seconds: float = 0.0
    llm_enabled: bool = False
    llm_provider: str = ""
    llm_model: str = ""
    llm_timeout_seconds: float = 10.0
    llm_input_bytes: int = 12_000
    llm_concurrency: int = 2
    llm_allowed_groups: frozenset[str] = frozenset()
    question_enabled: bool = True
    memory_enabled: bool = True
    memory_words: tuple[str, ...] = (
        "之前",
        "上次",
        "刚才",
        "那个",
        "继续",
        "接着",
    )
    debounce_seconds: float = 5.0
    engaged_idle_seconds: float = 60.0
    engaged_max_seconds: float = 300.0
    engaged_max_arbitrations: int = 3


@dataclass(frozen=True)
class TriggerDecision:
    """触发判断结果和原因，便于日志和测试。"""

    triggered: bool
    reason: str


@dataclass(frozen=True)
class TriggerAction:
    """分层触发状态机交给 adapter 的无副作用动作。"""

    kind: str
    reason: str = ""
    candidate_type: str = ""
    due_at: float | None = None
    revision: int | None = None
    generation: int | None = None


@dataclass
class LayeredTriggerState:
    """一个群的内存触发状态；重启后由 adapter 重新创建为 idle。"""

    config: TriggerConfig
    mode: str = "idle"
    debounce_due: float | None = None
    wait_until: float | None = None
    engaged_until: float | None = None
    engaged_max_until: float | None = None
    arbitration_count: int = 0
    llm_calls: int = 0
    llm_failures: int = 0
    last_candidate_type: str = ""
    dirty_revision: int | None = None
    _candidate_type: str = field(default="", repr=False)
    _judgement_generation: int = field(default=0, repr=False)

    def observe_message(
        self,
        *,
        chat_type: str,
        text: str,
        mentioned_self: bool,
        has_context: bool,
        revision: int,
        now: float,
        last_trigger_at: float | None = None,
    ) -> TriggerAction:
        """观察一条已入队消息，返回直接触发或安排仲裁的动作。"""
        hard = should_trigger(
            chat_type=chat_type,
            text=text,
            mentioned_self=mentioned_self,
            config=self.config,
            last_trigger_at=last_trigger_at,
            now=now,
        )
        if hard.reason == "cooldown":
            return TriggerAction("none", reason="cooldown")
        if hard.triggered:
            self._clear_pending_judgement()
            return TriggerAction("direct", reason=hard.reason)
        if chat_type != "group":
            return TriggerAction("none", reason=hard.reason)

        if self.mode == "judging":
            self.dirty_revision = revision
            return TriggerAction("none", reason="judging_dirty")

        if self.mode == "engaged":
            if self.engaged_until is not None and now >= self.engaged_until:
                self._leave_engaged()
            elif self.arbitration_count >= self.config.engaged_max_arbitrations:
                return TriggerAction("none", reason="arbitration_limit")
            else:
                return self._schedule("engaged", revision, now)

        if self.mode == "waiting":
            if self.arbitration_count >= self.config.engaged_max_arbitrations:
                return TriggerAction("none", reason="arbitration_limit")
            return self._schedule("wait_followup", revision, now)

        if self.mode == "debounce":
            return self._schedule(self._candidate_type or "candidate", revision, now)

        candidate_type = self._candidate_type_for(text, has_context)
        if not candidate_type:
            return TriggerAction("none", reason="non_candidate")
        return self._schedule(candidate_type, revision, now)

    def on_timer(self, *, now: float) -> TriggerAction:
        """处理 debounce、wait 和 engaged 的到期事件。"""
        if self.mode == "engaged" and self.engaged_until is not None and now >= self.engaged_until:
            self._leave_engaged()
            return TriggerAction("none", reason="engaged_expired")
        if self.mode == "waiting" and self.wait_until is not None and now >= self.wait_until:
            if (
                self.engaged_until is not None
                and self.engaged_until > now
                and self.engaged_max_until is not None
                and self.engaged_max_until > now
            ):
                self.mode = "engaged"
                self.wait_until = None
                self._candidate_type = ""
                self.dirty_revision = None
            else:
                self._leave_engaged()
            return TriggerAction("none", reason="wait_expired")
        if self.mode == "debounce" and self.debounce_due is not None and now >= self.debounce_due:
            return self._begin_judgement()
        return TriggerAction("none", reason="timer_not_due")

    def on_llm_result(
        self,
        *,
        decision: str,
        wait_seconds: int,
        observed_revision: int,
        current_revision: int,
        now: float,
        generation: int | None = None,
    ) -> TriggerAction:
        """应用严格的 LLM 结果；dirty 队列优先重新安排一次仲裁。"""
        if self.mode != "judging" or (
            generation is not None and generation != self._judgement_generation
        ):
            return TriggerAction("none", reason="stale_judgement")
        self.debounce_due = None
        if observed_revision != current_revision:
            self.mode = "debounce"
            self._candidate_type = self._candidate_type or "dirty"
            self.debounce_due = now + self.config.debounce_seconds
            self.dirty_revision = current_revision
            return TriggerAction(
                "schedule",
                reason="queue_dirty",
                candidate_type=self._candidate_type,
                due_at=self.debounce_due,
                revision=current_revision,
                generation=self._judgement_generation,
            )
        if decision == "trigger":
            self.mode = "idle"
            self._candidate_type = ""
            self.dirty_revision = None
            return TriggerAction(
                "direct", reason="llm", generation=self._judgement_generation
            )
        if decision == "wait" and wait_seconds in {5, 10, 30, 60}:
            self.mode = "waiting"
            self.wait_until = now + wait_seconds
            self._candidate_type = ""
            self.dirty_revision = None
            return TriggerAction("wait", reason="llm_wait", due_at=self.wait_until)
        if decision not in {"ignore", "wait"} or (
            decision == "wait" and wait_seconds not in {5, 10, 30, 60}
        ):
            self.llm_failures += 1
        self._return_to_engaged_or_idle(now)
        self.wait_until = None
        self._candidate_type = ""
        self.dirty_revision = None
        return TriggerAction("none", reason="llm_ignore" if decision == "ignore" else "invalid_result")

    def on_llm_failure(
        self,
        *,
        now: float,
        current_revision: int,
        generation: int | None = None,
    ) -> TriggerAction:
        """把超时、模型缺失和非法响应作为 ignore，并保留新消息。"""
        if generation is not None and not self.judgement_is_current(generation):
            return TriggerAction("none", reason="stale_judgement")
        self.llm_failures += 1
        if self.mode in {"judging", "debounce"}:
            if self.mode == "judging" and self.dirty_revision is not None and current_revision >= self.dirty_revision:
                return self._schedule(
                    self._candidate_type or "candidate",
                    current_revision,
                    now,
                )
            self._return_to_engaged_or_idle(now)
            self.debounce_due = None
            self.wait_until = None
            self._candidate_type = ""
            self.dirty_revision = None
        return TriggerAction("none", reason="llm_failure")

    def pause(self) -> None:
        """暂停群级自动触发，并让未完成旁路判断失效。"""
        self._judgement_generation += 1
        self.mode = "idle"
        self.debounce_due = None
        self.wait_until = None
        self.engaged_until = None
        self.engaged_max_until = None
        self.arbitration_count = 0
        self._candidate_type = ""
        self.dirty_revision = None

    def judgement_is_current(self, generation: int | None) -> bool:
        """判断旁路结果是否仍属于当前群的这次仲裁。"""
        return (
            generation is not None
            and self.mode == "judging"
            and generation == self._judgement_generation
        )

    def invalidate_judgement(self) -> None:
        """使已经离开当前状态机的旁路结果永久失效并回到 idle。"""
        self._judgement_generation += 1
        self.mode = "idle"
        self.debounce_due = None
        self.wait_until = None
        self._candidate_type = ""
        self.dirty_revision = None

    def generation_matches(self, generation: int | None) -> bool:
        """只校验 generation 坐标，不要求状态仍处于 judging。"""
        return generation is not None and generation == self._judgement_generation

    def on_turn_complete(
        self,
        *,
        success: bool,
        now: float,
        preserve_pending: bool = False,
        has_hard_trigger: bool = False,
    ) -> None:
        """成功 Agent turn 进入 engaged；有新消息时不覆盖其待处理状态。"""
        if preserve_pending and (
            not success
            or has_hard_trigger
            or self.mode in {"debounce", "judging", "waiting"}
        ):
            return
        self._judgement_generation += 1
        self.debounce_due = None
        self.wait_until = None
        self._candidate_type = ""
        self.dirty_revision = None
        if not success:
            self._leave_engaged()
            return
        if self.engaged_max_until is None or self.engaged_max_until <= now:
            self.engaged_max_until = now + self.config.engaged_max_seconds
            self.arbitration_count = 0
        self.engaged_until = min(
            now + self.config.engaged_idle_seconds,
            self.engaged_max_until,
        )
        self.mode = "engaged"

    def snapshot(self) -> dict[str, Any]:
        """返回 status 和审计需要的有限状态，不包含消息正文。"""
        return {
            "mode": self.mode,
            "debounce_due": self.debounce_due,
            "wait_until": self.wait_until,
            "engaged_until": self.engaged_until,
            "engaged_max_until": self.engaged_max_until,
            "arbitration_count": self.arbitration_count,
            "llm_calls": self.llm_calls,
            "llm_failures": self.llm_failures,
            "last_candidate_type": self.last_candidate_type,
        }

    def _candidate_type_for(self, text: str, has_context: bool) -> str:
        """按 balanced 策略识别问句和有上下文的记忆回指。"""
        if self.config.question_enabled and is_question(text):
            return "question"
        if self.config.memory_enabled and has_context and memory_matches(
            text, self.config.memory_words
        ):
            return "memory"
        return ""

    def _schedule(self, candidate_type: str, revision: int, now: float) -> TriggerAction:
        """安排或重置 trailing debounce，不创建 lease。"""
        self.mode = "debounce"
        self.debounce_due = now + self.config.debounce_seconds
        self.wait_until = None
        self._candidate_type = candidate_type
        self.last_candidate_type = candidate_type
        self.dirty_revision = revision
        return TriggerAction(
            "schedule",
            reason="candidate",
            candidate_type=candidate_type,
            due_at=self.debounce_due,
            revision=revision,
        )

    def _begin_judgement(self) -> TriggerAction:
        """把到期的 debounce 转成一个唯一的 LLM judgement。"""
        self._judgement_generation += 1
        generation = self._judgement_generation
        self.mode = "judging"
        self.debounce_due = None
        self.arbitration_count += 1
        self.llm_calls += 1
        observed_revision = self.dirty_revision
        self.dirty_revision = None
        return TriggerAction(
            "judge",
            reason="debounce_due",
            candidate_type=self._candidate_type or "candidate",
            revision=observed_revision,
            generation=generation,
        )

    def _clear_pending_judgement(self) -> None:
        """硬触发优先，令旧的 debounce/judgement 结果失效。"""
        self._judgement_generation += 1
        self.mode = "idle"
        self.debounce_due = None
        self.wait_until = None
        self._candidate_type = ""
        self.dirty_revision = None

    def _leave_engaged(self) -> None:
        """离开活跃窗口并重置其仲裁预算。"""
        self.mode = "idle"
        self.engaged_until = None
        self.engaged_max_until = None
        self.arbitration_count = 0

    def _return_to_engaged_or_idle(self, now: float) -> None:
        """仲裁不触发时保留尚未到期的活跃窗口。"""
        if (
            self.engaged_until is not None
            and self.engaged_until > now
            and self.engaged_max_until is not None
            and self.engaged_max_until > now
        ):
            self.mode = "engaged"
        else:
            self._leave_engaged()


def _parse_keywords(value: Any) -> tuple[str, ...]:
    """将字符串/YAML list 规范化为 casefold 后的非空关键词。"""
    values: Iterable[Any]
    if value is None:
        values = ()
    elif isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        raise ValueError("trigger_keywords 必须是字符串或 YAML list")
    normalized: list[str] = []
    for item in values:
        if item is None:
            continue
        text = str(item).strip().casefold()
        if text:
            normalized.append(text)
    return tuple(dict.fromkeys(normalized))


def _parse_group_ids(value: Any) -> frozenset[str]:
    """解析 LLM trigger 的显式群 allowlist。"""
    if value is None:
        return frozenset()
    values = value.split(",") if isinstance(value, str) else value
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise ValueError("llm_trigger_groups 必须是字符串或 YAML list")
    return frozenset(str(item).strip() for item in values if str(item).strip())


def _setting(extra: Mapping[str, Any], nested: Mapping[str, Any], name: str, default: Any = None) -> Any:
    """读取扁平配置优先、嵌套配置兜底的触发器设置。"""
    return extra[name] if name in extra else nested.get(name, default)


def _bounded_float(value: Any, *, name: str, minimum: float, maximum: float | None = None) -> float:
    """解析有限浮点数，并拒绝越界值而不是静默夹紧。"""
    if isinstance(value, bool):
        raise ValueError(f"{name} 必须是数字")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是数字") from exc
    if not math.isfinite(parsed) or parsed < minimum or (maximum is not None and parsed > maximum):
        bound = f"{minimum} 至 {maximum}" if maximum is not None else f"不小于 {minimum}"
        raise ValueError(f"{name} 必须是有限数字，范围为 {bound}")
    return parsed


def _bounded_int(value: Any, *, name: str, minimum: int, maximum: int) -> int:
    """解析整数配置，拒绝布尔值、非整数和越界值。"""
    if isinstance(value, bool):
        raise ValueError(f"{name} 必须是整数")
    try:
        parsed_float = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是整数") from exc
    if not math.isfinite(parsed_float) or not parsed_float.is_integer():
        raise ValueError(f"{name} 必须是整数")
    parsed = int(parsed_float)
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} 必须在 {minimum} 至 {maximum} 之间")
    return parsed


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
ENGLISH_QUESTION_RE = re.compile(
    r"\b(?:who|what|when|where|why|how|can|could|would|should|do|does|did|is|are|will)\b",
    re.IGNORECASE,
)


def is_question(text: str) -> bool:
    """用低成本启发式识别中文疑问词、问号和英文问句。"""
    normalized = (text or "").casefold().strip()
    return bool(
        "?" in normalized
        or "？" in normalized
        or any(word in normalized for word in QUESTION_WORDS)
        or ENGLISH_QUESTION_RE.search(normalized)
    )


def memory_matches(text: str, words: Iterable[str]) -> bool:
    """识别带有回指词的记忆候选，不执行外部检索。"""
    folded = (text or "").casefold()
    return any(str(word).casefold() in folded for word in words if str(word).strip())


def parse_llm_decision(value: Any) -> tuple[str, int] | None:
    """严格解析旁路模型的三态 JSON 结果。"""
    if not isinstance(value, Mapping):
        return None
    if set(value) != {"decision", "wait_seconds"}:
        return None
    decision = value.get("decision")
    wait_seconds = value.get("wait_seconds")
    if decision not in {"trigger", "wait", "ignore"} or type(wait_seconds) is not int:
        return None
    if decision == "wait" and wait_seconds not in {5, 10, 30, 60}:
        return None
    if decision != "wait" and wait_seconds != 0:
        return None
    return str(decision), wait_seconds


def build_trigger_config(extra: dict[str, Any]) -> TriggerConfig:
    """从 extra 读取触发配置并严格解析布尔值。"""
    raw_cooldown = extra.get("trigger_cooldown_seconds", 0)
    cooldown = _bounded_float(
        raw_cooldown,
        name="trigger_cooldown_seconds",
        minimum=0.0,
    )
    raw_llm = extra.get("llm_trigger")
    if raw_llm is None:
        raw_llm = extra.get("trigger_llm")
    if raw_llm is None:
        raw_llm = {}
    if not isinstance(raw_llm, Mapping):
        raise ValueError("llm_trigger 必须是 YAML mapping")
    llm_enabled = parse_bool(
        _setting(extra, raw_llm, "llm_trigger_enabled", raw_llm.get("enabled")),
        default=False,
        name="llm_trigger_enabled",
    )
    raw_provider = _setting(
        extra,
        raw_llm,
        "llm_trigger_provider",
        raw_llm.get("provider", ""),
    )
    raw_model = _setting(
        extra,
        raw_llm,
        "llm_trigger_model",
        raw_llm.get("model", ""),
    )
    if raw_provider is None:
        raw_provider = ""
    if raw_model is None:
        raw_model = ""
    if not isinstance(raw_provider, str) or not isinstance(raw_model, str):
        raise ValueError("llm_trigger provider/model 必须是字符串")
    provider = raw_provider.strip()
    model = raw_model.strip()
    if llm_enabled and (not provider or not model):
        raise ValueError("启用 LLM trigger 时必须配置 provider 和 model")
    allowed_groups = _parse_group_ids(
        _setting(extra, raw_llm, "llm_trigger_groups", raw_llm.get("groups"))
    )
    timeout = _bounded_float(
        _setting(extra, raw_llm, "llm_trigger_timeout_seconds", raw_llm.get("timeout", 10)),
        name="llm_trigger_timeout_seconds",
        minimum=0.1,
        maximum=300.0,
    )
    input_bytes = _bounded_int(
        _setting(extra, raw_llm, "llm_trigger_input_bytes", raw_llm.get("input_bytes", 12_000)),
        name="llm_trigger_input_bytes",
        minimum=512,
        maximum=64_000,
    )
    concurrency = _bounded_int(
        _setting(extra, raw_llm, "llm_trigger_concurrency", raw_llm.get("concurrency", 2)),
        name="llm_trigger_concurrency",
        minimum=1,
        maximum=32,
    )
    debounce = _bounded_float(
        _setting(extra, raw_llm, "trigger_debounce_seconds", 5),
        name="trigger_debounce_seconds",
        minimum=0.1,
        maximum=60.0,
    )
    engaged_idle = _bounded_float(
        _setting(extra, raw_llm, "engaged_idle_seconds", 60),
        name="engaged_idle_seconds",
        minimum=1.0,
        maximum=3600.0,
    )
    engaged_max = _bounded_float(
        _setting(extra, raw_llm, "engaged_max_seconds", 300),
        name="engaged_max_seconds",
        minimum=engaged_idle,
        maximum=86_400.0,
    )
    engaged_arbitrations = _bounded_int(
        _setting(extra, raw_llm, "engaged_max_arbitrations", 3),
        name="engaged_max_arbitrations",
        minimum=0,
        maximum=32,
    )
    question_enabled = parse_bool(
        _setting(extra, raw_llm, "question_trigger_enabled", True),
        default=True,
        name="question_trigger_enabled",
    )
    memory_enabled = parse_bool(
        _setting(extra, raw_llm, "memory_trigger_enabled", True),
        default=True,
        name="memory_trigger_enabled",
    )
    memory_words = _parse_keywords(
        _setting(
            extra,
            raw_llm,
            "memory_trigger_words",
            ("之前", "上次", "刚才", "那个", "继续", "接着"),
        )
    )
    return TriggerConfig(
        require_mention=parse_bool(extra.get("require_mention"), default=True, name="require_mention"),
        keywords=_parse_keywords(extra.get("trigger_keywords", extra.get("keywords"))),
        always=parse_bool(extra.get("always_trigger"), default=False, name="always_trigger"),
        cooldown_seconds=cooldown,
        llm_enabled=llm_enabled,
        llm_provider=provider,
        llm_model=model,
        llm_timeout_seconds=timeout,
        llm_input_bytes=input_bytes,
        llm_concurrency=concurrency,
        llm_allowed_groups=allowed_groups,
        question_enabled=question_enabled,
        memory_enabled=memory_enabled,
        memory_words=memory_words,
        debounce_seconds=debounce,
        engaged_idle_seconds=engaged_idle,
        engaged_max_seconds=engaged_max,
        engaged_max_arbitrations=engaged_arbitrations,
    )


def build_llm_trigger_input(
    summary: str,
    messages: Iterable[QueueMessage],
    max_bytes: int,
    candidate_type: str = "candidate",
) -> str:
    """按字节预算拼接输入，始终保留 JSON 合同和最新消息。"""
    max_bytes = max(1, int(max_bytes))
    queued = tuple(messages)
    latest = queued[-1] if queued else None
    contract = "\n".join(
        (
            "判断当前 OneBot11 群消息是否应该让机器人回复。",
            f"候选类型：{candidate_type}。请结合历史摘要和当前队列判断。",
            "只输出严格 JSON。合法结果示例：",
            '{"decision":"trigger","wait_seconds":0}',
            '{"decision":"wait","wait_seconds":5}',
            '{"decision":"ignore","wait_seconds":0}',
            "decision=wait 时 wait_seconds 只能是 5、10、30、60；trigger/ignore 使用 0。",
        )
    )
    queue_prefix = contract + "\n当前队列：\n"
    latest_line = (
        f"#{latest.seq or '?'} [{latest.user_name}] {latest.text}"
        if latest is not None
        else "（当前没有消息）"
    )

    def byte_len(value: str) -> int:
        """返回 UTF-8 字节长度。"""
        return len(value.encode("utf-8"))

    def truncate(value: str, limit: int) -> str:
        """按 UTF-8 字节保留字符串开头，避免破坏字符。"""
        return value.encode("utf-8")[: max(0, limit)].decode("utf-8", errors="ignore")

    optional: list[str] = []
    if summary:
        optional.append(f"历史摘要：{summary}")
    optional.extend(
        f"#{message.seq or '?'} [{message.user_name}] {message.text}"
        for message in queued[:-1]
    )

    # 配置下限足以容纳完整 JSON 合同时，优先保留合同，再裁剪最新消息
    # 正文；只有调用方传入小于合同本身的测试预算时才退化为最新消息。
    latest_bytes = byte_len(latest_line)
    prefix_bytes = byte_len(queue_prefix)
    if prefix_bytes >= max_bytes:
        return truncate(latest_line, max_bytes)
    latest_budget = max_bytes - prefix_bytes
    if latest_bytes > latest_budget:
        return queue_prefix + truncate(latest_line, latest_budget)
    lead_budget = max_bytes - latest_bytes

    available = lead_budget - prefix_bytes
    selected_newest_first: list[str] = []
    used = 0
    for line in reversed(optional):
        line_bytes = byte_len(line) + 1
        if used + line_bytes > available:
            break
        selected_newest_first.append(line)
        used += line_bytes
    selected = list(reversed(selected_newest_first))
    omitted = len(optional) - len(selected)
    if omitted:
        marker = f"[已省略 {omitted} 条更早上下文]"
        marker_bytes = byte_len(marker) + 1
        while selected and used + marker_bytes > available:
            removed = selected.pop(0)
            used -= byte_len(removed) + 1
        if used + marker_bytes <= available:
            selected.insert(0, marker)
            used += marker_bytes
    optional_text = ("\n".join(selected) + "\n") if selected else ""
    result = queue_prefix + optional_text + latest_line
    if byte_len(result) <= max_bytes:
        return result
    # 这是最后一道合同保护：无论调用方传入多小的预算，都不能
    # 返回超限输入；尾部包含完整或受限的最新消息。
    return truncate(queue_prefix, max(0, max_bytes - latest_bytes - 1)) + "\n" + latest_line


def keyword_matches(text: str, keywords: Iterable[str]) -> bool:
    """使用 Unicode casefold 的普通子串匹配关键词。"""
    folded = (text or "").casefold()
    return any(keyword.casefold() in folded for keyword in keywords if keyword)


def should_trigger(
    *,
    chat_type: str,
    text: str,
    mentioned_self: bool,
    config: TriggerConfig,
    last_trigger_at: float | None = None,
    now: float | None = None,
) -> TriggerDecision:
    """判断当前消息是否应发起一个 Hermes turn。"""
    if chat_type == "dm":
        reason = "private_message"
    elif config.always or not config.require_mention:
        reason = "always"
    elif mentioned_self:
        reason = "mention"
    elif keyword_matches(text, config.keywords):
        reason = "keyword"
    else:
        return TriggerDecision(False, "no_trigger")
    if config.cooldown_seconds > 0 and last_trigger_at is not None:
        current = monotonic() if now is None else now
        if current - last_trigger_at < config.cooldown_seconds:
            return TriggerDecision(False, "cooldown")
    return TriggerDecision(True, reason)
