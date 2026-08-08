"""OneBot 11 精确触发规则与自动 TurnAnchor 选择合同。"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from time import time as wall_clock
from typing import Any, Literal, Protocol

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


@dataclass(frozen=True)
class TriggerDecision:
    """触发判断结果和原因，便于日志和测试。"""

    triggered: bool
    reason: str
    explicit: bool = False

    @property
    def creates_message_anchor(self) -> bool:
        """只有明确指向机器人的规则才能直接继承发送者 authority。"""
        return self.triggered and self.explicit


@dataclass(frozen=True)
class TriggerMessageSnapshot:
    """自动锚点选择器可见的最小消息投影，不携带角色或工具配置。"""

    seq: int
    user_id: str
    user_name: str
    text: str
    reply_to_message_id: str | None = None
    markers: tuple[str, ...] = ()


@dataclass(frozen=True)
class TriggerSnapshot:
    """一次自动锚点判断使用的有序、只读消息快照。"""

    chat_id: str
    messages: tuple[TriggerMessageSnapshot, ...]


@dataclass(frozen=True)
class AnchorSelectorPrompt:
    """selector prompt 文本及模型实际看到的最大消息序号。"""

    text: str
    visible_max_seq: int | None


AnchorReasonCode = Literal["automatic_request", "no_request"]
SelectorScheduleReason = Literal["question", "reply", "active_window"]


@dataclass(frozen=True)
class AnchorDecision:
    """自动 evaluator 的受限输出；权限始终由真实锚点消息决定。"""

    anchor_seq: int | None
    reason_code: AnchorReasonCode

    def __post_init__(self) -> None:
        """拒绝无效序号和互相矛盾的固定原因。"""
        if self.anchor_seq is not None and (
            type(self.anchor_seq) is not int or self.anchor_seq <= 0
        ):
            raise ValueError("anchor_seq 必须是正整数或 null")
        expected = "no_request" if self.anchor_seq is None else "automatic_request"
        if self.reason_code != expected:
            raise ValueError("anchor_seq 与 reason_code 不匹配")


class TriggerEvaluator(Protocol):
    """未来 LLM、语义或记忆 evaluator 共用的最小异步合同。"""

    async def evaluate(self, snapshot: TriggerSnapshot) -> AnchorDecision:
        """从快照中选择至多一个锚点。"""
        ...


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


def build_trigger_config(extra: dict[str, Any]) -> TriggerConfig:
    """从 extra 读取触发配置并严格解析布尔值。"""
    raw_cooldown = extra.get("trigger_cooldown_seconds", 0)
    try:
        cooldown = max(0.0, float(raw_cooldown))
    except (TypeError, ValueError) as exc:
        raise ValueError("trigger_cooldown_seconds 必须是数字") from exc
    raw_llm = extra.get("llm_trigger") or extra.get("trigger_llm") or {}
    if not isinstance(raw_llm, Mapping):
        raise ValueError("llm_trigger 必须是 YAML mapping")
    llm_enabled = parse_bool(
        _setting(extra, raw_llm, "llm_trigger_enabled", raw_llm.get("enabled")),
        default=False,
        name="llm_trigger_enabled",
    )
    provider = str(_setting(extra, raw_llm, "llm_trigger_provider", raw_llm.get("provider", "")) or "").strip()
    model = str(_setting(extra, raw_llm, "llm_trigger_model", raw_llm.get("model", "")) or "").strip()
    allowed_groups = _parse_group_ids(
        _setting(extra, raw_llm, "llm_trigger_groups", raw_llm.get("groups"))
    )
    try:
        timeout = max(0.1, min(300.0, float(_setting(extra, raw_llm, "llm_trigger_timeout_seconds", raw_llm.get("timeout", 10)))))
        input_bytes = max(512, min(64_000, int(_setting(extra, raw_llm, "llm_trigger_input_bytes", raw_llm.get("input_bytes", 12_000)))))
        concurrency = max(1, min(32, int(_setting(extra, raw_llm, "llm_trigger_concurrency", raw_llm.get("concurrency", 2)))))
    except (TypeError, ValueError) as exc:
        raise ValueError("LLM trigger timeout/input/concurrency 配置格式错误") from exc
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
    )


def build_llm_trigger_input(
    summary: str,
    messages: Iterable[QueueMessage],
    max_bytes: int,
) -> str:
    """按字节预算拼接历史摘要和最新队列消息，保留最新消息尾部。"""
    max_bytes = max(1, int(max_bytes))
    lines = [
        "判断当前 OneBot11 群消息是否明确要求机器人回复。",
        "只输出 JSON 布尔值 true 或 false，不要输出 Markdown、解释或其他字符。",
    ]
    if summary:
        lines.extend(["历史摘要：", summary])
    lines.append("当前队列：")
    for message in messages:
        lines.append(f"#{message.seq or '?'} [{message.user_name}] {message.text}")
    text = "\n".join(lines)
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    suffix = "\n当前队列（最新内容截取）：\n"
    tail_budget = max(0, max_bytes - len(suffix.encode("utf-8")))
    if tail_budget == 0:
        return suffix.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")
    return suffix + raw[-tail_budget:].decode("utf-8", errors="ignore")


def build_trigger_snapshot(
    chat_id: str,
    messages: Iterable[QueueMessage],
) -> TriggerSnapshot:
    """把持久消息投影为 selector 可见字段；不暴露角色或工具配置。"""
    projected: list[TriggerMessageSnapshot] = []
    for message in messages:
        if message.seq is None:
            continue
        reply_to = str(message.metadata.get("onebot11_reply_to") or "").strip() or None
        markers = tuple(
            str(item)[:128]
            for item in (message.metadata.get("onebot11_markers") or ())
            if str(item).strip()
        )[:32]
        projected.append(
            TriggerMessageSnapshot(
                seq=int(message.seq),
                user_id=str(message.user_id),
                user_name=str(message.user_name),
                text=str(message.text),
                reply_to_message_id=reply_to,
                markers=markers,
            )
        )
    return TriggerSnapshot(str(chat_id), tuple(projected))


def _json_object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """构造 JSON object 时拒绝重复键，避免模型输出被静默覆盖。"""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"自动锚点输出包含重复字段: {key}")
        result[key] = value
    return result


def parse_anchor_decision(content: str) -> AnchorDecision:
    """严格解析自动 evaluator 输出，不接受布尔旧协议或额外字段。"""
    if not isinstance(content, str):
        raise ValueError("自动锚点输出必须是 JSON 字符串")
    try:
        payload = json.loads(content, object_pairs_hook=_json_object_no_duplicates)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError("自动锚点输出不是合法 JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {"anchor_seq", "reason_code"}:
        raise ValueError("自动锚点输出只能包含 anchor_seq 和 reason_code")
    anchor_seq = payload["anchor_seq"]
    if anchor_seq is not None and (type(anchor_seq) is not int or anchor_seq <= 0):
        raise ValueError("anchor_seq 必须是正整数或 null")
    reason_code = payload["reason_code"]
    if reason_code not in {"automatic_request", "no_request"}:
        raise ValueError("reason_code 不是允许的固定值")
    return AnchorDecision(anchor_seq, reason_code)


def _selector_message_payload(message: TriggerMessageSnapshot, text: str) -> dict[str, Any]:
    """生成 selector prompt 的字段白名单，不透传任意消息元数据。"""
    payload: dict[str, Any] = {
        "seq": message.seq,
        "user_id": str(message.user_id),
        "user_name": str(message.user_name),
        "text": text,
    }
    if message.reply_to_message_id:
        payload["reply_to_message_id"] = str(message.reply_to_message_id)
    if message.markers:
        payload["markers"] = [str(marker) for marker in message.markers]
    return payload


def _compact_json(value: Mapping[str, Any]) -> str:
    """稳定序列化 selector 可见字段。"""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def build_anchor_selector_prompt(
    snapshot: TriggerSnapshot,
    max_bytes: int,
) -> AnchorSelectorPrompt:
    """构造有界 selector prompt，并返回模型实际看到的最大 seq。"""
    max_bytes = max(1, int(max_bytes))
    header = "\n".join(
        (
            "从以下按 seq 升序排列的群消息中，选择最早一条明确要求机器人开始独立任务的消息。",
            "只输出一个 JSON 对象：有请求时输出 "
            '{"anchor_seq":正整数,"reason_code":"automatic_request"}；'
            '无请求时输出 {"anchor_seq":null,"reason_code":"no_request"}。',
            "禁止输出其他字段、解释或 Markdown。",
            "消息（JSON Lines）：",
        )
    )
    if len(header.encode("utf-8")) >= max_bytes:
        return AnchorSelectorPrompt(
            header.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore"),
            None,
        )

    lines = [header]
    truncated = False
    visible_max_seq: int | None = None
    truncation_note = "\n（因字节预算截断；未展示的当前文本和后续消息不得作为锚点。）"
    note_bytes = len(truncation_note.encode("utf-8"))
    for message in snapshot.messages:
        serialized = "\n" + _compact_json(_selector_message_payload(message, message.text))
        current_bytes = len("".join(lines).encode("utf-8"))
        if current_bytes + len(serialized.encode("utf-8")) <= max_bytes:
            lines.append(serialized)
            visible_max_seq = message.seq
            continue

        available = max_bytes - current_bytes - note_bytes
        minimal = "\n" + _compact_json(_selector_message_payload(message, ""))
        if available >= len(minimal.encode("utf-8")):
            low = 0
            high = len(message.text)
            best = minimal
            while low <= high:
                middle = (low + high) // 2
                candidate = "\n" + _compact_json(
                    _selector_message_payload(message, message.text[:middle])
                )
                if len(candidate.encode("utf-8")) <= available:
                    best = candidate
                    low = middle + 1
                else:
                    high = middle - 1
            lines.append(best)
            visible_max_seq = message.seq
        truncated = True
        break
    if truncated:
        lines.append(truncation_note)
    return AnchorSelectorPrompt("".join(lines), visible_max_seq)


def selector_schedule_reason(
    *,
    text: str,
    reply_to_message_id: str | None = None,
    active_window: bool = False,
) -> SelectorScheduleReason | None:
    """返回是否值得调度自动 evaluator；该信号本身绝不创建锚点。"""
    if reply_to_message_id:
        return "reply"
    normalized = (text or "").strip().casefold()
    question_prefixes = (
        "怎么",
        "如何",
        "为什么",
        "为何",
        "能否",
        "可否",
        "是否",
        "是不是",
        "有没有",
        "哪里",
        "什么",
        "谁",
        "哪",
    )
    if normalized.endswith(("?", "？")) or normalized.startswith(question_prefixes):
        return "question"
    if active_window:
        return "active_window"
    return None


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
    elif mentioned_self:
        return TriggerDecision(True, "mention", explicit=True)
    elif keyword_matches(text, config.keywords):
        return TriggerDecision(True, "keyword", explicit=True)
    elif config.always or not config.require_mention:
        reason = "always"
    else:
        return TriggerDecision(False, "no_trigger")
    if config.cooldown_seconds > 0 and last_trigger_at is not None:
        current = wall_clock() if now is None else now
        if current - last_trigger_at < config.cooldown_seconds:
            return TriggerDecision(False, "cooldown", explicit=False)
    return TriggerDecision(True, reason, explicit=False)
