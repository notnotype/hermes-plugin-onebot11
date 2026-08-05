"""OneBot 11 触发规则。

``should_trigger`` 只做确定性判断；旁路 LLM 判断由 adapter 负责调度和持久化，
这样关键词、@ 和兼容模式可以在没有 Hermes 的情况下完整测试。
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
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


@dataclass(frozen=True)
class TriggerDecision:
    """触发判断结果和原因，便于日志和测试。"""

    triggered: bool
    reason: str


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
