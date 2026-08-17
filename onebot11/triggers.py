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

from .permissions import parse_bool, parse_id_list
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
    llm_base_url: str = ""
    llm_api_key_env: str = ""
    llm_timeout_seconds: float = 30.0
    llm_input_bytes: int = 12_000
    llm_concurrency: int = 2
    llm_max_failures: int = 3
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
    debounce_seconds: float = 2.0
    engaged_idle_seconds: float = 60.0
    engaged_max_seconds: float = 300.0
    engaged_max_arbitrations: int = 2
    # 三档 engage 预算：shallow（省 token）/ normal（默认）/ deep（重要任务）。
    # 只有预算不同，不改变 trigger/wait/ignore 三态合同。
    shallow_engaged_idle_seconds: float = 30.0
    shallow_engaged_max_seconds: float = 120.0
    shallow_max_arbitrations: int = 1
    shallow_timeout_seconds: float = 12.0
    shallow_input_bytes: int = 6_000
    deep_engaged_idle_seconds: float = 180.0
    deep_engaged_max_seconds: float = 900.0
    deep_max_arbitrations: int = 4
    deep_timeout_seconds: float = 45.0
    deep_input_bytes: int = 20_000
    # deep 档 waiting 攒满 N 条新消息立即判，不等到期。
    deep_wait_messages: int = 2
    # bot 上轮回复以这些请求短语收尾时视为"正在等待用户提供信息"；
    # 只放明确的动词短语，避免"日志/截图"等名词单独出现造成误判。
    bot_asked_words: tuple[str, ...] = (
        "复现一下",
        "请复现",
        "发我",
        "发给我",
        "发一下",
        "贴一下",
        "提供一下",
        "请提供",
        "发个日志",
        "贴个日志",
        "发个截图",
        "截图给我",
        "发下日志",
        "发下截图",
        "告诉",
    )
    # 任务词命中时升级 deep 预算（只升预算，仍走 selector）。
    task_words: tuple[str, ...] = (
        "报错",
        "错误",
        "坏了",
        "崩溃",
        "复现",
        "日志",
        "截图",
        "帮忙",
        "帮我看看",
    )
    # `question_interest_words` 非空时定义主题范围；`question_bot_words` 可选。
    # bot 词非空表示仍要求显式关联 bot，bot 词为空表示允许群内其他成员的
    # 相关主题问句进入 selector；两组都为空时保持兼容行为，不做本地主题门控。
    question_bot_words: tuple[str, ...] = ()
    question_interest_words: tuple[str, ...] = ()
    # engaged 中短消息（≤ 该字数）且 bot 未提问、非问句、无回指、未引用
    # bot 时本地判 ignore，不进 selector，省成本并让群聊更安静。
    # 默认 0 = 关闭：短确认词仍统一交给 selector（既有合同），需要更安静
    # 的群可按需开启。
    short_rule_max_chars: int = 0

    def tier_for(self, level: str) -> EngagementTier:
        """按 level 返回对应预算档，normal 使用现有顶层参数。"""
        if level == "shallow":
            return EngagementTier(
                idle_seconds=self.shallow_engaged_idle_seconds,
                max_seconds=self.shallow_engaged_max_seconds,
                max_arbitrations=self.shallow_max_arbitrations,
                timeout_seconds=self.shallow_timeout_seconds,
                input_bytes=self.shallow_input_bytes,
            )
        if level == "deep":
            return EngagementTier(
                idle_seconds=self.deep_engaged_idle_seconds,
                max_seconds=self.deep_engaged_max_seconds,
                max_arbitrations=self.deep_max_arbitrations,
                timeout_seconds=self.deep_timeout_seconds,
                input_bytes=self.deep_input_bytes,
            )
        return EngagementTier(
            idle_seconds=self.engaged_idle_seconds,
            max_seconds=self.engaged_max_seconds,
            max_arbitrations=self.engaged_max_arbitrations,
            timeout_seconds=self.llm_timeout_seconds,
            input_bytes=self.llm_input_bytes,
        )


@dataclass(frozen=True)
class EngagementTier:
    """一个 engage 预算档的解析结果。"""

    idle_seconds: float
    max_seconds: float
    max_arbitrations: int
    timeout_seconds: float
    input_bytes: int


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
    anchor_seq: int | None = None


@dataclass
class LayeredTriggerState:
    """一个群的内存触发状态；重启后由 adapter 重新创建为 idle。"""

    config: TriggerConfig
    mode: str = "idle"
    level: str = "normal"
    debounce_due: float | None = None
    wait_until: float | None = None
    engaged_until: float | None = None
    engaged_max_until: float | None = None
    arbitration_count: int = 0
    llm_calls: int = 0
    model_calls: int = 0
    model_failures: int = 0
    llm_failures: int = 0
    last_candidate_type: str = ""
    dirty_revision: int | None = None
    last_message_at: float | None = None
    last_user_id: str = ""
    bot_asked: bool = False
    ignore_streak: int = 0
    wait_message_count: int = 0
    _candidate_type: str = field(default="", repr=False)
    _judgement_generation: int = field(default=0, repr=False)
    _window_arbitration_limit: int = field(default=0, repr=False)

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
        user_id: str = "",
        reply_to_bot: bool = False,
    ) -> TriggerAction:
        """观察一条已入队消息，返回直接触发或安排仲裁的动作。"""
        # 记录本群消息间隔，用于自适应节流：距上一条消息超过 debounce
        # 窗口视为群不活跃，候选立即判断；窗口内视为活跃，合并节流。
        previous_at = self.last_message_at
        self.last_message_at = now
        gap = now - previous_at if previous_at is not None else None
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
        if not (text or "").strip():
            # 纯图片/媒体消息没有可判断的文本，不进入 selector；@ 机器人的
            # 硬触发已经在上面提前返回。
            return TriggerAction("none", reason="no_text")

        normalized_user_id = str(user_id or "").strip()
        same_user = bool(
            normalized_user_id and normalized_user_id == self.last_user_id
        )
        self.last_user_id = normalized_user_id

        if self.mode == "judging":
            self.dirty_revision = revision
            return TriggerAction("none", reason="judging_dirty")

        if self.mode == "engaged":
            if self.engaged_until is not None and now >= self.engaged_until:
                self._leave_engaged()
            else:
                # 先做分级升级/降级，再按对应档位预算安排仲裁。
                level = self._level_for_engaged(
                    text,
                    same_user=same_user,
                    reply_to_bot=reply_to_bot,
                )
                self.level = level
                if self.arbitration_count >= self._window_arbitration_limit:
                    return TriggerAction("none", reason="arbitration_limit")
                if self.config.question_enabled and is_question(text):
                    # `is_question` 保留宽松语义供 bot 问句识别；只有显著问句
                    # 才能消耗一次旁路 LLM，弱问句直接静默忽略。
                    significant_question = is_significant_question(
                        text,
                        self.config.question_interest_words,
                    )
                    if significant_question and self._question_in_scope(
                        text,
                        same_user=same_user,
                        reply_to_bot=reply_to_bot,
                    ):
                        return self._schedule("question", revision, now, gap=gap)
                    if self.config.memory_enabled and has_context and memory_matches(
                        text, self.config.memory_words
                    ):
                        return self._schedule("memory", revision, now, gap=gap)
                    return TriggerAction(
                        "none",
                        reason=("question_out_of_scope" if significant_question else "weak_question"),
                    )
                if self._short_rule_ignore(text):
                    return TriggerAction("none", reason="short_rule_ignore")
                # deep 档同用户 follow-up 立即判，不等 trailing debounce。
                if level == "deep" and same_user:
                    return self._schedule(
                        "engaged",
                        revision,
                        now,
                        gap=None,
                    )
                return self._schedule("engaged", revision, now, gap=gap)

        if self.mode == "waiting":
            if self.arbitration_count >= self._window_arbitration_limit:
                return TriggerAction("none", reason="arbitration_limit")
            # deep 档按"新增消息数"判定：攒满 N 条新消息立即仲裁，不等到期。
            if self.level == "deep" and self.config.deep_wait_messages > 0:
                self.wait_message_count += 1
                if self.wait_message_count >= self.config.deep_wait_messages:
                    return self._schedule(
                        "wait_followup",
                        revision,
                        now,
                        gap=None,
                    )
                # 尚未攒满：消息继续留在 pending，等待后续新消息或 wait 到期。
                return TriggerAction("none", reason="wait_collecting")
            return self._schedule("wait_followup", revision, now, gap=gap)

        if self.mode == "debounce":
            return self._schedule(
                self._candidate_type or "candidate", revision, now, gap=gap
            )

        candidate_type = self._candidate_type_for(
            text,
            has_context,
            same_user=same_user,
            reply_to_bot=reply_to_bot,
        )
        if not candidate_type:
            if self.config.question_enabled and is_question(text):
                significant_question = is_significant_question(
                    text,
                    self.config.question_interest_words,
                )
                return TriggerAction(
                    "none",
                    reason=(
                        "question_out_of_scope"
                        if significant_question
                        else "weak_question"
                    ),
                )
            return TriggerAction("none", reason="non_candidate")
        return self._schedule(candidate_type, revision, now, gap=gap)

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
        anchor_seq: int | None,
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
            if type(anchor_seq) is not int or anchor_seq <= 0:
                self.llm_failures += 1
                self._return_to_engaged_or_idle(now)
                self.wait_until = None
                self._candidate_type = ""
                self.dirty_revision = None
                return TriggerAction("none", reason="invalid_result")
            self.mode = "idle"
            self._candidate_type = ""
            self.dirty_revision = None
            self.level = "normal"
            self.ignore_streak = 0
            self.wait_message_count = 0
            return TriggerAction(
                "direct",
                reason="llm",
                generation=self._judgement_generation,
                anchor_seq=anchor_seq,
            )
        if decision == "wait" and anchor_seq is None:
            self.mode = "waiting"
            # 新合同不再允许模型控制等待秒数；等待窗口由本地状态机
            # 控制，避免模型通过任意数字制造高频轮询。
            self.wait_until = now + self.config.engaged_idle_seconds
            self._candidate_type = ""
            self.dirty_revision = None
            return TriggerAction("wait", reason="llm_wait", due_at=self.wait_until)
        if decision == "ignore" and anchor_seq is None:
            self.ignore_streak += 1
            self.wait_message_count = 0
            # 连续 ignore 使预算降档，避免同一窗口反复消耗 selector。
            if self.ignore_streak >= 2 and self.level != "shallow":
                if self.level == "deep":
                    self.level = "normal"
                else:
                    self.level = "shallow"
            self._return_to_engaged_or_idle(now)
            self.wait_until = None
            self._candidate_type = ""
            self.dirty_revision = None
            return TriggerAction("none", reason="llm_ignore")
        if decision not in {"ignore", "wait"}:
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
                failure_gap = (
                    now - self.last_message_at
                    if self.last_message_at is not None
                    else None
                )
                return self._schedule(
                    self._candidate_type or "candidate",
                    current_revision,
                    now,
                    gap=failure_gap,
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
        self.level = "normal"
        self.debounce_due = None
        self.wait_until = None
        self.engaged_until = None
        self.engaged_max_until = None
        self.arbitration_count = 0
        self._window_arbitration_limit = 0
        self._candidate_type = ""
        self.dirty_revision = None
        self.bot_asked = False
        self.ignore_streak = 0
        self.wait_message_count = 0

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
        self.level = "normal"
        self.debounce_due = None
        self.wait_until = None
        self._candidate_type = ""
        self.dirty_revision = None
        self._window_arbitration_limit = 0
        self.bot_asked = False
        self.ignore_streak = 0
        self.wait_message_count = 0

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
        bot_asked: bool = False,
        anchor_user_id: str = "",
    ) -> None:
        """成功 Agent turn 进入 engaged；有新消息时不覆盖其待处理状态。"""
        if preserve_pending and not success:
            # 失败/取消/unknown 的 turn 不属于连续对话成功窗口；即使期间
            # 有 pending 消息，也不能把下一条普通消息误判成 follow-up。
            self._judgement_generation += 1
            self.debounce_due = None
            self.wait_until = None
            self._candidate_type = ""
            self.dirty_revision = None
            self._leave_engaged()
            return
        if preserve_pending and (
            has_hard_trigger
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
            self.bot_asked = False
            self.level = "normal"
            self.ignore_streak = 0
            self.wait_message_count = 0
            return
        # 只有成功回复才更新 bot_asked：bot 以问句/请求收尾时，用户下一条
        # 消息应得到大预算（deep），这是"重要消息"最确定的本地信号。
        self.bot_asked = bool(bot_asked)
        self.level = "deep" if self.bot_asked else "normal"
        self.ignore_streak = 0
        self.wait_message_count = 0
        if anchor_user_id:
            self.last_user_id = str(anchor_user_id)
        tier = self.config.tier_for(self.level)
        if self.engaged_max_until is None or self.engaged_max_until <= now:
            self.engaged_max_until = now + tier.max_seconds
            self.arbitration_count = 0
            self._window_arbitration_limit = tier.max_arbitrations
        self.engaged_until = min(
            now + tier.idle_seconds,
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
            "arbitrations": self.llm_calls,
            "model_calls": self.model_calls,
            "model_failures": self.model_failures,
            "llm_failures": self.llm_failures,
            "last_candidate_type": self.last_candidate_type,
            "level": self.level,
            "bot_asked": self.bot_asked,
            "ignore_streak": self.ignore_streak,
            "wait_message_count": self.wait_message_count,
        }

    def _level_for_engaged(
        self,
        text: str,
        *,
        same_user: bool,
        reply_to_bot: bool,
    ) -> str:
        """按本地信号决定 engaged 消息的预算档，只升级预算不直接触发。"""
        if reply_to_bot:
            # 引用 bot 上一条回复是最强的继续对话信号。
            return "deep"
        if self.bot_asked:
            if same_user:
                # bot 上轮问了用户、用户回复：最重要的大预算场景。
                return "deep"
            # 他人插话不享受 deep，回落 normal 预算。
            return "normal"
        if self.config.task_words and any(
            word in (text or "") for word in self.config.task_words
        ):
            return "deep"
        if self.ignore_streak >= 2:
            return "shallow"
        return self.level

    def _short_rule_ignore(self, text: str) -> bool:
        """shallow 档的短消息且无任何信号时本地判 ignore，不进 selector。

        只在连续 ignore 降级到 shallow 后生效，避免破坏正常 engaged 中
        短确认词统一交给 selector 的合同。
        """
        if self.level != "shallow":
            return False
        limit = self.config.short_rule_max_chars
        if limit <= 0 or len((text or "").strip()) > limit:
            return False
        if self.bot_asked:
            return False
        if is_question(text):
            return False
        if memory_matches(text, self.config.memory_words):
            return False
        return True
    def _question_in_scope(
        self,
        text: str,
        *,
        same_user: bool,
        reply_to_bot: bool,
    ) -> bool:
        """判断问句是否命中主题范围，并按配置决定是否要求关联 bot。"""
        bot_words = self.config.question_bot_words
        interest_words = self.config.question_interest_words
        if not bot_words and not interest_words:
            return True
        if interest_words and not keyword_matches(text, interest_words):
            return False
        if not bot_words:
            # 空 bot 词表明确表示不要求 @/引用/bot 追问关系，允许其他成员的
            # 相关主题问句进入 selector；最终是否回复仍由旁路模型判断。
            return True
        if reply_to_bot or (same_user and self.bot_asked):
            return True
        return keyword_matches(text, bot_words)


    def _candidate_type_for(
        self,
        text: str,
        has_context: bool,
        *,
        same_user: bool,
        reply_to_bot: bool,
    ) -> str:
        """按显著问句范围和有上下文的记忆回指识别候选。"""
        if self.config.question_enabled and is_significant_question(
            text,
            self.config.question_interest_words,
        ):
            if self._question_in_scope(
                text,
                same_user=same_user,
                reply_to_bot=reply_to_bot,
            ):
                return "question"
            if self.config.memory_enabled and has_context and memory_matches(
                text, self.config.memory_words
            ):
                return "memory"
            return ""
        if self.config.memory_enabled and has_context and memory_matches(
            text, self.config.memory_words
        ):
            return "memory"
        return ""

    def _schedule(
        self,
        candidate_type: str,
        revision: int,
        now: float,
        *,
        gap: float | None = None,
    ) -> TriggerAction:
        """安排或重置 trailing debounce；群不活跃时立即判断，不创建 lease。"""
        if gap is None:
            wait = 0.0  # 本群第一条消息：视为不活跃，立即进入判断
        else:
            wait = max(0.0, self.config.debounce_seconds - gap)
        self.mode = "debounce"
        self.debounce_due = now + wait
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
        self.level = "normal"
        self.engaged_until = None
        self.engaged_max_until = None
        self.arbitration_count = 0
        self._window_arbitration_limit = 0
        self.bot_asked = False
        self.ignore_streak = 0
        self.wait_message_count = 0

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
        if not isinstance(item, str):
            raise ValueError("trigger_keywords 和 memory_trigger_words 只能包含字符串")
        text = item.strip().casefold()
        if text:
            normalized.append(text)
    return tuple(dict.fromkeys(normalized))


def _parse_group_ids(value: Any) -> frozenset[str]:
    """解析 LLM trigger 的显式群 allowlist。"""
    return frozenset(parse_id_list(value))


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
    "帮我找",
    "找资料",
    "查资料",
    "查一下",
    "搜一下",
    "搜索一下",
    "检索",
    "推荐一下",
    "介绍一下",
    "解释一下",
    "几",
    "多少",
    "啥",
    "谁",
    "哪",
    "是不是",
    "能否",
    "要不要",
    "行不行",
    "多久",
    "几点",
    "什么时候",
    "有没有",
    "想问一下",
    "请教一下",
    "这个怎么弄",
    "要放哪",
    "能不能帮忙",
    "有没有办法",
    "这个正常吗",
)
# 问号、明确求助短语，或“主题词 + 强疑问词”才消耗旁路 LLM。
# 单独的“吗/几”以及脱离主题的闲聊疑问词不进入 selector。
SIGNIFICANT_QUESTION_WORDS = tuple(
    word for word in QUESTION_WORDS if word not in {"吗", "几"}
)
EXPLICIT_QUESTION_PHRASES = (
    "请问",
    "请教",
    "想问",
    "帮我",
    "帮忙",
    "找资料",
    "查资料",
    "查一下",
    "搜一下",
    "搜索一下",
    "检索",
    "推荐一下",
    "介绍一下",
    "解释一下",
    "能不能帮",
    "有没有办法",
    "这个怎么弄",
    "这个正常吗",
)
ENGLISH_QUESTION_RE = re.compile(
    r"\b(?:who|what|when|where|why|how|can|could|would|should|do|does|did|is|are|will)\b",
    re.IGNORECASE,
)
ENGLISH_QUESTION_START_RE = re.compile(
    r"^(?:who|what|when|where|why|how|can|could|would|should|do|does|did|is|are|will)\b",
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


def is_significant_question(
    text: str,
    interest_words: Iterable[str] = (),
) -> bool:
    """只放行足够明确、值得消耗一次 LLM 仲裁的问句。"""
    normalized = (text or "").casefold().strip()
    if not normalized:
        return False
    if "?" in normalized or "？" in normalized:
        return True
    if ENGLISH_QUESTION_START_RE.search(normalized):
        return True
    if any(phrase in normalized for phrase in EXPLICIT_QUESTION_PHRASES):
        return True
    has_interest = any(
        str(word).strip().casefold() in normalized
        for word in interest_words
        if str(word).strip()
    )
    # 无问号的疑问词只在命中已配置主题时放行，避免把群聊闲谈送进 LLM。
    return has_interest and any(
        word.casefold() in normalized for word in SIGNIFICANT_QUESTION_WORDS
    )


def memory_matches(text: str, words: Iterable[str]) -> bool:
    """识别带有回指词的记忆候选，不执行外部检索。"""
    folded = (text or "").casefold()
    return any(str(word).casefold() in folded for word in words if str(word).strip())


def parse_llm_decision(value: Any) -> tuple[str, int | None] | None:
    """严格解析旁路模型的三态 JSON 结果。"""
    if not isinstance(value, Mapping):
        return None
    if set(value) != {"decision", "anchor_seq"}:
        return None
    decision = value.get("decision")
    anchor_seq = value.get("anchor_seq")
    if decision not in {"trigger", "wait", "ignore"}:
        return None
    if decision == "trigger":
        if type(anchor_seq) is not int or anchor_seq <= 0:
            return None
    elif anchor_seq is not None:
        return None
    return str(decision), anchor_seq


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
    provider_default = "deepseek" if not llm_enabled else ""
    model_default = "deepseek-v4-flash" if not llm_enabled else ""
    raw_provider = _setting(
        extra,
        raw_llm,
        "llm_trigger_provider",
        raw_llm.get("provider", provider_default),
    )
    raw_model = _setting(
        extra,
        raw_llm,
        "llm_trigger_model",
        raw_llm.get("model", model_default),
    )
    if raw_provider is None:
        raw_provider = ""
    if raw_model is None:
        raw_model = ""
    if not isinstance(raw_provider, str) or not isinstance(raw_model, str):
        raise ValueError("llm_trigger provider/model 必须是字符串")
    provider = raw_provider.strip()
    model = raw_model.strip()
    if provider.casefold() == "custom":
        # Node helper 使用这个保留字选择 OpenAI-compatible 自定义 provider；
        # 统一大小写，避免配置验证通过但运行时被当作内置 provider。
        provider = "custom"
    if llm_enabled and (not provider or not model):
        raise ValueError("启用 LLM trigger 时必须配置 provider 和 model")
    raw_base_url = _setting(extra, raw_llm, "llm_trigger_base_url", raw_llm.get("base_url", ""))
    if "api_key" in raw_llm or "llm_trigger_api_key" in extra:
        raise ValueError("llm_trigger 不允许直接配置 api_key，只能配置 api_key_env")
    if raw_base_url is None:
        raw_base_url = ""
    if not isinstance(raw_base_url, str):
        raise ValueError("llm_trigger base_url 必须是字符串")
    base_url = raw_base_url.strip()
    if base_url:
        from urllib.parse import urlparse

        parsed_url = urlparse(base_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("llm_trigger base_url 必须是 http/https URL")
        if parsed_url.username or parsed_url.password:
            raise ValueError("llm_trigger base_url 不能包含用户名或密码")
        if parsed_url.query or parsed_url.fragment:
            raise ValueError("llm_trigger base_url 不能包含 query 或 fragment")
    raw_api_key_env = _setting(
        extra,
        raw_llm,
        "llm_trigger_api_key_env",
        raw_llm.get("api_key_env", ""),
    )
    if raw_api_key_env is None:
        raw_api_key_env = ""
    if not isinstance(raw_api_key_env, str):
        raise ValueError("llm_trigger api_key_env 必须是字符串")
    api_key_env = raw_api_key_env.strip()
    if api_key_env and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", api_key_env):
        raise ValueError("llm_trigger api_key_env 不是合法环境变量名")
    if llm_enabled and provider.casefold() == "custom":
        if not base_url:
            raise ValueError("custom llm_trigger 必须配置 base_url")
        if not api_key_env:
            raise ValueError("custom llm_trigger 必须配置 api_key_env")
    if base_url and provider.casefold() != "custom":
        raise ValueError("llm_trigger base_url 只允许与 provider=custom 一起使用")
    allowed_groups = _parse_group_ids(
        _setting(extra, raw_llm, "llm_trigger_groups", raw_llm.get("groups"))
    )
    timeout = _bounded_float(
        _setting(extra, raw_llm, "llm_trigger_timeout_seconds", raw_llm.get("timeout", 30)),
        name="llm_trigger_timeout_seconds",
        minimum=0.1,
        maximum=300.0,
    )
    max_failures = _bounded_int(
        _setting(extra, raw_llm, "llm_trigger_max_failures", raw_llm.get("max_failures", 3)),
        name="llm_trigger_max_failures",
        minimum=0,
        maximum=32,
    )
    input_bytes = _bounded_int(
        _setting(extra, raw_llm, "llm_trigger_input_bytes", raw_llm.get("input_bytes", 12_000)),
        name="llm_trigger_input_bytes",
        minimum=768,
        maximum=64_000,
    )
    concurrency = _bounded_int(
        _setting(extra, raw_llm, "llm_trigger_concurrency", raw_llm.get("concurrency", 2)),
        name="llm_trigger_concurrency",
        minimum=1,
        maximum=32,
    )
    debounce = _bounded_float(
        _setting(extra, raw_llm, "trigger_debounce_seconds", 2),
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
        _setting(extra, raw_llm, "engaged_max_arbitrations", 2),
        name="engaged_max_arbitrations",
        minimum=0,
        maximum=32,
    )
    # 三档 engage 预算：shallow / normal / deep。normal 复用顶层参数；
    # 分档键名统一为 {档位}_{字段}，可放在 llm_trigger 顶层或嵌套 tiers。
    raw_tiers = raw_llm.get("tiers")
    if raw_tiers is None:
        raw_tiers = {}
    if not isinstance(raw_tiers, Mapping):
        raise ValueError("llm_trigger.tiers 必须是 YAML mapping")
    shallow_tier = raw_tiers.get("shallow") if isinstance(
        raw_tiers.get("shallow"), Mapping
    ) else {}
    deep_tier = raw_tiers.get("deep") if isinstance(
        raw_tiers.get("deep"), Mapping
    ) else {}
    if raw_tiers.get("shallow") is not None and not isinstance(
        raw_tiers.get("shallow"), Mapping
    ):
        raise ValueError("llm_trigger.tiers.shallow 必须是 YAML mapping")
    if raw_tiers.get("deep") is not None and not isinstance(
        raw_tiers.get("deep"), Mapping
    ):
        raise ValueError("llm_trigger.tiers.deep 必须是 YAML mapping")

    shallow_idle = _bounded_float(
        _setting(
            extra,
            shallow_tier,
            "shallow_engaged_idle_seconds",
            shallow_tier.get("engaged_idle_seconds", 30),
        ),
        name="shallow_engaged_idle_seconds",
        minimum=1.0,
        maximum=3600.0,
    )
    shallow_max = _bounded_float(
        _setting(
            extra,
            shallow_tier,
            "shallow_engaged_max_seconds",
            shallow_tier.get("max_seconds", 120),
        ),
        name="shallow_engaged_max_seconds",
        minimum=shallow_idle,
        maximum=86_400.0,
    )
    shallow_arbitrations = _bounded_int(
        _setting(
            extra,
            shallow_tier,
            "shallow_max_arbitrations",
            shallow_tier.get("max_arbitrations", 1),
        ),
        name="shallow_max_arbitrations",
        minimum=0,
        maximum=32,
    )
    shallow_timeout = _bounded_float(
        _setting(
            extra,
            shallow_tier,
            "shallow_timeout_seconds",
            shallow_tier.get("timeout_seconds", 12),
        ),
        name="shallow_timeout_seconds",
        minimum=0.1,
        maximum=300.0,
    )
    shallow_input = _bounded_int(
        _setting(
            extra,
            shallow_tier,
            "shallow_input_bytes",
            shallow_tier.get("input_bytes", 6_000),
        ),
        name="shallow_input_bytes",
        minimum=768,
        maximum=64_000,
    )
    deep_idle = _bounded_float(
        _setting(
            extra,
            deep_tier,
            "deep_engaged_idle_seconds",
            deep_tier.get("engaged_idle_seconds", 180),
        ),
        name="deep_engaged_idle_seconds",
        minimum=1.0,
        maximum=86_400.0,
    )
    deep_max = _bounded_float(
        _setting(
            extra,
            deep_tier,
            "deep_engaged_max_seconds",
            deep_tier.get("max_seconds", 900),
        ),
        name="deep_engaged_max_seconds",
        minimum=deep_idle,
        maximum=86_400.0,
    )
    deep_arbitrations = _bounded_int(
        _setting(
            extra,
            deep_tier,
            "deep_max_arbitrations",
            deep_tier.get("max_arbitrations", 4),
        ),
        name="deep_max_arbitrations",
        minimum=0,
        maximum=32,
    )
    deep_timeout = _bounded_float(
        _setting(
            extra,
            deep_tier,
            "deep_timeout_seconds",
            deep_tier.get("timeout_seconds", 45),
        ),
        name="deep_timeout_seconds",
        minimum=0.1,
        maximum=300.0,
    )
    deep_input = _bounded_int(
        _setting(
            extra,
            deep_tier,
            "deep_input_bytes",
            deep_tier.get("input_bytes", 20_000),
        ),
        name="deep_input_bytes",
        minimum=768,
        maximum=64_000,
    )
    deep_wait_messages = _bounded_int(
        _setting(
            extra,
            deep_tier,
            "deep_wait_messages",
            deep_tier.get("wait_messages", 2),
        ),
        name="deep_wait_messages",
        minimum=0,
        maximum=32,
    )
    memory_words = _parse_keywords(
        _setting(
            extra,
            raw_llm,
            "memory_trigger_words",
            ("之前", "上次", "刚才", "那个", "继续", "接着"),
        )
    )
    bot_asked_words = _parse_keywords(
        _setting(
            extra,
            raw_llm,
            "bot_asked_words",
            (
                "复现一下",
                "请复现",
                "发我",
                "发给我",
                "发一下",
                "贴一下",
                "提供一下",
                "请提供",
                "发个日志",
                "贴个日志",
                "发个截图",
                "截图给我",
                "发下日志",
                "发下截图",
                "告诉",
            ),
        )
    )
    task_words = _parse_keywords(
        _setting(
            extra,
            raw_llm,
            "task_words",
            (
                "报错",
                "错误",
                "坏了",
                "崩溃",
                "复现",
                "日志",
                "截图",
                "帮忙",
                "帮我看看",
            ),
        )
    )
    short_rule_chars = _bounded_int(
        _setting(extra, raw_llm, "short_rule_max_chars", 0),
        name="short_rule_max_chars",
        minimum=0,
        maximum=200,
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
    question_bot_words = _parse_keywords(
        _setting(
            extra,
            raw_llm,
            "question_bot_words",
            (),
        )
    )
    question_interest_words = _parse_keywords(
        _setting(
            extra,
            raw_llm,
            "question_interest_words",
            (),
        )
    )
    if question_bot_words and not question_interest_words:
        raise ValueError(
            "配置 question_bot_words 时必须同时配置 question_interest_words；"
            "仅配置兴趣词时不要求关联 bot"
        )
    return TriggerConfig(
        require_mention=parse_bool(extra.get("require_mention"), default=True, name="require_mention"),
        keywords=_parse_keywords(extra.get("trigger_keywords", extra.get("keywords"))),
        always=parse_bool(extra.get("always_trigger"), default=False, name="always_trigger"),
        cooldown_seconds=cooldown,
        llm_enabled=llm_enabled,
        llm_provider=provider,
        llm_model=model,
        llm_base_url=base_url,
        llm_api_key_env=api_key_env,
        llm_timeout_seconds=timeout,
        llm_input_bytes=input_bytes,
        llm_concurrency=concurrency,
        llm_max_failures=max_failures,
        llm_allowed_groups=allowed_groups,
        question_enabled=question_enabled,
        memory_enabled=memory_enabled,
        memory_words=memory_words,
        question_bot_words=question_bot_words,
        question_interest_words=question_interest_words,
        debounce_seconds=debounce,
        engaged_idle_seconds=engaged_idle,
        engaged_max_seconds=engaged_max,
        engaged_max_arbitrations=engaged_arbitrations,
        shallow_engaged_idle_seconds=shallow_idle,
        shallow_engaged_max_seconds=shallow_max,
        shallow_max_arbitrations=shallow_arbitrations,
        shallow_timeout_seconds=shallow_timeout,
        shallow_input_bytes=shallow_input,
        deep_engaged_idle_seconds=deep_idle,
        deep_engaged_max_seconds=deep_max,
        deep_max_arbitrations=deep_arbitrations,
        deep_timeout_seconds=deep_timeout,
        deep_input_bytes=deep_input,
        deep_wait_messages=deep_wait_messages,
        bot_asked_words=bot_asked_words,
        task_words=task_words,
        short_rule_max_chars=short_rule_chars,
    )


def build_llm_trigger_input(
    summary: str,
    messages: Iterable[QueueMessage],
    max_bytes: int,
    candidate_type: str = "candidate",
) -> str:
    max_bytes = max(1, int(max_bytes))
    queued = tuple(messages)
    latest = queued[-1] if queued else None
    question_rule = (
        "问号或常见疑问词（吗、什么、怎么、为什么、哪里、如何、能不能、请问、有没有等）只代表候选；"
        "只有明确向机器人求助，或问题属于 AI、资料检索、文档、技术、项目等相关主题时才 trigger。"
    )
    if candidate_type == "engaged":
        rules = (
            question_rule,
            "其余消息只有明确向机器人提问、请求帮助、讨论 AI/资料/技术/项目，或明显延续与机器人的对话才 trigger；",
            "群成员之间的互动、评论、调侃、@其他人、陈述句和纯图片消息默认 ignore。",
        )
    else:
        rules = (
            question_rule,
            "群成员之间的互动、评论、调侃、@其他人、无相关主题上下文的陈述句默认 ignore。",
            "疑似问句不等于必须回复；不确定时选择 ignore。",
        )
    contract = "\n".join(
        (
            f"判断当前 OneBot11 群消息是否需要回复；候选类型：{candidate_type}。",
            "规则：" + rules[0],
            rules[1],
            *(rules[2:]),
            "只输出严格 JSON；trigger 只能选择当前队列中真实的 seq 作为 authority。",
            '{"decision":"trigger","anchor_seq":123}',
            '{"decision":"wait","anchor_seq":null}',
            '{"decision":"ignore","anchor_seq":null}',
            "wait/ignore 必须使用 null。",
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

    latest_bytes = byte_len(latest_line)
    prefix_bytes = byte_len(queue_prefix)
    if prefix_bytes + latest_bytes + 1 <= max_bytes:
        available = max_bytes - prefix_bytes - latest_bytes - 1
        selected: list[str] = []
        used = 0
        for line in reversed(optional):
            line_bytes = byte_len(line) + 1
            if used + line_bytes > available:
                break
            selected.insert(0, line)
            used += line_bytes
        optional_text = "\n".join(selected)
        if optional_text:
            optional_text += "\n"
        return queue_prefix + optional_text + truncate(
            latest_line,
            max_bytes - prefix_bytes - byte_len(optional_text),
        )

    compact_prefix = "\n".join(
        (
            f"OneBot11 selector；候选类型：{candidate_type}。",
            "问句只代表候选；明确向机器人求助，或 AI/资料/文档/技术/项目主题相关时才 trigger；",
            "群成员之间的互动默认 ignore；疑似问句不等于必须回复；不确定时选择 ignore。",
            'trigger={"decision":"trigger","anchor_seq":123}',
            'wait={"decision":"wait","anchor_seq":null}',
            'ignore={"decision":"ignore","anchor_seq":null}',
        )
    )
    compact_bytes = byte_len(compact_prefix)
    if compact_bytes < max_bytes:
        return compact_prefix + "\n" + truncate(latest_line, max_bytes - compact_bytes - 1)
    return truncate(latest_line, max_bytes)

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
    # 群的 @、关键词、always 是明确的硬触发，不能因为上一轮刚结束
    # 就把用户明确唤醒消息吞掉。私聊沿用原有 cooldown 语义。
    if chat_type == "dm" and config.cooldown_seconds > 0 and last_trigger_at is not None:
        current = monotonic() if now is None else now
        if current - last_trigger_at < config.cooldown_seconds:
            return TriggerDecision(False, "cooldown")
    return TriggerDecision(True, reason)
