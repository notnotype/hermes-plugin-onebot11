"""群聊触发判定:mention / 关键词 / LLM 判断。

零 Hermes 依赖。LLM 判断通过 judge 回调注入(adapter 里接 ctx.llm),本模块可独立测试。
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .events import InboundEvent
from .queue import GroupMessageQueue

# LLM 判定回调: (chat_id, 队列快照文本, 当前消息) -> bool
LlmJudge = Callable[[str, str, str], Awaitable[bool]]


@dataclass
class TriggerPolicy:
    """触发策略。

    - mention 始终是触发源(用户设计:最基本的 @/mention 触发)
    - keywords: 正则命中即触发
    - llm_judge: 异步回调;None = 不启用
    """

    keywords: list[str] = field(default_factory=list)
    llm_judge: LlmJudge | None = None

    def _keyword_hit(self, text: str) -> bool:
        return any(re.search(kw, text) for kw in self.keywords if kw)

    async def decide(self, event: InboundEvent, queue: GroupMessageQueue) -> bool:
        """返回是否触发。mention / 关键词 / LLM 判断任一命中。"""
        if event.mentioned_self:
            return True
        if self._keyword_hit(event.text):
            return True
        if self.llm_judge is not None:
            snapshot = "\n".join(f"[{m.user_name}] {m.text}" for m in queue.snapshot(event.chat_id))
            return await self.llm_judge(event.chat_id, snapshot, event.text)
        return False
