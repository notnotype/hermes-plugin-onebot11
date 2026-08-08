"""群级 dispatch 状态机。

这里故意不认识 Hermes。它只负责每个 chat 同时最多一个活动 lease、heartbeat、
完成后的状态转换和恢复触发请求；适配器通过 callback 启动真实 turn。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .queue import QueueLease, QueueStore

logger = logging.getLogger(__name__)
_COMPLETION_RETRY_DELAYS: tuple[float, ...] = (2.0, 4.0, 8.0)


@dataclass(frozen=True)
class ActiveTurn:
    """调度器当前记录的活动 turn。"""

    lease: QueueLease
    started_at: float
    lease_lost: bool = False


class GroupDispatcher:
    """维护每群一个活动 turn，避免 Hermes session 并发执行。"""

    def __init__(
        self,
        store: QueueStore,
        start_turn: Callable[[QueueLease], Awaitable[None]],
        *,
        lease_seconds: float = 120.0,
        heartbeat_seconds: float | None = None,
        recovery_poll_seconds: float = 5.0,
        can_dispatch: Callable[[str], bool] | None = None,
        recovery_chat_ids: Callable[[], set[str] | frozenset[str] | tuple[str, ...] | None]
        | None = None,
        on_lease_lost: Callable[[QueueLease], Awaitable[None]] | None = None,
        on_recovery_tick: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """初始化调度器；lease 的续租由后台 heartbeat 负责。"""
        self.store = store
        self._start_turn = start_turn
        self.lease_seconds = max(5.0, float(lease_seconds))
        self.heartbeat_seconds = heartbeat_seconds or max(1.0, self.lease_seconds / 3)
        self.recovery_poll_seconds = max(0.05, float(recovery_poll_seconds))
        self._can_dispatch = can_dispatch or (lambda _chat_id: True)
        self._recovery_chat_ids = recovery_chat_ids
        self._on_lease_lost = on_lease_lost
        self._on_recovery_tick = on_recovery_tick
        self._locks: dict[str, asyncio.Lock] = {}
        self._active: dict[str, ActiveTurn] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task[None]] = {}
        self._recovery_task: asyncio.Task[None] | None = None
        self._recovery_dispatch_tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False

    def _lock_for(self, chat_id: str) -> asyncio.Lock:
        """懒创建群级异步锁，避免构造期绑定错误 event loop。"""
        lock = self._locks.get(str(chat_id))
        if lock is None:
            lock = asyncio.Lock()
            self._locks[str(chat_id)] = lock
        return lock

    async def notify(self, chat_id: str) -> bool:
        """在有持久触发请求时尝试启动该群唯一 turn。"""
        if self._closed:
            return False
        chat_id = str(chat_id)
        if not self._can_dispatch(chat_id):
            return False
        lock = self._lock_for(chat_id)
        async with lock:
            if (
                chat_id in self._active
                or self.store.status(chat_id).get("paused")
                or not self._can_dispatch(chat_id)
            ):
                return False
            lease = await asyncio.to_thread(self.store.claim, chat_id, self.lease_seconds)
            if lease is None:
                return False
            self._active[chat_id] = ActiveTurn(lease=lease, started_at=lease.claimed_at)
            self._heartbeat_tasks[chat_id] = asyncio.create_task(self._heartbeat(lease))
        try:
            await self._start_turn(lease)
        except asyncio.CancelledError:
            # 取消通常发生在 shutdown；立即停止本进程续租，持久 lease 交给恢复路径。
            await self.abandon(lease.lease_id)
            raise
        except Exception:
            logger.exception("OneBot11 turn 启动失败: chat=%s lease=%s", chat_id, lease.lease_id)
            try:
                await self.complete(lease.lease_id, outcome="failure", unknown=False)
            except Exception:
                # 启动失败已经需要向上层报告；收尾持久化失败不能留下本地 active/heartbeat。
                logger.exception("OneBot11 turn 启动失败后的 lease 收尾失败: %s", lease.lease_id)
            finally:
                await self.abandon(lease.lease_id)
            raise
        return True

    async def _heartbeat(self, lease: QueueLease) -> None:
        """长 Hermes turn 期间续租，避免被另一个进程/恢复路径重复认领。"""
        try:
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                renewed = await asyncio.to_thread(self.store.renew, lease, self.lease_seconds)
                if not renewed:
                    await self._mark_lease_lost(lease)
                    return
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("OneBot11 lease heartbeat 失败: %s", lease.lease_id)
            await self._mark_lease_lost(lease)
            return

    async def _mark_lease_lost(self, lease: QueueLease) -> None:
        """隔离失效 turn，并让 adapter 取消其 Hermes task。"""
        chat_id = str(lease.chat_id)
        lock = self._lock_for(chat_id)
        async with lock:
            active = self._active.get(chat_id)
            if active is None or active.lease.lease_id != lease.lease_id:
                return
            self._active[chat_id] = ActiveTurn(
                lease=active.lease,
                started_at=active.started_at,
                lease_lost=True,
            )
        if self._on_lease_lost is not None:
            try:
                await self._on_lease_lost(lease)
            except Exception:
                logger.exception("OneBot11 lease 丢失回调失败: %s", lease.lease_id)

    async def complete(
        self,
        lease_id: str,
        *,
        outcome: str,
        unknown: bool,
        known_failure: bool = False,
        reason: str | None = None,
    ) -> bool:
        """按真实处理结果确认、释放或标记 uncertain，不在此处启动下一轮。

        持久状态转换的短暂异常按 2/4/8 秒有限重试；若仍无法完成，
        只停止本进程的 active/heartbeat，持久 lease 等待自然过期后恢复。
        """
        if self._closed:
            return False
        lease_id = str(lease_id)
        for attempt in range(len(_COMPLETION_RETRY_DELAYS) + 1):
            try:
                changed = await self._complete_once(
                    lease_id,
                    outcome=outcome,
                    unknown=unknown,
                    known_failure=known_failure,
                    reason=reason,
                )
            except asyncio.CancelledError:
                await self.abandon(lease_id)
                raise
            except Exception:
                if attempt >= len(_COMPLETION_RETRY_DELAYS):
                    logger.exception(
                        "OneBot11 lease completion 重试耗尽，停止本进程续租: %s",
                        lease_id,
                    )
                    await self.abandon(lease_id)
                    raise
                delay = _COMPLETION_RETRY_DELAYS[attempt]
                logger.warning(
                    "OneBot11 lease completion 暂时失败，将在 %.1f 秒后重试 (%d/%d): %s",
                    delay,
                    attempt + 1,
                    len(_COMPLETION_RETRY_DELAYS),
                    lease_id,
                    exc_info=True,
                )
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    await self.abandon(lease_id)
                    raise
                continue
            if not changed:
                # False 表示 lease 已失效或状态转换没有发生；绝不能让旧 active
                # 阻塞之后对同一 chat 的恢复认领。
                logger.warning(
                    "OneBot11 lease completion was fenced or already transitioned: %s",
                    lease_id,
                )
                await self.abandon(lease_id)
            return changed
        return False  # pragma: no cover - 循环总会在成功或异常路径返回

    async def _complete_once(
        self,
        lease_id: str,
        *,
        outcome: str,
        unknown: bool,
        known_failure: bool,
        reason: str | None,
    ) -> bool:
        """执行一次持久状态转换；异常留给外层有限重试。"""
        if self._closed:
            return False
        active = self.active_by_lease(lease_id)
        if active is None:
            return False
        chat_id = str(active.lease.chat_id)
        lock = self._lock_for(chat_id)
        async with lock:
            if self._closed:
                return False
            current = self._active.get(chat_id)
            if current is None or current.lease.lease_id != lease_id:
                return False
            if current.lease_lost:
                return False
            if unknown:
                changed = await asyncio.to_thread(
                    self.store.mark_uncertain,
                    current.lease,
                    reason or "outbound result unknown",
                )
            elif outcome == "success":
                changed = await asyncio.to_thread(self.store.ack, current.lease)
            else:
                changed = await asyncio.to_thread(
                    self.store.release,
                    current.lease,
                    reason=reason,
                    allow_after_outbound=known_failure,
                )
        if changed:
            await self._detach_active(lease_id)
        return bool(changed)

    async def recover(self) -> list[str]:
        """恢复过期 lease 和持久触发请求，返回已尝试 dispatch 的群。"""
        if self._closed:
            return []
        self._ensure_recovery_loop()
        allowed_chat_ids = self._recovery_scope()
        requests = await asyncio.to_thread(
            self.store.recover_trigger_requests,
            allowed_chat_ids,
        )
        if self._on_recovery_tick is not None:
            await self._on_recovery_tick()
        chats: list[str] = []
        for chat_id in dict.fromkeys(request.chat_id for request in requests):
            if self._can_dispatch(chat_id) and await self.notify(chat_id):
                chats.append(chat_id)
        return chats

    def _ensure_recovery_loop(self) -> None:
        """启动轻量恢复轮询，等待其他进程失效 lease 到期。"""
        if self._recovery_task is None or self._recovery_task.done():
            self._recovery_task = asyncio.create_task(self._recovery_loop())

    async def _recovery_loop(self) -> None:
        """周期恢复过期 lease，不抢占仍由其他进程持有的 lease。"""
        while not self._closed:
            try:
                await asyncio.sleep(self.recovery_poll_seconds)
                if self._closed:
                    return
                allowed_chat_ids = self._recovery_scope()
                requests = await asyncio.to_thread(
                    self.store.recover_trigger_requests,
                    allowed_chat_ids,
                )
                if self._on_recovery_tick is not None:
                    await self._on_recovery_tick()
                for request in requests:
                    chat_id = request.chat_id
                    if (
                        not self._can_dispatch(chat_id)
                        or chat_id in self._active
                        or chat_id in self._recovery_dispatch_tasks
                    ):
                        continue
                    task = asyncio.create_task(self.notify(chat_id))
                    self._recovery_dispatch_tasks[chat_id] = task
                    task.add_done_callback(
                        lambda finished, chat_id=chat_id: self._finish_recovery_dispatch(
                            chat_id, finished
                        )
                    )
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("OneBot11 lease 恢复本轮失败，下轮继续")

    def _finish_recovery_dispatch(self, chat_id: str, task: asyncio.Task[None]) -> None:
        """清理恢复任务引用，并记录启动失败。"""
        if self._recovery_dispatch_tasks.get(chat_id) is task:
            self._recovery_dispatch_tasks.pop(chat_id, None)
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            logger.warning("OneBot11 恢复群 %s 失败: %s", chat_id, error)

    async def set_paused(self, chat_id: str, paused: bool) -> None:
        """暂停或恢复自动 dispatch。"""
        await asyncio.to_thread(self.store.set_paused, str(chat_id), paused)
        if not paused:
            await self.notify(str(chat_id))

    def active(self, chat_id: str) -> ActiveTurn | None:
        """读取当前群活动 turn 的只读快照。"""
        return self._active.get(str(chat_id))

    def active_by_lease(self, lease_id: str) -> ActiveTurn | None:
        """按 lease ID 查找活动 turn。"""
        return next(
            (turn for turn in self._active.values() if turn.lease.lease_id == str(lease_id)),
            None,
        )

    async def abandon(self, lease_id: str) -> bool:
        """停止指定 lease 的本进程续租，不修改其持久状态。"""
        return await self._detach_active(lease_id)

    async def _detach_active(self, lease_id: str) -> bool:
        """移除指定 lease 的本进程状态并等待 heartbeat 停止。"""
        lease_id = str(lease_id)
        active = self.active_by_lease(lease_id)
        if active is None:
            return False
        chat_id = str(active.lease.chat_id)
        heartbeat: asyncio.Task[None] | None = None
        lock = self._lock_for(chat_id)
        async with lock:
            current = self._active.get(chat_id)
            if current is None or current.lease.lease_id != lease_id:
                return False
            self._active.pop(chat_id, None)
            heartbeat = self._heartbeat_tasks.pop(chat_id, None)
        if heartbeat is not None and heartbeat is not asyncio.current_task():
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        return True

    def _recovery_scope(self) -> set[str] | frozenset[str] | tuple[str, ...] | None:
        """读取恢复允许目标；策略读取失败时返回空集合并 fail-closed。"""
        if self._recovery_chat_ids is None:
            return None
        try:
            result = self._recovery_chat_ids()
        except Exception:
            logger.warning("OneBot11 恢复白名单读取失败，跳过本轮恢复", exc_info=True)
            return set()
        return result if result is not None else set()

    def fence_active(self) -> tuple[QueueLease, ...]:
        """立即隔离所有内存活动 lease，供 adapter 在断开开始时调用。"""
        leases: list[QueueLease] = []
        for chat_id, active in list(self._active.items()):
            leases.append(active.lease)
            self._active[chat_id] = ActiveTurn(
                lease=active.lease,
                started_at=active.started_at,
                lease_lost=True,
            )
        return tuple(leases)

    async def close(self) -> None:
        """立即 fencing 内存活动 lease，再停止 heartbeat 和恢复任务。"""
        self._closed = True
        self.fence_active()
        recovery = self._recovery_task
        self._recovery_task = None
        if recovery is not None:
            recovery.cancel()
        recovery_dispatch = list(self._recovery_dispatch_tasks.values())
        self._recovery_dispatch_tasks.clear()
        for task in recovery_dispatch:
            task.cancel()
        if recovery is not None or recovery_dispatch:
            await asyncio.gather(
                *(task for task in [recovery, *recovery_dispatch] if task is not None),
                return_exceptions=True,
            )
        tasks = list(self._heartbeat_tasks.values())
        self._heartbeat_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._active.clear()
