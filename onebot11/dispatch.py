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
        on_lease_lost: Callable[[QueueLease], Awaitable[None]] | None = None,
    ) -> None:
        """初始化调度器；lease 的续租由后台 heartbeat 负责。"""
        self.store = store
        self._start_turn = start_turn
        self.lease_seconds = max(5.0, float(lease_seconds))
        self.heartbeat_seconds = heartbeat_seconds or max(1.0, self.lease_seconds / 3)
        self.recovery_poll_seconds = max(0.05, float(recovery_poll_seconds))
        self._can_dispatch = can_dispatch or (lambda _chat_id: True)
        self._on_lease_lost = on_lease_lost
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
        except Exception:
            logger.exception("OneBot11 turn 启动失败: chat=%s lease=%s", chat_id, lease.lease_id)
            try:
                await self.complete(lease.lease_id, outcome="failure", unknown=False)
            except Exception:
                # 持久化失败时保留 durable lease，等过期恢复；不能用内存状态阻塞整群。
                logger.exception("OneBot11 turn 启动失败后的 lease 收口失败: %s", lease.lease_id)
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
        """按真实处理结果确认、释放或标记 uncertain，不在此处启动下一轮。"""
        lease_id = str(lease_id)
        active: ActiveTurn | None = None
        chat_id: str | None = None
        for candidate_chat, candidate in list(self._active.items()):
            if candidate.lease.lease_id == lease_id:
                chat_id = candidate_chat
                active = candidate
                break
        if active is None or chat_id is None:
            return False
        lock = self._lock_for(chat_id)
        async with lock:
            current = self._active.get(chat_id)
            if current is None or current.lease.lease_id != lease_id:
                return False
            task = self._heartbeat_tasks.pop(chat_id, None)
            if task is not None:
                task.cancel()
            if current.lease_lost:
                self._active.pop(chat_id, None)
                return False
            changed = False
            try:
                if unknown:
                    changed = await asyncio.to_thread(
                        self.store.mark_uncertain,
                        active.lease,
                        reason or "outbound result unknown",
                    )
                elif outcome == "success":
                    changed = await asyncio.to_thread(self.store.ack, active.lease)
                else:
                    changed = await asyncio.to_thread(
                        self.store.release,
                        active.lease,
                        reason=reason,
                        allow_after_outbound=known_failure,
                    )
            finally:
                # SQLite 异常时 durable lease 仍在库中；只清理内存引用，避免永远卡住该群。
                self._active.pop(chat_id, None)
        if not changed:
            logger.warning(
                "OneBot11 lease completion was fenced or already transitioned: %s",
                lease_id,
            )
        return changed

    async def recover(self) -> list[str]:
        """恢复过期 lease 和持久触发请求，返回已尝试 dispatch 的群。"""
        if self._closed:
            return []
        self._ensure_recovery_loop()
        requests = await asyncio.to_thread(self.store.recover_trigger_requests)
        chats: list[str] = []
        for request in requests:
            if self._can_dispatch(request.chat_id) and await self.notify(request.chat_id):
                chats.append(request.chat_id)
        return chats

    def _ensure_recovery_loop(self) -> None:
        """启动轻量恢复轮询，等待其他进程失效 lease 到期。"""
        if self._recovery_task is None or self._recovery_task.done():
            self._recovery_task = asyncio.create_task(self._recovery_loop())

    async def _recovery_loop(self) -> None:
        """周期恢复过期 lease，不抢占仍由其他进程持有的 lease。"""
        try:
            while not self._closed:
                await asyncio.sleep(self.recovery_poll_seconds)
                requests = await asyncio.to_thread(self.store.recover_trigger_requests)
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
            logger.exception("OneBot11 lease 恢复轮询失败")

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

    async def set_paused(
        self,
        chat_id: str,
        paused: bool,
        *,
        notify_on_resume: bool = True,
    ) -> None:
        """暂停或恢复自动 dispatch；调用方可延迟恢复通知。"""
        await asyncio.to_thread(self.store.set_paused, str(chat_id), paused)
        if not paused and notify_on_resume:
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

    async def close(self) -> None:
        """停止 heartbeat；不擅自改变 lease 状态，交给恢复流程处理。"""
        self._closed = True
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

    async def reopen(self) -> None:
        """为同一 adapter 的 reconnect 清空内存状态并重新允许 dispatch。"""
        self._closed = False
        self._active.clear()
        self._heartbeat_tasks.clear()
        self._recovery_dispatch_tasks.clear()
        self._locks.clear()
