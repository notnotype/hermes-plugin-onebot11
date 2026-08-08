"""群级 dispatcher 的共享 lease 测试。"""

import asyncio
import threading

import pytest

from onebot11.dispatch import ActiveTurn, GroupDispatcher
from onebot11.queue import QueueMessage, QueueStore, TriggerRequest


def _message(message_id: str) -> QueueMessage:
    """构造 dispatcher 测试消息。"""
    return QueueMessage(
        chat_id="888",
        chat_type="group",
        message_id=message_id,
        user_id=message_id,
        user_name=f"用户{message_id}",
        text=f"消息{message_id}",
        message_key=f"group:{message_id}",
    )


async def test_同群多个用户串行启动独立锚点lease(tmp_path):
    """同群并发 notify 只能启动一个 turn，完成后再启动下一用户 anchor。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    for message_id in ("1", "2"):
        message = _message(message_id)
        store.enqueue(message, TriggerRequest.create("888", str(message.message_key), "mention", message.user_id, message.user_name))
    started = asyncio.Event()
    allow_return = asyncio.Event()
    leases = []

    async def start_turn(lease):
        leases.append(lease)
        started.set()
        await allow_return.wait()

    dispatcher = GroupDispatcher(store, start_turn)
    first = asyncio.create_task(dispatcher.notify("888"))
    await started.wait()
    assert await dispatcher.notify("888") is False
    assert len(leases) == 1
    assert [message.message_id for message in leases[0].messages] == ["1"]
    allow_return.set()
    assert await first
    assert await dispatcher.complete(leases[0].lease_id, outcome="success", unknown=False)
    assert store.status("888")["pending"] == 1

    started.clear()
    allow_return.clear()
    second = asyncio.create_task(dispatcher.notify("888"))
    await started.wait()
    assert len(leases) == 2
    assert [message.message_id for message in leases[1].messages] == ["2"]
    allow_return.set()
    assert await second
    assert await dispatcher.complete(leases[1].lease_id, outcome="success", unknown=False)
    assert store.status("888")["pending"] == 0
    await dispatcher.close()
    store.close()


async def test_其他进程退出后lease到期自动恢复(tmp_path, monkeypatch):
    """恢复进程不能抢活 lease，但应在失效后自动重新 dispatch。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    owner = QueueStore(path)
    store = QueueStore(path)
    message = _message("1")
    owner.enqueue(message, TriggerRequest.create("888", "group:1", "mention", "1", "用户1"))
    lease = owner.claim("888", lease_seconds=5)
    assert lease is not None
    assert owner.bind_authority(lease, "user", {"qq_get_message"}) is not None

    started = asyncio.Event()
    allow_return = asyncio.Event()
    recovered = []

    async def start_turn(recovered_lease):
        recovered.append(recovered_lease)
        started.set()
        await allow_return.wait()

    dispatcher = GroupDispatcher(store, start_turn, recovery_poll_seconds=0.05)
    assert await dispatcher.recover() == []
    now[0] = 1006.0
    assert await dispatcher.recover() == []
    now[0] = 1008.0
    await asyncio.wait_for(started.wait(), timeout=1)
    assert len(recovered) == 1
    assert recovered[0].lease_id != lease.lease_id
    allow_return.set()
    assert await dispatcher.complete(recovered[0].lease_id, outcome="success", unknown=False)
    await dispatcher.close()
    owner.close()
    store.close()


async def test_heartbeat异常会隔离旧turn(tmp_path, monkeypatch):
    """续租异常不能让旧 turn 继续调用工具或发送消息。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("1")
    store.enqueue(message, TriggerRequest.create("888", "group:1", "mention", "1", "用户1"))
    lease = store.claim("888", lease_seconds=10)
    assert lease is not None
    lost = asyncio.Event()

    async def on_lost(_lease):
        lost.set()

    def broken_renew(*_args, **_kwargs):
        raise OSError("sqlite unavailable")

    monkeypatch.setattr(store, "renew", broken_renew)
    dispatcher = GroupDispatcher(
        store,
        lambda _lease: asyncio.sleep(0),
        heartbeat_seconds=0.05,
        on_lease_lost=on_lost,
    )
    dispatcher._active["888"] = ActiveTurn(lease, lease.claimed_at)
    heartbeat = asyncio.create_task(dispatcher._heartbeat(lease))
    await asyncio.wait_for(lost.wait(), timeout=3)
    await heartbeat
    assert dispatcher.active("888") is not None
    assert dispatcher.active("888").lease_lost is True
    await dispatcher.close()
    store.close()


async def test_持久化完成失败会移除本地活动状态(tmp_path, monkeypatch):
    """ack 返回 False 时，失效 lease 不能继续阻塞同群恢复。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("ack-failure")
    store.enqueue(message, TriggerRequest.create("888", "group:ack-failure", "mention", "1", "用户1"))
    captured = []

    async def start_turn(lease):
        captured.append(lease)

    dispatcher = GroupDispatcher(store, start_turn)
    assert await dispatcher.notify("888")
    heartbeat = dispatcher._heartbeat_tasks["888"]
    assert store.bind_authority(captured[0], "user", {"qq_get_message"}) is not None
    monkeypatch.setattr(store, "ack", lambda _lease: False)
    assert not await dispatcher.complete(captured[0].lease_id, outcome="success", unknown=False)
    assert dispatcher.active("888") is None
    assert "888" not in dispatcher._heartbeat_tasks
    assert heartbeat.done()
    await dispatcher.close()
    store.close()


async def test_持久化完成异常按有限退避重试(tmp_path, monkeypatch):
    """持久状态转换的暂时异常按有界次数重试，成功后清理本地状态。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("ack-error")
    store.enqueue(
        message,
        TriggerRequest.create("888", "group:ack-error", "mention", "1", "用户1"),
    )
    captured = []

    async def start_turn(lease):
        captured.append(lease)

    dispatcher = GroupDispatcher(store, start_turn)
    assert await dispatcher.notify("888")
    heartbeat = dispatcher._heartbeat_tasks["888"]

    calls = 0

    def broken_ack(_lease):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OSError("sqlite temporarily unavailable")
        return True

    monkeypatch.setattr(store, "ack", broken_ack)
    monkeypatch.setattr("onebot11.dispatch._COMPLETION_RETRY_DELAYS", (0.0, 0.0, 0.0))
    assert await dispatcher.complete(captured[0].lease_id, outcome="success", unknown=False)
    assert calls == 3
    assert dispatcher.active("888") is None
    assert "888" not in dispatcher._heartbeat_tasks
    assert heartbeat.done()
    await dispatcher.close()
    store.close()


async def test_start_turn失败且complete异常后fence本地lease(tmp_path, monkeypatch):
    """启动失败后的 release 持久化耗尽时，也不能遗留 active 或 heartbeat。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("start-failure")
    store.enqueue(
        message,
        TriggerRequest.create("888", "group:start-failure", "mention", "1", "用户1"),
    )
    release_calls = 0

    async def start_turn(_lease):
        raise RuntimeError("Hermes turn failed before handoff")

    def broken_release(_lease, **_kwargs):
        nonlocal release_calls
        release_calls += 1
        raise OSError("sqlite temporarily unavailable")

    monkeypatch.setattr(store, "release", broken_release)
    monkeypatch.setattr("onebot11.dispatch._COMPLETION_RETRY_DELAYS", (0.0, 0.0, 0.0))
    dispatcher = GroupDispatcher(store, start_turn)

    with pytest.raises(RuntimeError, match="Hermes turn failed before handoff"):
        await dispatcher.notify("888")

    assert release_calls == 4
    assert dispatcher.active("888") is None
    assert dispatcher._heartbeat_tasks == {}
    await dispatcher.close()
    store.close()


async def test_complete_false后heartbeat停止且lease可被恢复接管(tmp_path, monkeypatch):
    """状态转换返回 False 后，旧 dispatcher 不再阻塞另一实例接管过期 lease。"""
    now = [1000.0]
    monkeypatch.setattr("onebot11.queue.time.time", lambda: now[0])
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    message = _message("fenced")
    store.enqueue(
        message,
        TriggerRequest.create("888", "group:fenced", "mention", "1", "用户1"),
    )
    captured = []

    async def start_turn(lease):
        captured.append(lease)

    dispatcher = GroupDispatcher(
        store,
        start_turn,
        lease_seconds=5,
        heartbeat_seconds=0.05,
    )
    assert await dispatcher.notify("888")
    heartbeat = dispatcher._heartbeat_tasks["888"]
    assert store.bind_authority(captured[0], "user", {"qq_get_message"}) is not None
    monkeypatch.setattr(store, "ack", lambda _lease: False)

    assert not await dispatcher.complete(captured[0].lease_id, outcome="success", unknown=False)
    assert dispatcher.active("888") is None
    assert heartbeat.done()

    now[0] = 1006.0
    owner = QueueStore(path)
    assert owner.claim("888", lease_seconds=5) is None
    now[0] = 1008.0
    recovered = owner.claim("888", lease_seconds=5)
    assert recovered is not None
    assert recovered.lease_id != captured[0].lease_id
    assert owner.ack(recovered)

    await dispatcher.close()
    owner.close()
    store.close()


async def test_recovery_loop单轮异常后继续恢复(tmp_path, monkeypatch):
    """一次恢复查询失败只能跳过当前轮，下一轮仍应认领最早 anchor。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    for message_id in ("recover-first", "recover-second"):
        message = _message(message_id)
        store.enqueue(
            message,
            TriggerRequest.create(
                "888",
                f"group:{message_id}",
                "mention",
                message.user_id,
                message.user_name,
            ),
        )
    original_recover = store.recover_trigger_requests
    recover_calls = 0

    def flaky_recover(allowed_chat_ids=None):
        nonlocal recover_calls
        recover_calls += 1
        if recover_calls == 1:
            raise OSError("temporary recovery failure")
        return original_recover(allowed_chat_ids)

    monkeypatch.setattr(store, "recover_trigger_requests", flaky_recover)
    started = asyncio.Event()
    leases = []

    async def start_turn(lease):
        leases.append(lease)
        started.set()

    dispatcher = GroupDispatcher(store, start_turn, recovery_poll_seconds=0.05)
    dispatcher._ensure_recovery_loop()
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert recover_calls >= 2
        assert len(leases) == 1
        assert [message.message_id for message in leases[0].messages] == ["recover-first"]
        assert leases[0].trigger.anchor_seq == leases[0].messages[0].seq
        assert await dispatcher.complete(
            leases[0].lease_id,
            outcome="success",
            unknown=False,
        )
    finally:
        await dispatcher.close()
        store.close()


async def test_shutdown后straggler完成不会访问已关闭SQLite(tmp_path):
    """dispatcher close 后旧 turn 只能被 fencing，不能再触碰 QueueStore。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("shutdown")
    store.enqueue(message, TriggerRequest.create("888", "group:shutdown", "mention", "1", "用户1"))
    lease = store.claim("888")
    assert lease is not None
    dispatcher = GroupDispatcher(store, lambda _lease: asyncio.sleep(0))
    dispatcher._active["888"] = ActiveTurn(lease, lease.claimed_at)
    await dispatcher.close()
    store.close()
    assert not await dispatcher.complete(lease.lease_id, outcome="success", unknown=False)


async def test_shutdown会取消正在claim的notify(tmp_path, monkeypatch):
    """dispatcher close 不应让尚未完成的 notify 在 QueueStore 关闭后继续运行。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("notify-shutdown")
    store.enqueue(
        message,
        TriggerRequest.create("888", "group:notify-shutdown", "mention", "1", "用户1"),
    )
    entered = threading.Event()
    release = threading.Event()

    def slow_claim(*_args, **_kwargs):
        entered.set()
        release.wait(timeout=2)
        return None

    monkeypatch.setattr(store, "claim", slow_claim)
    dispatcher = GroupDispatcher(store, lambda _lease: asyncio.sleep(0))
    notifying = asyncio.create_task(dispatcher.notify("888"))
    assert await asyncio.to_thread(entered.wait, 1)
    closing = asyncio.create_task(dispatcher.close())
    await asyncio.sleep(0.05)
    release.set()
    await closing
    with pytest.raises(asyncio.CancelledError):
        await notifying
    store.close()
