"""群级 dispatcher 的共享 lease 测试。"""

import asyncio

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


async def test_同群多个用户只有一个活动lease(tmp_path):
    """并发 notify 只能启动一个共享群 turn，多个 anchor 串行认领。"""
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
    assert await dispatcher.notify("888")
    assert [message.message_id for message in leases[1].messages] == ["2"]
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
    await asyncio.sleep(0.1)
    now[0] = 1008.1
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


async def test_持久化完成失败不会报告完成(tmp_path, monkeypatch):
    """ack 没有真正写入时，dispatcher 不能让 adapter 推进下一轮。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("ack-failure")
    store.enqueue(message, TriggerRequest.create("888", "group:ack-failure", "mention", "1", "用户1"))
    captured = []

    async def start_turn(lease):
        captured.append(lease)

    dispatcher = GroupDispatcher(store, start_turn)
    assert await dispatcher.notify("888")
    monkeypatch.setattr(store, "ack", lambda _lease: False)
    assert not await dispatcher.complete(captured[0].lease_id, outcome="success", unknown=False)
    assert dispatcher.active("888") is None
    await dispatcher.close()
    store.close()


async def test_reopen会取消旧heartbeat和恢复任务(tmp_path):
    """dispatcher reopen 不能只清空字典，旧后台 task 必须真正结束。"""
    store = QueueStore(tmp_path / "queue.sqlite3")
    message = _message("reopen")
    store.enqueue(
        message,
        TriggerRequest.create("888", "group:reopen", "mention", "1", "用户1"),
    )
    lease = store.claim("888")
    assert lease is not None
    dispatcher = GroupDispatcher(store, lambda _lease: asyncio.sleep(0))
    dispatcher._active["888"] = ActiveTurn(lease, lease.claimed_at)
    heartbeat = asyncio.create_task(dispatcher._heartbeat(lease))
    dispatcher._heartbeat_tasks["888"] = heartbeat
    await dispatcher.reopen()
    assert heartbeat.done()
    assert dispatcher.active("888") is None
    assert not dispatcher._closed
    await dispatcher.close()
    store.close()
