"""队列模块测试:按群分桶、长度截断、快照与清空。"""
from onebot11.queue import GroupMessageQueue


def test_按群分桶互不影响():
    q = GroupMessageQueue()
    q.push("g1", "a", "1", "小明", 1.0)
    q.push("g2", "b", "2", "小红", 2.0)
    assert [m.text for m in q.snapshot("g1")] == ["a"]
    assert [m.text for m in q.snapshot("g2")] == ["b"]


def test_超长单条截断():
    q = GroupMessageQueue(max_chars_per_entry=10)
    q.push("g1", "x" * 100, "1", "小明", 1.0)
    msg = q.snapshot("g1")[0]
    assert len(msg.text) <= 11  # 10 字符 + 省略号


def test_超过上限丢最旧():
    q = GroupMessageQueue(max_entries=3)
    for i in range(5):
        q.push("g1", f"m{i}", "1", "小明", float(i))
    assert [m.text for m in q.snapshot("g1")] == ["m2", "m3", "m4"]


def test_清空后为空():
    q = GroupMessageQueue()
    q.push("g1", "a", "1", "小明", 1.0)
    q.clear("g1")
    assert q.snapshot("g1") == []
