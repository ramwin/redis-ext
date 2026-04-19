import fakeredis
import pytest
from redis_ext.json_task_queue import JsonTaskQueue


@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def test_push_and_pop(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "test_queue")
    task1 = {"id": 1, "action": "send_email"}
    task2 = {"id": 2, "action": "send_sms"}

    assert queue.push(task1) is True
    assert queue.push(task2) is True
    assert len(queue) == 2

    tasks = queue.pop(count=1)
    assert len(tasks) == 1
    assert tasks[0] == task1
    assert len(queue) == 1


def test_deduplication(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "dedup_queue")
    task = {"user_id": 123, "action": "notify"}

    assert queue.push(task) is True
    assert queue.push(task) is False  # 重复添加
    assert len(queue) == 1


def test_dict_order_dedup(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "order_queue")
    task_a = {"b": 2, "a": 1}
    task_b = {"a": 1, "b": 2}

    assert queue.push(task_a) is True
    assert queue.push(task_b) is False  # 内容相同，顺序不同也视为重复


def test_pop_multiple(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "multi_queue")
    for i in range(5):
        queue.push({"id": i})

    tasks = queue.pop(count=3)
    assert len(tasks) == 3
    assert tasks[0] == {"id": 0}
    assert tasks[1] == {"id": 1}
    assert tasks[2] == {"id": 2}
    assert len(queue) == 2


def test_peek(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "peek_queue")
    task = {"msg": "hello"}
    queue.push(task)

    tasks = queue.peek(count=1)
    assert len(tasks) == 1
    assert tasks[0] == task
    assert len(queue) == 1  # peek 不移除


def test_remove(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "remove_queue")
    task = {"id": 99}
    queue.push(task)

    assert task in queue
    assert queue.remove(task) is True
    assert task not in queue
    assert len(queue) == 0


def test_clear(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "clear_queue")
    queue.push({"id": 1})
    queue.push({"id": 2})

    queue.clear()
    assert len(queue) == 0
    assert {"id": 1} not in queue


def test_complex_json(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "complex_queue")
    task = {
        "nested": {"key": "value", "list": [1, 2, 3]},
        "numbers": [42, 3.14],
        "boolean": True,
        "null_value": None,
    }

    assert queue.push(task) is True
    tasks = queue.pop(count=1)
    assert len(tasks) == 1
    assert tasks[0] == task


def test_pop_empty(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "empty_queue")
    tasks = queue.pop(count=1)
    assert tasks == []


def test_contains(fake_redis: fakeredis.FakeRedis) -> None:
    queue = JsonTaskQueue[dict](fake_redis, "contains_queue")
    task = {"key": "value"}

    assert task not in queue
    queue.push(task)
    assert task in queue
    queue.pop(count=1)
    assert task not in queue
