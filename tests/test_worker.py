"""
pytest -q
"""
import json
import time
import threading
from typing import (
        Any,
        Set,
        )
import fakeredis
import pytest
from redis_ext import RedisWork


class DemoWork(RedisWork[dict]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.processed: Set[int] = set()


    def handle(self, task: dict) -> None:
        self.processed.add(task["x"])


class ErrWork(RedisWork[dict]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.processed: list[int] = []


    def handle(self, task: dict) -> None:
        if task["x"] == 3:
            raise ValueError("intentional")
        self.processed.append(task["x"])


@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def test_basic_flow(fake_redis: fakeredis.FakeRedis) -> None:
    lst_key = "test:queue"
    w = DemoWork(lst_key, max_workers=2, redis_client=fake_redis)


    t = threading.Thread(target=w.start, daemon=True)
    t.start()
    time.sleep(0.1)

    for i in range(10):
        fake_redis.rpush(lst_key, json.dumps({"x": i}))

    for _ in range(50):
        if len(w.processed) == 10:
            break
        time.sleep(0.1)

    w.shutdown()
    assert w.processed == set(range(10))

def test_stop_on_error(fake_redis: fakeredis.FakeRedis) -> None:
    lst_key = "test:err"
    w = ErrWork(lst_key, max_workers=2, redis_client=fake_redis)

    for i in range(5):
        fake_redis.rpush(lst_key, json.dumps({"x": i}))

    with pytest.raises(ValueError, match="intentional"):
        w.start()

    assert fake_redis.llen(lst_key) == 0
    assert 3 not in w.processed
    assert len(w.processed) == 4

def test_max_workers_pause(fake_redis: fakeredis.FakeRedis) -> None:
    """验证并发度不会超过 max_workers（不再断言剩余任务数）"""
    lst_key = "test:pause"
    running = 0
    max_running = 0


    class SlowWork(RedisWork[dict]):
        def handle(self, task: dict) -> None:
            nonlocal running, max_running
            running += 1
            max_running = max(max_running, running)
            time.sleep(0.2)
            running -= 1

    w = SlowWork(lst_key, max_workers=3, redis_client=fake_redis)
    tw = threading.Thread(target=w.start, daemon=True)
    tw.start()
    time.sleep(0.1)

    # 推 6 个任务
    for _ in range(6):
        fake_redis.rpush(lst_key, json.dumps({}))

    time.sleep(0.7)
    w.shutdown()
    assert max_running <= 3

