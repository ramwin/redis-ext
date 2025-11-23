"""
pytest -q
需要安装：
  pip install fakeredis[lua] pytest
"""
import json
import time
import threading
import pytest
import fakeredis
from redis_ext import RedisWork


@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def test_basic_flow(fake_redis: fakeredis.FakeRedis) -> None:
    """端到端：push -> 消费 -> 验证结果"""
    lst_key = "test:queue"
    result_box: list[int] = []

    def handler(task: dict) -> None:
        result_box.append(task["x"])

    rw = RedisWork(lst_key, max_workers=2, redis_client=fake_redis)
    rw.register_handler(handler)

    # 后台启动监听线程
    t = threading.Thread(target=rw.start, daemon=True)
    t.start()
    time.sleep(0.1)  # 让监听线程跑起来

    # 推送 10 个任务
    for i in range(10):
        fake_redis.rpush(lst_key, json.dumps({"x": i}))

    # 等待全部任务被消费（最多 5 秒）
    for _ in range(50):
        if len(result_box) == 10:
            break
        time.sleep(0.1)

    rw.shutdown()
    assert len(result_box) == 10
    assert result_box == list(range(10))


def test_max_workers(fake_redis: fakeredis.FakeRedis) -> None:
    """验证并发度不会超过 max_workers"""
    lst_key = "test:queue2"
    running = 0
    max_running = 0

    def handler(_: dict) -> None:
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        time.sleep(0.2)
        running -= 1

    rw = RedisWork(lst_key, max_workers=3, redis_client=fake_redis)
    rw.register_handler(handler)

    t = threading.Thread(target=rw.start, daemon=True)
    t.start()
    time.sleep(0.1)

    # 一次推 6 个任务
    for _ in range(6):
        fake_redis.rpush(lst_key, json.dumps({}))

    time.sleep(0.5)
    rw.shutdown()
    assert max_running <= 3
