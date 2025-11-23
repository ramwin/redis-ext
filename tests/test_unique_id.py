import time
import fakeredis
import pytest
from redis_ext.unique_id import UniqueId
@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)
def test_basic_flow(fake_redis):
    uid = UniqueId(fake_redis, "task", timeout=2)
    assert uid.get_or_create("key1") == 1
    assert uid.get_or_create("key1") == 1  # 未过期
    assert uid.get_or_create("key2") == 2  # key2 占 ID 2
    time.sleep(2.1)  # key1 过期
    assert uid.get_or_create("key1") == 3  # 重新创建，ID 递增
def test_klass_isolation(fake_redis):
    uid1 = UniqueId(fake_redis, "task1", timeout=60)
    uid2 = UniqueId(fake_redis, "task2", timeout=60)
    assert uid1.get_or_create("same") == 1
    assert uid2.get_or_create("same") == 1
def test_custom_timeout(fake_redis):
    uid = UniqueId(fake_redis, "test", timeout=1)
    uid.get_or_create("key")
    time.sleep(1.1)
    assert uid.get_or_create("key") == 2
def test_concurrent_safety(fake_redis):
    uid = UniqueId(fake_redis, "conc", timeout=60)
    ids = [uid.get_or_create(f"k{i}") for i in range(5)]
    assert ids == [1, 2, 3, 4, 5]
def test_no_cache_interference(fake_redis):
    uid = UniqueId(fake_redis, "nocache", timeout=60)
    assert uid.get_or_create("a") == 1
    fake_redis.set("nocache:id:a", 999)
    assert uid.get_or_create("a") == 999  # 直接读取 Redis 值
