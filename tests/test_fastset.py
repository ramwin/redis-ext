import json
import time
from typing import Any, Dict, Set
import fakeredis
import pytest
from redis_ext.fastset import FastSet
@pytest.fixture
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)
class _MyDict:
    def __init__(self, d: Dict[str, Any]) -> None:
        self._d = d
    def __hash__(self) -> int:
        return hash(self._json())
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _MyDict) and self._d == other._d
    def __str__(self) -> str:
        return self._json()
    def _json(self) -> str:
        return json.dumps(self._d, sort_keys=True, indent=None)
    def __repr__(self) -> str:
        return f"_MyDict({self._d})"
def test_write_and_read(fake_redis: fakeredis.FakeRedis) -> None:
    fs = FastSet[int](fake_redis, "test", timeout=60)
    fs.add(1)
    fs.add(2)
    fs.add(1)
    assert len(fs) == 2
    assert fs.pop() in {1, 2}
    assert len(fs) == 1
def test_timeout_refresh(fake_redis: fakeredis.FakeRedis) -> None:
    fs = FastSet[str](fake_redis, "refresh", timeout=0.5)
    fs.add("a")
    time.sleep(0.6)
    fake_redis.sadd("refresh:value", '"b"')
    fake_redis.incr("refresh:version")
    assert "b" in fs
    assert "a" not in fs
def test_serde_sort_keys(fake_redis: fakeredis.FakeRedis) -> None:
    fs = FastSet[_MyDict](fake_redis, "serde", timeout=60)
    d1 = _MyDict({"x": 1, "y": 2})
    d2 = _MyDict({"y": 2, "x": 1})
    fs.add(d1)
    fs.add(d2)
    assert len(fs) == 1
def test_set_operations(fake_redis: fakeredis.FakeRedis) -> None:
    fs = FastSet[int](fake_redis, "ops", timeout=60)
    fs.update([1, 2, 3])
    assert fs & {2, 3, 4} == {2, 3}
    assert fs - {2} == {1, 3}
    assert fs | {4} == {1, 2, 3, 4}
    assert fs ^ {2, 4} == {1, 3, 4}
def test_version_increment(fake_redis: fakeredis.FakeRedis) -> None:
    fs = FastSet[int](fake_redis, "ver", timeout=60)
    init_ver = int(fake_redis.get("ver:version") or 0)
    fs.add(10)
    assert int(fake_redis.get("ver:version")) == init_ver + 1
    fs.discard(10)
    assert int(fake_redis.get("ver:version")) == init_ver + 2
def test_copy_and_comparison(fake_redis: fakeredis.FakeRedis) -> None:
    fs = FastSet[int](fake_redis, "cp", timeout=60)
    fs.update([1, 2])
    cp: Set[int] = fs.copy()
    assert cp == {1, 2}
    assert fs == cp
    assert fs.isdisjoint({3, 4})
    assert fs.issubset({1, 2, 3})
    assert fs.issuperset({1})
def test_clear(fake_redis: fakeredis.FakeRedis) -> None:
    fs = FastSet[int](fake_redis, "clr", timeout=60)
    fs.update([1, 2, 3])
    assert len(fs) == 3
    fs.clear()
    assert len(fs) == 0
    assert fake_redis.exists("clr:value") == 0
