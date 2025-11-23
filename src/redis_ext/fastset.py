import json
import time
from typing import Any, Generic, Iterable, Iterator, Optional, Set, TypeVar, Union
from redis import Redis

T = TypeVar("T")
# 序列化/反序列化辅助
_dumps = lambda obj: json.dumps(obj, sort_keys=True, indent=None)
_loads = json.loads


class FastSet(Generic[T]):
    """
    本地 Python set + 远端 Redis set 双写，带版本号刷新。
    只在「距上次刷新 ≥ timeout」时才检查版本并可选刷新。
    所有行为与内置 set 保持一致。
    """

    def __init__(
        self,
        redis: Redis,
        key: str,
        timeout: float = 60,
        iterable: Optional[Iterable[T]] = None,
    ) -> None:
        self._r: Redis = redis
        self._key: str = key
        self._val_key: str = f"{key}:value"
        self._ver_key: str = f"{key}:version"
        self._timeout: float = timeout

        # 本地状态
        self._set: Set[T] = set()
        self._last_sync: float = 0.0

        if iterable:
            self.update(iterable)
            self._push_to_redis()

    # --------------------------------------------------
    # 内部工具
    # --------------------------------------------------
    def _need_refresh(self) -> bool:
        return time.time() - self._last_sync >= self._timeout

    def _fetch_from_redis(self) -> None:
        """强制把 Redis 数据拉回本地。"""
        raw_members = self._r.smembers(self._val_key)
        self._set = {_loads(r) for r in raw_members}
        self._last_sync = time.time()

    def _push_to_redis(self) -> None:
        """把本地 set 推到 Redis（覆盖式）。"""
        pipe = self._r.pipeline(transaction=True)
        pipe.delete(self._val_key)
        if self._set:
            pipe.sadd(self._val_key, *[_dumps(item) for item in self._set])
        # 版本号 +1
        pipe.incr(self._ver_key)
        pipe.execute()
        self._last_sync = time.time()

    def _maybe_refresh(self) -> None:
        if self._need_refresh():
            remote_ver: Optional[int] = self._r.get(self._ver_key)
            remote_ver = int(remote_ver) if remote_ver else None
            local_ver: Optional[int] = (
                int(self._r.get(self._ver_key)) if self._r.exists(self._ver_key) else None
            )
            # 简单比对：只要远端版本存在且 ≠ 本地初次缓存版本，就刷新
            if remote_ver is not None and remote_ver != local_ver:
                self._fetch_from_redis()

    # --------------------------------------------------
    # 只读接口：先 maybe_refresh 再返回
    # --------------------------------------------------
    def __len__(self) -> int:
        self._maybe_refresh()
        return len(self._set)

    def __contains__(self, item: object) -> bool:
        self._maybe_refresh()
        return item in self._set

    def __iter__(self) -> Iterator[T]:
        self._maybe_refresh()
        return iter(self._set)

    def __repr__(self) -> str:  # pragma: no cover
        self._maybe_refresh()
        return f"{self.__class__.__name__}({self._set!r})"

    # --------------------------------------------------
    # 可变接口：直接改本地 + 立即写回 Redis
    # --------------------------------------------------
    def add(self, item: T) -> None:
        self._set.add(item)
        self._push_to_redis()

    def discard(self, item: T) -> None:
        self._set.discard(item)
        self._push_to_redis()

    def remove(self, item: T) -> None:
        self._set.remove(item)
        self._push_to_redis()

    def pop(self) -> T:
        rv = self._set.pop()
        self._push_to_redis()
        return rv

    def clear(self) -> None:
        self._set.clear()
        self._push_to_redis()

    def update(self, iterable: Iterable[T]) -> None:
        self._set.update(iterable)
        self._push_to_redis()

    def intersection_update(self, *others: Iterable[Any]) -> None:
        self._set.intersection_update(*others)
        self._push_to_redis()

    def difference_update(self, *others: Iterable[Any]) -> None:
        self._set.difference_update(*others)
        self._push_to_redis()

    def symmetric_difference_update(self, other: Iterable[T]) -> None:
        self._set.symmetric_difference_update(other)
        self._push_to_redis()

    # --------------------------------------------------
    # 集合运算：返回新 set（不改动本地）
    # --------------------------------------------------
    def __or__(self, other: Iterable[T]) -> Set[T]:
        self._maybe_refresh()
        return self._set | set(other)

    def __and__(self, other: Iterable[Any]) -> Set[T]:
        self._maybe_refresh()
        return self._set & set(other)

    def __sub__(self, other: Iterable[Any]) -> Set[T]:
        self._maybe_refresh()
        return self._set - set(other)

    def __xor__(self, other: Iterable[T]) -> Set[T]:
        self._maybe_refresh()
        return self._set ^ set(other)

    # 可哈希元素支持
    def __eq__(self, other: object) -> bool:
        self._maybe_refresh()
        return self._set == other

    def __hash__(self) -> int:  # pragma: no cover
        raise TypeError("FastSet 本身不可哈希")

    # 额外工具
    def copy(self) -> Set[T]:
        self._maybe_refresh()
        return set(self._set)

    def isdisjoint(self, other: Iterable[Any]) -> bool:
        self._maybe_refresh()
        return self._set.isdisjoint(other)

    def issubset(self, other: Iterable[Any]) -> bool:
        self._maybe_refresh()
        return self._set.issubset(other)

    def issuperset(self, other: Iterable[Any]) -> bool:
        self._maybe_refresh()
        return self._set.issuperset(other)

# 方便模块级导入
__all__ = ["FastSet"]
