import json
import time
from typing import Any, Generic, Iterable, Iterator, Optional, Set, TypeVar, Union
from redis import Redis
T = TypeVar("T")
_dumps = lambda obj: json.dumps(obj, sort_keys=True, indent=None)
_loads = json.loads
class FastSet(Generic[T]):
    def __init__(self, redis: Redis, key: str, timeout: float = 60, iterable: Optional[Iterable[T]] = None) -> None:
        self._r = redis
        self._key = key
        self._val_key = f"{key}:value"
        self._ver_key = f"{key}:version"
        self._timeout = timeout
        self._set: Set[T] = set()
        self._ver: int = 0
        self._last_check = 0.0
        if iterable:
            self.update(iterable)
    def _need_check(self) -> bool:
        return time.time() - self._last_check >= self._timeout
    def _fetch(self) -> None:
        pipe = self._r.pipeline()
        pipe.smembers(self._val_key)
        pipe.get(self._ver_key)
        raw, ver = pipe.execute()
        self._set = {_loads(r) for r in raw}
        self._ver = int(ver) if ver else 0
        self._last_check = time.time()
    def _maybe_refresh(self) -> None:
        if self._need_check():
            remote_ver = self._r.get(self._ver_key)
            remote_ver = int(remote_ver) if remote_ver else 0
            if remote_ver != self._ver:
                self._fetch()
    def _pipeline_single(self, op: str, *args: Any) -> bool:
        while True:
            try:
                pipe = self._r.pipeline()
                pipe.watch(self._ver_key)
                remote_ver = pipe.get(self._ver_key)
                remote_ver = int(remote_ver) if remote_ver else 0
                if remote_ver != self._ver:
                    self._fetch()
                    return False
                pipe.multi()
                if op == "sadd":
                    pipe.sadd(self._val_key, *args)
                elif op == "srem":
                    pipe.srem(self._val_key, *args)
                pipe.incr(self._ver_key)
                new_ver = pipe.execute()[-1]
                self._ver = new_ver
                self._last_check = time.time()
                return True
            except Exception:
                self._fetch()
                return False
    def add(self, item: T) -> None:
        if item in self._set:
            return
        ser = _dumps(item)
        if self._pipeline_single("sadd", ser):
            self._set.add(item)
        else:
            self._maybe_refresh()
            if item not in self._set:
                self.add(item)
    def discard(self, item: T) -> None:
        if item not in self._set:
            return
        ser = _dumps(item)
        if self._pipeline_single("srem", ser):
            self._set.discard(item)
        else:
            self._maybe_refresh()
            if item in self._set:
                self.discard(item)
    def remove(self, item: T) -> None:
        if item not in self._set:
            raise KeyError(item)
        self.discard(item)
    def pop(self) -> T:
        if not self._set:
            raise KeyError("pop from empty set")
        item = next(iter(self._set))
        self.discard(item)
        return item
    def clear(self) -> None:
        pipe = self._r.pipeline()
        pipe.delete(self._val_key)
        pipe.incr(self._ver_key)
        new_ver = pipe.execute()[-1]
        self._set.clear()
        self._ver = new_ver
        self._last_check = time.time()
    def update(self, iterable: Iterable[T]) -> None:
        new_items = {i for i in iterable if i not in self._set}
        if not new_items:
            return
        sers = [_dumps(i) for i in new_items]
        pipe = self._r.pipeline()
        pipe.sadd(self._val_key, *sers)
        pipe.incr(self._ver_key)
        new_ver = pipe.execute()[-1]
        self._set.update(new_items)
        self._ver = new_ver
        self._last_check = time.time()
    def __len__(self) -> int:
        self._maybe_refresh()
        return len(self._set)
    def __contains__(self, item: object) -> bool:
        self._maybe_refresh()
        return item in self._set
    def __iter__(self) -> Iterator[T]:
        self._maybe_refresh()
        return iter(self._set)
    def __repr__(self) -> str:
        self._maybe_refresh()
        return f"{self.__class__.__name__}({self._set!r})"
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
    def __and__(self, other: Iterable[Any]) -> Set[T]:
        self._maybe_refresh()
        return self._set & set(other)
    def __or__(self, other: Iterable[T]) -> Set[T]:
        self._maybe_refresh()
        return self._set | set(other)
    def __sub__(self, other: Iterable[Any]) -> Set[T]:
        self._maybe_refresh()
        return self._set - set(other)
    def __xor__(self, other: Iterable[T]) -> Set[T]:
        self._maybe_refresh()
        return self._set ^ set(other)
    def __eq__(self, other: object) -> bool:
        self._maybe_refresh()
        return self._set == other
