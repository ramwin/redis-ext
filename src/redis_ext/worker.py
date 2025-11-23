import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional, Union
import redis
from redis import Redis

logger = logging.getLogger(__name__)


class RedisWork:
    """
    监听 Redis list，弹出任务后在线程池中并发处理。
    支持外部注入 redis.Redis 实例，也可通过 host/port 创建。
    """

    def __init__(
        self,
        list_key: str,
        max_workers: int = 4,
        redis_client: Optional[Redis] = None,
        *,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        timeout: int = 5,
        decode_responses: bool = True,
    ):
        self.list_key = list_key
        self.timeout = timeout
        if redis_client is None:
            self.rdb = Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=decode_responses,
            )
        else:
            self.rdb = redis_client

        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._stop_evt = threading.Event()

    # ---------------- 公共 API ----------------
    def register_handler(self, func: Callable[[dict], Any]) -> None:
        """注册任务处理函数，仅接受一个 dict 参数。"""
        self._handler = func

    def start(self) -> None:
        """阻塞式启动；捕捉到 KeyboardInterrupt 会优雅退出。"""
        logger.info("RedisWork started, listening '%s'", self.list_key)
        try:
            while not self._stop_evt.is_set():
                item = self.rdb.blpop(self.list_key, timeout=self.timeout)
                if item is None:           # 超时继续
                    continue
                _, raw = item
                try:
                    task = json.loads(raw)
                except Exception as exc:
                    logger.error("Bad task format: %s", raw, exc_info=exc)
                    continue
                self.executor.submit(self._safe_handler, task)
        except KeyboardInterrupt:
            logger.info("Caught Ctrl-C, shutting down…")
        finally:
            self.shutdown()

    def shutdown(self, wait: bool = True) -> None:
        """关闭线程池，停止监听。"""
        self._stop_evt.set()
        self.executor.shutdown(wait=wait)
        logger.info("RedisWork stopped.")

    # ---------------- 内部 ----------------
    def _safe_handler(self, task: dict) -> None:
        try:
            self._handler(task)
        except Exception as exc:
            logger.exception("Task handler failed: %s", exc)
