#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


from typing import (
        NewType,
        Union,
        )

from redis import Redis
from redis.cluster import RedisCluster


RedisClient = Union[Redis, RedisCluster]
