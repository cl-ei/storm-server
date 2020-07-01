import time
import json
import random
import pickle
import asyncio
import aioredis
import configparser
from config import REDIS_CONFIG
from typing import Dict, Any, Union, List, Iterable, Sequence

PKL_PROTOCOL = pickle.DEFAULT_PROTOCOL


class RedisCache(object):
    def __init__(self, host, port, db, password):
        self.uri = f'redis://{host}:{port}'
        self.db = db
        self.password = password
        self.redis_conn = None

    async def execute(self, *args, **kwargs):
        if self.redis_conn is None:
            self.redis_conn = await aioredis.create_redis_pool(
                address=self.uri,
                db=self.db,
                password=self.password
            )
        return await self.redis_conn.execute(*args, **kwargs)

    async def close(self):
        if self.redis_conn is not None:
            self.redis_conn.close()
            await self.redis_conn.wait_closed()
            self.redis_conn = None

    async def non_repeated_save(self, key, info, ex=3600*24*7):
        return await self.execute("set", key, json.dumps(info), "ex", ex, "nx")

    async def keys(self, pattern):
        keys = await self.execute("keys", pattern)
        return [k.decode("utf-8") for k in keys]

    async def set(self, key, value, timeout=0, _un_pickle=False):
        v = value if _un_pickle else pickle.dumps(value)
        if timeout > 0:
            return await self.execute("setex", key, timeout, v)
        else:
            return await self.execute("set", key, v)

    async def expire(self, key, timeout):
        if timeout > 0:
            return await self.execute("EXPIRE", key, timeout)

    async def set_if_not_exists(self, key, value, timeout=3600*24*7):
        v = pickle.dumps(value)
        return await self.execute("set", key, v, "ex", timeout, "nx")

    async def delete(self, key):
        return await self.execute("DEL", key)

    async def ttl(self, key):
        return await self.execute("ttl", key)

    async def get(self, key, _un_pickle=False):
        r = await self.execute("get", key)
        if _un_pickle:
            return r

        try:
            return pickle.loads(r)
        except (TypeError, pickle.UnpicklingError):
            return r

    async def mget(self, *keys, _un_pickle=False):
        r = await self.execute("MGET", *keys)

        if _un_pickle:
            return r

        result = []
        for _ in r:
            if _ is None:
                result.append(None)
                continue

            try:
                _ = pickle.loads(_)
            except (TypeError, pickle.UnpicklingError):
                _ = TypeError("UnpicklingError")
            result.append(_)
        return result

    async def hash_map_set(self, name, key_values):
        args = []
        for key, value in key_values.items():
            args.append(pickle.dumps(key))
            args.append(pickle.dumps(value))
        return await self.execute("hmset", name, *args)

    async def hash_map_get(self, name, *keys):
        if keys:
            r = await self.execute("hmget", name, *[pickle.dumps(k) for k in keys])
            if not isinstance(r, list) or len(r) != len(keys):
                raise Exception(f"Redis hash map read error! r: {r}")

            result = [pickle.loads(_) for _ in r]
            return result[0] if len(result) == 1 else result

        else:
            """HDEL key field1 [field2] """
            r = await self.execute("hgetall", name)
            if not isinstance(r, list):
                raise Exception(f"Redis hash map read error! r: {r}")

            result = {}
            key_temp = None
            for index in range(len(r)):
                if index & 1:
                    result[pickle.loads(key_temp)] = pickle.loads(r[index])
                else:
                    key_temp = r[index]
            return result

    async def list_push(self, name, *items):
        r = await self.execute("LPUSH", name, *[pickle.dumps(e) for e in items])
        return r

    async def list_rpop_to_another_lpush(self, source_list_name, dist_list_name):
        r = await self.execute("RPOPLPUSH", source_list_name, dist_list_name)
        if not r:
            return None
        return pickle.loads(r)

    async def list_del(self, name, item):
        r = await self.execute("LREM", name, 0, pickle.dumps(item))
        return r

    async def list_get_all(self, name):
        # count = await self.execute("LLEN", name)

        r = await self.execute("LRANGE", name, 0, 100000)
        if isinstance(r, list):
            return [pickle.loads(e) for e in r]
        return []

    async def list_rpop(self, name):
        v = await self.execute("RPOP", name)
        if v is None:
            return None
        return pickle.loads(v)

    async def list_br_pop(self, *names, timeout=10):
        r = await self.execute("BRPOP", *names, "LISTN", timeout)
        if r is None:
            return None
        return r[0], pickle.loads(r[1])

    async def set_add(self, name, *items):
        r = await self.execute("SADD", name, *[pickle.dumps(e) for e in items])
        return r

    async def set_remove(self, name, *items):
        r = await self.execute("SREM", name, *[pickle.dumps(e) for e in items])
        return r

    async def set_is_member(self, name, item):
        return await self.execute("SISMEMBER", name, pickle.dumps(item))

    async def set_get_all(self, name):
        r = await self.execute("SMEMBERS", name)
        if isinstance(r, list):
            return [pickle.loads(e) for e in r]
        return []

    async def set_get_count(self, name):
        r = await self.execute("SCARD", name)
        return r

    async def incr(self, key):
        return await self.execute("INCR", key)

    async def zset_zadd(self, key: str, member_pairs: Iterable[Sequence[Any, float]], _un_pickle=False):
        """
        向有序集合添加一个或多个成员，或者更新已存在成员的分数

        ZADD key score1 member1 [score2 member2]

        """
        safe_args = []
        for member, score in member_pairs:
            if not _un_pickle:
                member = pickle.dumps(member, protocol=PKL_PROTOCOL)
            safe_args.extend([float(score), member])
        return await self.execute("ZADD", key, *safe_args)

    async def zset_zcard(self, key) -> int:
        """ 获取有序集合的成员数 """
        return await self.execute("ZCARD", key)

    async def zset_zrange_by_score(
            self,
            key: str,
            min_: Union[str, float] = "-inf",
            max_: Union[str, float] = "+inf",
            offset: int = 0,
            limit: int = 10000,
            _un_pickle: bool = False,
    ) -> Iterable[Sequence[Any, float]]:
        """
        通过分数返回有序集合指定区间内的成员

        ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT]
        """
        result = await self.execute(
            "ZRANGEBYSCORE", key, min_, max_, "WITHSCORES",
            "limit", offset, limit
        )

        return_data = []
        temp_obj = None
        for i, data in enumerate(result):
            if i % 2 == 0:  # member
                if not _un_pickle:
                    data = pickle.loads(data)
                temp_obj = data
            else:
                return_data.append((temp_obj, float(data)))
        return return_data

    async def zset_zrem(self, key, *members, _un_pickle=False):
        """
        移除有序集合中的一个或多个成员

        ZREM key member [member ...]
        """
        if not _un_pickle:
            members = [
                pickle.dumps(m, protocol=PKL_PROTOCOL)
                for m in members
            ]
        return await self.execute("ZREM", key, *members)

    async def zset_zrem_by_score(
            self,
            key: str,
            min_: Union[str, float],
            max_: Union[str, float]
    ) -> int:
        """
        移除有序集合中给定的分数区间的所有成员

        ZREMRANGEBYSCORE key min max
        """
        return await self.execute("ZREMRANGEBYSCORE", key, min_, max_)

    async def zset_zscore(self, key: str, member: Any, _un_pickle: bool = False):
        """
        返回有序集中，成员的分数值

        ZSCORE key member
        """
        if not _un_pickle:
            member = pickle.dumps(member, protocol=PKL_PROTOCOL)
        return await self.execute("ZSCORE", key, member)


redis_cache = RedisCache(**REDIS_CONFIG)


async def gen_x_node_redis() -> RedisCache:
    config_file = "/etc/madliar.settings.ini"
    config = configparser.ConfigParser()
    config.read(config_file)
    redis = RedisCache(**{
        "host": config["xnode_redis"]["host"],
        "port": int(config["xnode_redis"]["port"]),
        "password": config["xnode_redis"]["password"],
        "db": int(config["xnode_redis"]["stormgift_db"]),
    })
    return redis


class XNodeRedis:
    def __init__(self):
        self._x_node_redis = None

    async def __aenter__(self) -> RedisCache:
        self._x_node_redis = await gen_x_node_redis()
        return self._x_node_redis

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._x_node_redis.close()


class RedisLock:
    def __init__(self, key, timeout=30):
        self.key = f"LT_LOCK_{key}"
        self.timeout = timeout

    async def __aenter__(self):
        while True:
            lock = await redis_cache.set_if_not_exists(key=self.key, value=1, timeout=self.timeout)
            if lock:
                return self
            else:
                await asyncio.sleep(0.2 + random.random())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await redis_cache.delete(self.key)


class ValuableLiveRoom(object):
    _key = "VALUABLE_LIVE_ROOM_LIST"

    @classmethod
    async def set(cls, room_id_list):
        if not room_id_list:
            return False
        value = "_".join([str(room_id) for room_id in room_id_list])
        return await redis_cache.set(cls._key, value=value, _un_pickle=True)

    @classmethod
    async def get_all(cls):
        value = await redis_cache.get(cls._key, _un_pickle=True)
        if isinstance(value, bytes):
            value = value.decode()

        if not isinstance(value, str):
            return []

        de_dup = set()
        result = []
        for room_id in value.split("_"):
            try:
                room_id = int(room_id)
            except (TypeError, ValueError):
                continue

            if room_id <= 0:
                continue

            if room_id in de_dup:
                continue
            de_dup.add(room_id)
            result.append(room_id)
        return result


class InLotteryLiveRooms(object):
    _key = "IN_LOTTERY_LIVE_ROOM"
    time_out = 60*10

    @classmethod
    async def add(cls, room_id):
        old = await redis_cache.get(cls._key)
        if not isinstance(old, dict):
            old = dict()

        old[room_id] = time.time()
        return await redis_cache.set(cls._key, old)

    @classmethod
    async def get_all(cls) -> set:
        room_dict = await redis_cache.get(cls._key)
        if not isinstance(room_dict, dict):
            return set()

        result = {}
        now = time.time()
        changed = False
        for room_id, timestamp in room_dict.items():
            if now - timestamp < cls.time_out:
                result[room_id] = timestamp
            else:
                changed = True

        if changed:
            await redis_cache.set(cls._key, result)

        return set(result.keys())


class MonitorLiveRooms(object):
    _key = "MonitorLiveRooms_KEY"

    @classmethod
    async def get(cls) -> set:
        r = await redis_cache.get(cls._key)
        if not r or not isinstance(r, set):
            return set()
        return r

    @classmethod
    async def set(cls, live_room_id_set: set):
        live_room_id_set = {
            int(room_id) for room_id in live_room_id_set
            if room_id not in (0, "0", None, "")
        }
        return await redis_cache.set(cls._key, live_room_id_set)


class RedisGuard:
    key = "LT_GUARD"

    @classmethod
    async def add(cls, raffle_id, value):
        key = f"{cls.key}_{raffle_id}"
        await redis_cache.set(key, value, timeout=24 * 3600 * 7)

    @classmethod
    async def get_all(cls, redis=None):
        if redis:
            keys = await redis.keys(f"{cls.key}_*")
            if not keys:
                return []

            values = await redis.mget(*keys)
            return values
        else:
            async with XNodeRedis() as redis:
                keys = await redis.keys(f"{cls.key}_*")
                if not keys:
                    return []

                values = await redis.mget(*keys)
                return values

    @classmethod
    async def delete(cls, *raffle_ids, redis=None):
        if redis:
            for raffle_id in raffle_ids:
                await redis.delete(f"{cls.key}_{raffle_id}")
        else:
            async with XNodeRedis() as redis:
                for raffle_id in raffle_ids:
                    await redis.delete(f"{cls.key}_{raffle_id}")


class RedisRaffle:
    key = "LT_RAFFLE"

    @classmethod
    async def add(cls, raffle_id, value, _pre=False):
        key = f"{cls.key}_{raffle_id}"
        await redis_cache.set(key, value, timeout=24*3600*7)

        if _pre:
            key = f"LT_PRE_RAFFLE_{raffle_id}"
            await redis_cache.set(key, value, timeout=60*20)

    @classmethod
    async def get(cls, raffle_id):
        key = f"LT_PRE_RAFFLE_{raffle_id}"
        return await redis_cache.get(key)

    @classmethod
    async def get_all(cls, redis=None):
        if redis:
            keys = await redis.keys(f"{cls.key}_*")
            if not keys:
                return []

            values = await redis.mget(*keys)
            return values
        else:
            async with XNodeRedis() as redis:
                keys = await redis.keys(f"{cls.key}_*")
                if not keys:
                    return []

                values = await redis.mget(*keys)
                return values

    @classmethod
    async def delete(cls, *raffle_ids, redis=None):
        if redis:
            for raffle_id in raffle_ids:
                await redis.delete(f"{cls.key}_{raffle_id}")
        else:
            async with XNodeRedis() as redis:
                for raffle_id in raffle_ids:
                    await redis.delete(f"{cls.key}_{raffle_id}")


class RedisAnchor:
    key = "LT_ANCHOR"

    @classmethod
    async def add(cls, raffle_id, value):
        key = f"{cls.key}_{raffle_id}"
        await redis_cache.set(key, value, timeout=24*3600*7)

    @classmethod
    async def get_all(cls, redis=None):
        if redis:
            keys = await redis.keys(f"{cls.key}_*")
            if not keys:
                return []

            values = await redis.mget(*keys)
            return values
        else:
            async with XNodeRedis() as redis:
                keys = await redis.keys(f"{cls.key}_*")
                if not keys:
                    return []

                values = await redis.mget(*keys)
                return values

    @classmethod
    async def delete(cls, *raffle_ids, redis=None):
        if redis:
            for raffle_id in raffle_ids:
                await redis.delete(f"{cls.key}_{raffle_id}")
        else:
            async with XNodeRedis() as redis:
                for raffle_id in raffle_ids:
                    await redis.delete(f"{cls.key}_{raffle_id}")


async def test():
    pass


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
