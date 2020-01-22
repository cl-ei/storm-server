import json
import random
import aiohttp
from config import cloud_get_uid
from utils.dao import redis_cache
from config.log4 import api_logger as logging


async def get_available_cookie():
    key = "LT_AVAILABLE_COOKIES"
    r = await redis_cache.get(key)
    if r and isinstance(r, list):
        return random.choice(r)
    return ""


async def force_get_uid_by_name(user_name):
    cookie = await get_available_cookie()
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.post(cloud_get_uid, json={"cookie": cookie, "name": user_name}) as resp:
                status_code = resp.status
                content = await resp.text()
    except Exception as e:
        status_code = 5000
        content = f"Error: {e}"

    if status_code != 200:
        logging.error(f"Error happened when get_uid_by_name({user_name}), content: {content}.")
        return None

    try:
        r = json.loads(content)
        assert len(r) == 2
    except (json.JSONDecodeError, AssertionError) as e:
        logging.error(f"Error happened when get_uid_by_name({user_name}), e: {e}, content: {content}")
        return None

    flag, result = r
    if not flag:
        logging.error(f"Cannot get_uid_by_name by cloud_func, name: {user_name}, reason: {result}")
        return None
    return result

