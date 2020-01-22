import datetime
from utils.biliapi import BiliApi
from utils.dao import redis_cache
from utils.model import AsyncMySQL


async def get_send_gifts(user_obj):

    opened_guards = await AsyncMySQL.execute(
        "select g.room_id, g.gift_name, g.created_time "
        "from guard g "
        "where g.sender_obj_id = %s and g.created_time >= %s",
        (user_obj.id, datetime.datetime.now() - datetime.timedelta(days=45))
    )
    gifts = [r[:] for r in opened_guards]

    opened_lotteries = await AsyncMySQL.execute(
        "select g.room_id, g.gift_name, g.created_time "
        "from raffle g "
        "where g.sender_obj_id = %s and g.created_time >= %s",
        (user_obj.id, datetime.datetime.now() - datetime.timedelta(days=45))
    )
    gifts2 = [r[:] for r in opened_lotteries]
    gifts.extend(gifts2)

    guards_info = []
    if gifts:
        rooms_info = await AsyncMySQL.execute(
            "select real_room_id, short_room_id, name from biliuser where real_room_id in %s;",
            ([r[0] for r in gifts],)
        )
        room_id_map = {r[0]: r[1] for r in rooms_info if r[0] and r[1]}
        room_id_to_name = {r[0]: r[2] for r in rooms_info}

        def gen_time_prompt(interval):
            if interval > 3600 * 24:
                return f"约{int(interval // (3600 * 24))}天前"
            elif interval > 3600:
                return f"约{int(interval // 3600)}小时前"
            elif interval > 60:
                return f"约{int(interval // 60)}分钟前"
            return f"{int(interval)}秒前"

        now = datetime.datetime.now()
        group_by_room = {}
        for g in gifts:
            room_id, gift_name, created_time = g
            interval_prompt = gen_time_prompt((now - created_time).total_seconds())
            if room_id in group_by_room:
                gift = group_by_room[room_id]
            else:
                gift = {}
                group_by_room[room_id] = gift

            key = (gift_name, interval_prompt)
            if key in gift:
                gift[key] += 1
            else:
                gift[key] = 1

        sort_by_count = []
        for room_id, gifts in group_by_room.items():
            sort_by_count.append((len(gifts), room_id, gifts))
        sort_by_count.sort(key=lambda x: x[0], reverse=True)

        result = []
        for _, room_id, gifts in sort_by_count:
            master_name = room_id_to_name.get(room_id, "??")
            room_id = room_id_map.get(room_id, room_id)

            gifts_info = []
            for key, count in gifts.items():
                gift_name, interval_prompt = key
                gifts_info.append({
                    "gift_name": gift_name,
                    "interval_prompt": interval_prompt,
                    "count": count,
                })
            gifts_info.sort(key=lambda x: (x["gift_name"], x["interval_prompt"], x["count"]))

            for i, g in enumerate(gifts_info):
                if i == 0:
                    g.update({
                        "room_id": room_id,
                        "master_name": master_name,
                        "rowspan": len(gifts)
                    })
                result.append(g)

        guards_info = result
        print(guards_info)

    return guards_info


async def get_used_names(user_obj):
    q = await AsyncMySQL.execute(
        "select distinct sender_name from guard where sender_obj_id = %s", user_obj.id
    )
    n = {r[0] for r in q}

    q = await AsyncMySQL.execute(
        "select distinct sender_name from raffle where sender_obj_id = %s", user_obj.id
    )
    n |= {r[0] for r in q}
    return n


async def get_medal_info(user_obj):
    cache_key = f"USER_MEDAL_{user_obj.uid}"
    r = await redis_cache.get(cache_key)
    if r:
        return True, r

    now = datetime.datetime.now()
    freq_key = f"USER_MEDAL_FREQ_{now.hour}_{now.minute // 5}"
    count = redis_cache.incr(freq_key)
    redis_cache.expire(freq_key, timeout=60*5)
    if count > 10:
        return False, "服务器请求过多，请5分钟后再刷新。"

    flag, r = await BiliApi.get_user_medal_list(uid=user_obj.uid)
    if not flag or not isinstance(r, list) or not r:
        return False, r

    medal_list = sorted(r, key=lambda x: (x["level"], x["intimacy"]), reverse=True)
    await redis_cache.set(key=cache_key, value=medal_list, timeout=3600*36)
    return True, medal_list
