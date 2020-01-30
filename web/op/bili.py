import datetime
from utils.biliapi import BiliApi
from utils.dao import redis_cache
from utils.model import AsyncMySQL, BiliUser


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
    count = await redis_cache.incr(freq_key)
    redis_cache.expire(freq_key, timeout=60*5)
    if count > 10:
        return False, "服务器请求过多，请5分钟后再刷新。"

    flag, r = await BiliApi.get_user_medal_list(uid=user_obj.uid)
    if not flag or not isinstance(r, list) or not r:
        return False, r

    medal_list = sorted(r, key=lambda x: (x["level"], x["intimacy"]), reverse=True)
    medal_list = {"update_time": now, "medal_list": medal_list}
    await redis_cache.set(key=cache_key, value=medal_list, timeout=3600*36)
    return True, medal_list


async def query_raffles_by_user(user, day_range):
    now = datetime.datetime.now()
    raffle_start_record_time = now.replace(year=2019, month=7, day=2, hour=0, minute=0, second=0, microsecond=0)

    try:
        day_range = int(day_range)
        assert day_range > 1
    except (ValueError, TypeError, AssertionError):
        return "day_range参数错误。"

    end_date = now - datetime.timedelta(days=day_range)
    if end_date < raffle_start_record_time:
        total_days = int((now - raffle_start_record_time).total_seconds() / 3600 / 24)
        return f"day_range参数超出范围。最早可以查询2019年7月2日之后的记录，day_range范围 1 ~ {total_days}。"

    user_obj = await BiliUser.get_by_uid_or_name(user)
    if not user_obj:
        return f"未收录该用户: {user}"

    winner_obj_id, uid, user_name = user_obj.id, user_obj.uid, user_obj.name
    records = await AsyncMySQL.execute(
        (
            "select room_id, prize_gift_name, expire_time, sender_name, id from raffle "
            "where winner_obj_id = %s and expire_time > %s "
            "order by expire_time desc ;"
        ), (winner_obj_id, datetime.datetime.now() - datetime.timedelta(days=day_range))
    )
    if not records:
        return f"用户{uid} - {user_name} 在{day_range}天内没有中奖。"

    room_id_list = [row[0] for row in records]
    room_info = await AsyncMySQL.execute(
        (
            "select short_room_id, real_room_id, name "
            "from biliuser where real_room_id in %s;"
        ), (room_id_list, )
    )
    room_dict = {}
    for row in room_info:
        short_room_id, real_room_id, name = row
        room_dict[real_room_id] = (short_room_id, name)

    raffle_data = []
    for row in records:
        room_id, prize_gift_name, expire_time, sender_name, raffle_id = row
        short_room_id, master_name = room_dict.get(room_id, ("-", None))
        if short_room_id == room_id:
            short_room_id = "-"

        display_room_id = room_id
        if short_room_id and short_room_id not in ("-", "None", "NULL", "null"):
            display_room_id = short_room_id

        info = {
            "short_room_id": short_room_id,
            "real_room_id": room_id,
            "display_room_id": display_room_id,
            "raffle_id": raffle_id,
            "prize_gift_name": prize_gift_name,
            "sender_name": sender_name,
            "expire_time": str(expire_time),
            "master_name": master_name,
        }
        raffle_data.insert(0, info)

    context = {
        "uid": uid,
        "user_name": user_name,
        "day_range": day_range,
        "raffle_data": raffle_data,
    }
    return context
