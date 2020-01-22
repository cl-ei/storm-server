import datetime
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
        temp = {}
        for g in gifts:
            room_id, gift_name, created_time = g
            short_room_id = room_id_map.get(room_id, room_id)
            interval_prompt = gen_time_prompt((now - created_time).total_seconds())
            master_name = room_id_to_name.get(room_id, "??")

            key = (short_room_id, gift_name, interval_prompt, master_name)
            if key in temp:
                temp[key] += 1
            else:
                temp[key] = 1

        for k, count in temp.items():
            guards_info.append({
                "room_id": k[0],
                "gift_name": k[1],
                "count": count,
                "interval_prompt": k[2],
                "master_name": k[3],
            })
        guards_info.sort(key=lambda x: (x["room_id"], x["interval_prompt"], x["gift_name"], x["count"]))

    return guards_info

