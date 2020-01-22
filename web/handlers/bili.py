import time
import json
import copy
import datetime
from aiohttp import web
from jinja2 import Template
from config import CDN_URL
from utils.model import AsyncMySQL, BiliUser


def render_to_response(template, context=None):
    try:
        with open(template, encoding="utf-8") as f:
            template_context = f.read()
    except IOError:
        template_context = "<center><h3>Template Does Not Existed!</h3></center>"

    template = Template(template_context)
    return web.Response(text=template.render(context or {}), content_type="text/html")


def json_response(data):
    return web.Response(text=json.dumps(data), content_type="application/json")


async def guards(request):
    start_time = time.time()
    json_req = request.query.get("json")
    try:
        time_delta = int(request.query["time_delta"])
        assert 0 < time_delta < 3600*36
    except (ValueError, TypeError, KeyError, AssertionError):
        time_delta = 0

    try:
        start_time = int(request.query["start_time"])
        expire_time = datetime.datetime.fromtimestamp(start_time)
    except (ValueError, TypeError, KeyError, AssertionError):
        expire_time = datetime.datetime.now()

    expire_time = expire_time - datetime.timedelta(seconds=time_delta)

    guard_records = await AsyncMySQL.execute(
        (
            "select id, room_id, gift_name, sender_name, expire_time "
            "from guard where expire_time > %s and gift_name in %s;"
        ), (expire_time, ("总督", "提督", "舰长"))
    )
    room_id_list = [row[1] for row in guard_records] or [-1]

    room_info = await AsyncMySQL.execute(
        (
            "select name, short_room_id, real_room_id "
            "from biliuser where real_room_id in %s;"
        ), (room_id_list, )
    )
    room_dict = {}
    for row in room_info:
        name, short_room_id, real_room_id = row
        room_dict[real_room_id] = (name, short_room_id)

    def get_price(g):
        price_map = {
            "小电视飞船": 1250,
            "任意门": 600,
            "幻乐之声": 520,
            "摩天大楼": 450,
            "总督": -1,
            "提督": -2,
            "舰长": -3
        }
        return price_map.get(g, 0)

    records = []
    for row in guard_records:
        raffle_id, room_id, gift_name, sender_name, expire_time = row
        master_name, short_room_id = room_dict.get(room_id, (None, None))
        if short_room_id == room_id:
            short_room_id = "-"

        records.append({
            "gift_name": gift_name.replace("抽奖", ""),
            "short_room_id": short_room_id,
            "real_room_id": room_id,
            "master_name": master_name,
            "sender_name": sender_name,
            "raffle_id": raffle_id,
            "expire_time": str(expire_time),
        })
    records.sort(key=lambda x: (get_price(x["gift_name"]), x["real_room_id"]), reverse=True)
    db_query_time = time.time() - start_time
    update_time = time.time()
    update_datetime_str = f"{datetime.datetime.fromtimestamp(update_time)}"
    hash_str = f"{hash(update_datetime_str):0x}"[:8]
    e_tag = f"{update_datetime_str[:19]}-{hash_str}"
    if json_req:
        response = json.dumps({"code": 0, "e_tag": e_tag, "list": records})
        return web.Response(text=response, content_type="application/json")

    context = {
        "e_tag": e_tag,
        "records": records,
        "proc_time": f"{(time.time() - start_time):.3f}",
        "db_query_time": f"{db_query_time:.3f}",
    }
    return render_to_response("web/templates/guards.html", context=context)


async def raffles(request):
    user = request.query.get("user")
    if user:
        return await query_raffles_by_user(request, user)

    json_req = request.query.get("json")
    try:
        page_size = int(request.query["page_size"])
        assert 0 < page_size <= 100000
    except (ValueError, TypeError, KeyError, AssertionError):
        page_size = 1000
    update_time = time.time()
    update_datetime_str = f"{datetime.datetime.fromtimestamp(update_time)}"
    hash_str = f"{hash(update_datetime_str):0x}"[:8]
    e_tag = f"{update_datetime_str[:19]}-{hash_str}"

    start_date = datetime.datetime.now() - datetime.timedelta(hours=48)
    records = await AsyncMySQL.execute(
        (
            "select id, room_id, gift_name, gift_type, sender_obj_id, winner_obj_id, "
            "   prize_gift_name, expire_time, sender_name, winner_name "
            "from raffle "
            "where expire_time >= %s "
            "order by expire_time desc, id desc "
            "limit %s;"
        ), (start_date, page_size)
    )
    user_obj_ids = set()
    room_ids = set()
    for row in records:
        (
            id, room_id, gift_name, gift_type, sender_obj_id, winner_obj_id,
            prize_gift_name, expire_time, sender_name, winner_name
        ) = row

        room_ids.add(room_id)
        user_obj_ids.add(sender_obj_id)
        user_obj_ids.add(winner_obj_id)

    users = await AsyncMySQL.execute(
        (
            "select id, uid, name, short_room_id, real_room_id "
            "from biliuser "
            "where id in %s or real_room_id in %s "
            "order by id desc ;"
        ), (user_obj_ids, room_ids)
    )
    room_id_map = {}
    user_obj_id_map = {}
    for row in users:
        id, uid, name, short_room_id, real_room_id = row
        if short_room_id in (None, "", 0, "0"):
            short_room_id = None
        room_id_map[real_room_id] = (short_room_id, name)
        user_obj_id_map[id] = (uid, name)

    raffle_data = []
    for row in records:
        (
            id, real_room_id, gift_name, gift_type, sender_obj_id, winner_obj_id,
            prize_gift_name, expire_time, sender_name, winner_name
        ) = row

        short_room_id, master_uname = room_id_map.get(real_room_id, (None, ""))
        if short_room_id is None:
            short_room_id = ""
        elif short_room_id == real_room_id:
            short_room_id = "-"

        user_id, user_name = user_obj_id_map.get(winner_obj_id, ("", winner_name))
        sender_uid, sender_name = user_obj_id_map.get(sender_obj_id, ("", sender_name))

        display_name = gift_name.replace("抽奖", "")
        if display_name in ("", "-", None):
            display_name = f"&{gift_type}"
        info = {
            "short_room_id": short_room_id,
            "real_room_id": real_room_id,
            "raffle_id": id,
            "gift_name": display_name,
            "prize_gift_name": prize_gift_name or "",
            "created_time": expire_time,
            "user_id": user_id or "",
            "user_name": user_name or "",
            "master_uname": master_uname or "",
            "sender_uid": sender_uid or "",
            "sender_name": sender_name or "",
        }
        raffle_data.append(info)

    if json_req:
        json_result = copy.deepcopy(raffle_data)
        for info in json_result:
            for k, v in info.items():
                if isinstance(v, datetime.datetime):
                    info[k] = str(v)
                elif v == "":
                    info[k] = None

        return web.Response(
            text=json.dumps(
                {"code": 0, "e_tag": e_tag, "list": json_result},
                indent=2,
                ensure_ascii=False,
            ),
            content_type="application/json"
        )

    context = {
        "e_tag": e_tag,
        "raffle_data": raffle_data,
        "raffle_count": len(raffle_data),
        "CDN_URL": CDN_URL,
    }
    return render_to_response("web/templates/raffles.html", context=context)


async def query_raffles_by_user(request, user):
    day_range = request.query.get("day_range")
    now = datetime.datetime.now()
    raffle_start_record_time = now.replace(year=2019, month=7, day=2, hour=0, minute=0, second=0, microsecond=0)

    try:
        day_range = int(day_range)
        assert day_range > 1
    except (ValueError, TypeError, AssertionError):
        return web.Response(text="day_range参数错误。", content_type="text/html")

    end_date = now - datetime.timedelta(days=day_range)
    if end_date < raffle_start_record_time:
        total_days = int((now - raffle_start_record_time).total_seconds() / 3600 / 24)
        return web.Response(
            text=f"day_range参数超出范围。最早可以查询2019年7月2日之后的记录，day_range范围 1 ~ {total_days}。",
            content_type="text/html"
        )
    user_obj = await BiliUser.get_by_uid_or_name(user)
    if not user_obj:
        return web.Response(text=f"未收录该用户: {user}", content_type="text/html")

    winner_obj_id, uid, user_name = user_obj.id, user_obj.uid, user_obj.name
    records = await AsyncMySQL.execute(
        (
            "select room_id, prize_gift_name, expire_time, sender_name, id from raffle "
            "where winner_obj_id = %s and expire_time > %s "
            "order by expire_time desc ;"
        ), (winner_obj_id, datetime.datetime.now() - datetime.timedelta(days=day_range))
    )
    if not records:
        return web.Response(text=f"用户{uid} - {user_name} 在{day_range}天内没有中奖。", content_type="text/html")

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
        info = {
            "short_room_id": short_room_id,
            "real_room_id": room_id,
            "raffle_id": raffle_id,
            "prize_gift_name": prize_gift_name,
            "sender_name": sender_name,
            "expire_time": expire_time,
            "master_name": master_name,
        }
        raffle_data.insert(0, info)

    context = {
        "uid": uid,
        "user_name": user_name,
        "day_range": day_range,
        "raffle_data": raffle_data,
    }
    return render_to_response("web/templates/raffles_by_user.html", context=context)


async def broadcast(request):
    context = {"CDN_URL": CDN_URL}
    return render_to_response("web/templates/broadcast.html", context=context)


async def user_info(request):
    user_str = request.match_info['user']
    user_obj = await BiliUser.get_by_uid_or_name(user_str)
    if not user_obj:
        return web.Response(text=f"未收录该用户: {user_str}", content_type="text/html")

    context = {
        "user_name": user_obj.name,
        "uid": user_obj.uid
    }
    return render_to_response(template="web/templates/user_info", context=context)
