import time
import json
import copy
import aiohttp
import datetime
from aiohttp import web
from web.op import bili
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
    result = await bili.query_raffles_by_user(user, day_range=day_range)
    json_req = request.query.get("json")
    if json_req:
        if not isinstance(result, dict):
            result = {"code": 5000, "msg": result}
        if "code" not in result:
            result["code"] = 0
        return web.Response(text=json.dumps(result, ensure_ascii=False), content_type="application/json")

    if isinstance(result, dict):
        return render_to_response("web/templates/raffles_by_user.html", context=result)
    return web.Response(text=result)


async def broadcast(request):
    context = {"CDN_URL": CDN_URL}
    return render_to_response("web/templates/broadcast.html", context=context)


async def user_info(request):
    user_str = request.match_info['user']
    user_obj = await BiliUser.get_by_uid_or_name(user_str)
    if not user_obj:
        return web.Response(text=f"未收录该用户: {user_str}", content_type="text/html")

    guards_info = await bili.get_send_gifts(user_obj)
    used_names = await bili.get_used_names(user_obj)
    used_names = "、".join([n for n in used_names if n != user_obj.name])
    medal_flag, medal_info = await bili.get_medal_info(user_obj)
    context = {
        "last_update": user_obj.user_info_update_time,
        "user_name": user_obj.name,
        "uid": user_obj.uid,
        "attention": user_obj.attention,
        "title": user_obj.title,
        "short_room_id": user_obj.short_room_id,
        "real_room_id": user_obj.real_room_id,
        "create_at": user_obj.create_at,
        "used_names": used_names,
        "guards_info": guards_info,
        "medal_flag": medal_flag,
        "medal_info": medal_info,
    }
    return render_to_response(template="web/templates/user_info.html", context=context)


async def realtime_guards(request):
    now = datetime.datetime.now()
    guard_query = await AsyncMySQL.execute(
        "select room_id, gift_name from guard where expire_time > %s and gift_name in %s;",
        (now, ("舰长", "提督", "总督"))
    )

    room_id_list = {row[0] for row in guard_query}
    live_room_info = await AsyncMySQL.execute(
        "select short_room_id, real_room_id from biliuser where real_room_id in %s;",
        (room_id_list, )
    )
    real_to_short_dict = {row[1]: row[0] for row in live_room_info if row[0]}

    gifts = {}
    for row in guard_query:
        room_id, gift_name = row
        gifts.setdefault(room_id, []).append(gift_name)

    guard_list = []
    for room_id, gifts_list in gifts.items():
        display = []
        intimacy = 0

        z = [n for n in gifts_list if n == "总督"]
        if z:
            display.append(f"{len(z)}个总督")
            intimacy += 20*len(z)
        t = [n for n in gifts_list if n == "提督"]
        if t:
            display.append(f"{len(t)}个提督")
            intimacy += 5 * len(t)
        j = [n for n in gifts_list if n == "舰长"]
        if j:
            display.append(f"{len(j)}个舰长")
            intimacy += len(j)

        guard_list.append({
            "room_id": real_to_short_dict.get(room_id, room_id),
            "prompt": "、".join(display),
            "intimacy": intimacy
        })
    guard_list.sort(key=lambda x: (x["intimacy"], -x["room_id"]), reverse=True)

    context = {"guard_list": guard_list, "update_time": str(datetime.datetime.now())[:19]}
    return web.Response(text=json.dumps(context, ensure_ascii=False), content_type="application/json")


async def q(request):
    user_id = request.match_info['user_id']
    web_token = request.match_info['web_token']
    msg = request.query.get("msg")
    if msg:
        url = f"http://lt.madliar.com:2020/bili/q/{user_id}/{web_token}"
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.request("get", url=url, params={"msg": msg}, timeout=timeout) as req:
            response = await req.text(encoding="utf-8", errors="ignore")
        return web.Response(text=response)

    context = {
        "user_id": user_id,
        "web_token": web_token,
    }
    return render_to_response(template="web/templates/q.html", context=context)
