import asyncio
import aiohttp
from utils.udp import mq_client
from utils.biliapi import WsApi, BiliApi
from db.tables import DMKSource
from config.log4 import lt_server_logger as logging

DANMAKU_WS_URL = "ws://broadcastlv.chat.bilibili.com:2244/sub"
ALL_WS_CLIENTS = set()
MONITOR_LIVE_ROOM_IDS = set()


async def heart_beat():
    heart_beat_package = WsApi.gen_heart_beat_pkg()
    while True:
        await asyncio.sleep(30)
        for ws in ALL_WS_CLIENTS:
            if not ws.closed:
                await ws.send_bytes(heart_beat_package)


async def proc_danmaku(area_id, room_id, raw_msg) -> bool:
    for danmaku in WsApi.parse_msg(raw_msg):
        cmd = danmaku.get("cmd")
        if not cmd:
            continue

        if cmd in ("PREPARING", "ROOM_CHANGE"):
            return True

        elif cmd == "NOTICE_MSG":
            msg_type = danmaku.get("msg_type")
            if msg_type in (2, 8):
                real_room_id = danmaku['real_roomid']
                await mq_client.put(DMKSource(
                    prize_type="T",
                    room_id=real_room_id
                ))
                logging.info(f"NOTICE_MSG received. room_id: {real_room_id}")

        elif cmd == "GUARD_MSG" and danmaku["buy_type"] == 1:
            prize_room_id = danmaku['roomid']
            await mq_client.put(DMKSource(
                prize_type="Z",
                room_id=prize_room_id
            ))
            logging.info(f"GUARD_MSG received. room_id: {prize_room_id}")
    return False


async def get_living_room_id(
        index: int, area_id: int, old_room_id: int
) -> int:

    while True:
        flag, result = await BiliApi.get_living_rooms_by_area(area_id=area_id)
        if not flag:
            logging.error(
                f"Cannot get live rooms from Biliapi. "
                f"{index}-{area_id} -> {result}"
            )
            await asyncio.sleep(10)
            continue

        for room_id in result:
            if room_id not in MONITOR_LIVE_ROOM_IDS:
                MONITOR_LIVE_ROOM_IDS.add(room_id)
                MONITOR_LIVE_ROOM_IDS.discard(old_room_id)

                logging.info(
                    f"Get live rooms from Biliapi, "
                    f"{index}-{area_id}, old {old_room_id} -> {room_id}"
                )
                return room_id

        await asyncio.sleep(30)


async def listen_ws(index: int, area_id: int, room_id: int):
    is_preparing = False
    session = aiohttp.ClientSession()
    async with session.ws_connect(url=DANMAKU_WS_URL) as ws:
        await ws.send_bytes(WsApi.gen_join_room_pkg(room_id=room_id))

        ws.monitor_room_id = room_id
        ws.area_id = area_id
        ALL_WS_CLIENTS.add(ws)

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.ERROR:
                break

            is_preparing = await proc_danmaku(area_id, room_id, msg.data)
            if is_preparing is True:
                break

    ALL_WS_CLIENTS.remove(ws)
    logging.info(
        f"Client closed. {index}-{area_id} -> {room_id}, "
        f"By danmaku preparing: {is_preparing}"
    )


async def monitor(index):
    my_area_id = index % 6 + 1
    my_room_id = None
    while True:
        my_room_id = await get_living_room_id(
            index=index,
            area_id=my_area_id,
            old_room_id=my_room_id
        )

        await listen_ws(
            index=index,
            area_id=my_area_id,
            room_id=my_room_id
        )

        # print status
        message = "\n".join(sorted([
            f"\t({index})area: {ws.area_id}, "
            f"room_id: {ws.monitor_room_id}, "
            f"closed ? {ws.closed}"

            for ws in ALL_WS_CLIENTS
        ]))
        logging.info(f"TV SOURCE STATUS (before updated.): \n{message}\n")


loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(
    heart_beat(),
    *[monitor(i) for i in range(0, 18)]
))
