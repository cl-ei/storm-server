import time
import logging
import asyncio
from utils.biliapi import BiliApi
from utils.dao import redis_cache
from utils.udp import mq_source_to_raffle
from config.log4 import crontab_task_logger as logging


async def proc_one_room(room_id):
    flag, result = await BiliApi.lottery_check(room_id=room_id, force_cloud=True)
    if not flag:
        logging.error(f"Cannot get lottery from room: {room_id}. reason: {result}")
        return
    guards, gifts = result

    ts = int(time.time())
    for guard in guards:
        msg = {"data": {"lottery": guard}}
        mq_source_to_raffle.put_nowait(("G", room_id, msg, ts))
    logging.info(f"{room_id} -> guards: {len(guards)}")


async def main():
    start_time = time.time()
    logging.info("Now fetch guard list.")

    flag, data = await BiliApi.get_gurads_list()
    if not flag:
        logging.error(F"Cannot get guard list: {data}")
        return

    flag, data = await BiliApi.get_gurads_list()
    if not flag:
        logging.error(F"Cannot get guard list: {data}")
        return

    new_rooms = []
    for room_id, characteristic in data.items():
        key = f"LT_GUARD_INTERVAL_{room_id}"
        cached_characteristic = await redis_cache.get(key=key)
        if cached_characteristic == characteristic:
            continue

        new_rooms.append(room_id)
        await redis_cache.set(key=key, value=characteristic, timeout=3600*24)

    if len(new_rooms) < 15:
        display_rooms = ", ".join(map(str, new_rooms))
    else:
        display_rooms = ", ".join(map(str, new_rooms[:15])) + "..."
    logging.info(f"Get New rooms({len(new_rooms)}): {display_rooms}")

    for i, room_id in enumerate(new_rooms):
        await proc_one_room(room_id)
        await asyncio.sleep(1.1)

    cost = time.time() - start_time
    logging.info(f"Update live room info execute finished, cost: {cost/60:.3f} min.\n\n")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
