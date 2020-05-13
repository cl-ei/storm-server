import time
import logging
import asyncio
from utils.biliapi import BiliApi
from utils.dao import redis_cache
from utils.udp import mq_source_to_raffle
from config.log4 import crontab_task_logger as logging


async def get_new_room(td, jz):

    def get_sep(new, old):
        new_str = "-".join([str(_) for _ in new])
        while old:
            old_str = "-".join(map(str, old))
            if new_str.startswith(old_str):
                result = [int(_) for _ in new_str[len(old_str):].split("-") if _]
                break
            old.pop(0)
        else:
            result = new
        return result

    cache_key = "LT_INTERVAL_GUARD_LIST"
    old_record = await redis_cache.get(cache_key)
    logging.info(f"old_record: {len(old_record)}")
    await redis_cache.set(key=cache_key, value=[td, jz])

    if not old_record:
        return set(td + jz)

    old_td, old_jz = old_record
    new_td, new_jz = get_sep(td, old_td), get_sep(jz, old_jz)
    logging.info(f"new_td, new_jz: {len(new_td)}, {len(new_jz)}")
    return set(new_td + new_jz)


async def proc_one_room(room_id):
    flag, result = await BiliApi.lottery_check(room_id=room_id, force_cloud=True)
    if not flag:
        logging.error(f"Cannot get lottery from room: {room_id}. reason: {result}")
        return
    logging.info(f"result: {result}")
    guards, gifts = result

    ts = int(time.time())
    for guard in guards:
        msg = {"data": {"lottery": guard}}
        mq_source_to_raffle.put_nowait(("G", room_id, msg, ts))


async def main():
    start_time = time.time()
    logging.info("Now fetch guard list.")

    flag, data = await BiliApi.get_gurads_list()
    if not flag:
        logging.error(F"Cannot get guard list: {data}")
        return

    td, jz = data
    new_rooms = await get_new_room(*data)
    new_rooms = list(new_rooms)
    if len(new_rooms) < 15:
        display_rooms = ", ".join(map(str, new_rooms))
    else:
        display_rooms = ", ".join(map(str, new_rooms[:15])) + "..."
    logging.info(f"Get td: {len(td)}, jz: {len(jz)}, New rooms({len(new_rooms)}): {display_rooms}")

    for i, room_id in enumerate(new_rooms):
        await proc_one_room(room_id)
        await asyncio.sleep(1.1)

    cost = time.time() - start_time
    logging.info(f"Update live room info execute finished, cost: {cost/60:.3f} min.\n\n")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
