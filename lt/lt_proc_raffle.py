import time
import json
import asyncio
import datetime
import traceback
from config import g
from utils.cq import async_zy
from utils.udp import mq_server
from utils.biliapi import BiliApi
from db.tables import DMKSource, RaffleBroadCast
from config.log4 import lt_server_logger as logging
from utils.dao import redis_cache, RedisGuard, RedisRaffle, RedisAnchor, InLotteryLiveRooms
from utils.model import objects, Guard, Raffle


class Executor:
    def __init__(self, msg: DMKSource):
        self._start_time = time.time()
        self.msg = msg

    async def g(self, *args):
        key_type, room_id, danmaku, *_ = args

        guards = [danmaku["data"]["lottery"]]
        await self._handle_guard(room_id, guards)

    async def r(self, *args):
        """ record_raffle """

        key_type, room_id, danmaku, *_ = args
        cmd = danmaku["cmd"]

        if cmd == "ANCHOR_LOT_AWARD":
            data = danmaku["data"]
            raffle_id = data["id"]
            data["room_id"] = room_id
            await RedisAnchor.add(raffle_id=raffle_id, value=data)

        elif cmd in ("RAFFLE_END", "TV_END"):
            data = danmaku["data"]
            winner_name = data["uname"]
            winner_uid = None
            winner_face = data["win"]["face"]
            raffle_id = int(data["raffleId"])
            gift_type = data["type"]
            sender_name = data["from"]
            sender_face = data["fromFace"]
            prize_gift_name = data["giftName"]
            prize_count = int(data["win"]["giftNum"])

            raffle = await RedisRaffle.get(raffle_id=raffle_id)
            if not raffle:
                created_time = datetime.datetime.fromtimestamp(self._start_time)
                gift_gen_time = created_time - datetime.timedelta(seconds=180)
                gift_name = await redis_cache.get(key=f"GIFT_TYPE_{gift_type}")

                raffle = {
                    "raffle_id": raffle_id,
                    "room_id": room_id,
                    "gift_name": gift_name,
                    "gift_type": gift_type,
                    "sender_uid": None,
                    "sender_name": sender_name,
                    "sender_face": sender_face,
                    "created_time": gift_gen_time,
                    "expire_time": created_time,
                }

            update_param = {
                "prize_gift_name": prize_gift_name,
                "prize_count": prize_count,
                "winner_uid": winner_uid,
                "winner_name": winner_name,
                "winner_face": winner_face,
                "danmaku_json_str": json.dumps(danmaku),
            }
            raffle.update(update_param)
            await RedisRaffle.add(raffle_id=raffle_id, value=raffle)
            await Raffle.create(**raffle)

    async def d(self, *args):
        """ danmaku to qq """
        key_type, room_id, danmaku, *_ = args
        info = danmaku.get("info", {})
        msg = str(info[1])
        if msg in g.lottery_danmaku:
            return

        uid = info[2][0]
        user_name = info[2][1]
        is_admin = info[2][2]
        ul = info[4][0]
        d = info[3]
        dl = d[0] if d else "-"
        deco = d[1] if d else "undefined"
        message = (
            f"{room_id} ({datetime.datetime.fromtimestamp(self._start_time)}) ->\n\n"
            f"{'[管] ' if is_admin else ''}[{deco} {dl}] [{uid}][{user_name}][{ul}]-> {msg}"
        )
        logging.info(message)
        await async_zy.send_private_msg(user_id=g.QQ_NUMBER_DD, message=message)

    async def p(self, *args):
        """ pk """

        key_type, room_id, danmaku, *_ = args
        raffle_id = danmaku["data"]["id"]
        key = f"P${room_id}${raffle_id}"
        if await redis_cache.set_if_not_exists(key, "de-duplication"):
            await self.broadcast(json.dumps({
                "raffle_type": "pk",
                "ts": int(self._start_time),
                "real_room_id": room_id,
                "raffle_id": raffle_id,
                "gift_name": "PK",
            }, ensure_ascii=False))

    async def s(self, *args):
        """ storm """
        key_type, room_id, danmaku, *_ = args
        raffle_id = int(danmaku["data"]["39"]["id"])
        key = F"S${room_id}${raffle_id}"
        if not await redis_cache.set_if_not_exists(key, "de-duplication"):
            return

        created_time = datetime.datetime.fromtimestamp(self._start_time)
        expire_time = created_time - datetime.timedelta(seconds=90)
        inner_raffle_id = int(raffle_id/1000000)
        create_param = {
            "gift_id": inner_raffle_id,
            "room_id": room_id,
            "gift_name": "节奏风暴",
            "sender_uid": -1,
            "sender_name": "&__STORM_SENDER__",
            "sender_face": "",
            "created_time": created_time,
            "expire_time": expire_time,
        }
        await RedisGuard.add(raffle_id=inner_raffle_id, value=create_param)
        await Guard.create(**create_param)

        await self.broadcast(json.dumps({
            "raffle_type": "storm",
            "ts": int(self._start_time),
            "real_room_id": room_id,
            "raffle_id": raffle_id,
            "gift_name": "节奏风暴",
        }, ensure_ascii=False))

    async def a(self, *args):
        """
        anchor

        require_type = data["require_type"]
        0: 无限制; 1: 关注主播; 2: 粉丝勋章; 3大航海； 4用户等级；5主站等级
        """
        key_type, room_id, danmaku, *_ = args
        data = danmaku["data"]
        raffle_id = data["id"]
        room_id = data["room_id"]
        award_name = data["award_name"]
        award_num = data["award_num"]
        cur_gift_num = data["cur_gift_num"]
        gift_name = data["gift_name"]
        gift_num = data["gift_num"]
        gift_price = data["gift_price"]
        join_type = data["join_type"]
        require_type = data["require_type"]
        require_value = data["require_value"]
        require_text = data["require_text"]
        danmu = data["danmu"]

        key = f"A${room_id}${raffle_id}"
        if await redis_cache.set_if_not_exists(key, "de-duplication"):
            await self.broadcast(json.dumps({
                "raffle_type": "anchor",
                "ts": int(self._start_time),
                "real_room_id": room_id,
                "raffle_id": raffle_id,
                "gift_name": "天选时刻",
                "join_type": join_type,
                "require": f"{require_type}-{require_value}:{require_text}",
                "gift": f"{gift_num}*{gift_name or 'null'}({gift_price})",
                "award": f"{award_num}*{award_name}",
            }, ensure_ascii=False))

    async def _handle_guard(self, room_id, guard_list):
        for info in guard_list:
            raffle_id = info['id']
            key = F"G${room_id}${raffle_id}"
            if not await redis_cache.set_if_not_exists(key, "de-duplication"):
                continue

            privilege = info["privilege_type"]
            gift_name = {1: "舰长", 2: "提督", 3: "总督"}.get(privilege, f"guard_{privilege}")
            created_time = datetime.datetime.fromtimestamp(self._start_time)
            expire_time = created_time + datetime.timedelta(seconds=info["time"])

            bc = RaffleBroadCast(
                raffle_type="guard",
                ts=int(time.time()),
                real_room_id=room_id,
                raffle_id=raffle_id,
                gift_name=gift_name,
                created_time=created_time,
                expire_time=expire_time,
            )
            await bc.save(redis_cache)

            sender = info["sender"]
            create_param = {
                "gift_id": raffle_id,
                "room_id": room_id,
                "gift_name": gift_name,
                "sender_uid": sender["uid"],
                "sender_name": sender["uname"],
                "sender_face": sender["face"],
                "created_time": created_time,
                "expire_time": expire_time,
            }
            await RedisGuard.add(raffle_id=raffle_id, value=create_param)
            await Guard.create(**create_param)
            logging.info(
                f"\tGuard found: room_id: {room_id} $ {raffle_id} "
                f"({gift_name}) <- {sender['uname']}"
            )

    async def _handle_tv(self, room_id, gift_list):
        await InLotteryLiveRooms.add(room_id=room_id)
        gift_type_to_name_map = {}

        for info in gift_list:
            raffle_id = info["raffleId"]
            key = f"T${room_id}${raffle_id}"
            if not await redis_cache.set_if_not_exists(key, "de-duplication"):
                continue

            gift_type = info["type"]
            gift_name = info.get("thank_text", "").split("赠送的", 1)[-1]
            created_time = datetime.datetime.fromtimestamp(self._start_time)
            expire_time = created_time + datetime.timedelta(seconds=info["time"])

            bc = RaffleBroadCast(
                raffle_type="tv",
                ts=int(time.time()),
                real_room_id=room_id,
                raffle_id=raffle_id,
                gift_name=gift_name,
                created_time=created_time,
                expire_time=expire_time,
                gift_type=gift_type,
                time_wait=info["time_wait"],
                max_time=info["max_time"],
            )
            await bc.save(redis_cache)

            sender_name = info["from_user"]["uname"]
            sender_face = info["from_user"]["face"]
            logging.info(
                f"\tLottery found: room_id: {room_id} $ {raffle_id} "
                f"({gift_name}) <- {sender_name}"
            )

            create_param = {
                "raffle_id": raffle_id,
                "room_id": room_id,
                "gift_name": gift_name,
                "gift_type": gift_type,
                "sender_uid": None,
                "sender_name": sender_name,
                "sender_face": sender_face,
                "created_time": created_time,
                "expire_time": expire_time
            }
            await RedisRaffle.add(raffle_id=raffle_id, value=create_param, _pre=True)
            await Raffle.record_raffle_before_result(**create_param)
            gift_type_to_name_map[gift_type] = gift_name

        for gift_type, gift_name in gift_type_to_name_map.items():
            await redis_cache.set(key=f"GIFT_TYPE_{gift_type}", value=gift_name)

    async def hdl_lottery_or_guard(self):
        room_id = self.msg.room_id
        prize_type = self.msg.prize_type

        flag, result = await BiliApi.lottery_check(room_id=room_id)
        if not flag and "Empty raffle_id_list" in result:
            await asyncio.sleep(1)
            flag, result = await BiliApi.lottery_check(room_id=room_id)

        if not flag:
            logging.error(f"Cannot get lottery({prize_type}) from room: {room_id}. reason: {result}")
            return

        guards, gifts = result
        await self._handle_guard(room_id, guards)
        await self._handle_tv(room_id, gifts)

    async def raffle_start(self, *args):
        key_type, room_id, danmaku, *_ = args
        data = danmaku["data"]
        await self._handle_tv(room_id=room_id, gift_list=[data])

    async def run(self):
        if self.msg.prize_type in ("T", "Z"):
            await self.hdl_lottery_or_guard()


class RaffleProcessor:

    def __init__(self):
        self._dmk_source_q = asyncio.queues.Queue()
        self._workers = []

    async def receive(self):
        while True:
            de_dup = set()
            for index in range(mq_server.qzise()):
                msg = mq_server.get_nowait()
                if msg.prize_type in ("T", "Z"):
                    if msg.room_id in de_dup:
                        continue
                    de_dup.add(msg.room_id)
                    self._dmk_source_q.put_nowait(msg)
                    logging.info(f"Assign task: {msg.prize_type} -> {msg.room_id}")
                elif msg.prize_type in ("G", "S", "R", "D", "P", "A", "RAFFLE_START"):
                    self._dmk_source_q.put_nowait(msg)

            await asyncio.sleep(3)

    async def process_one(self, index: int):
        while True:
            msg = await self._dmk_source_q.get()
            start_time = time.time()

            try:
                executor = Executor(msg)
                await executor.run()
            except Exception as e:
                logging.error(f"RAFFLE worker[{index}] error: {e}\n{traceback.format_exc()}")

            cost_time = time.time() - start_time
            if cost_time > 5:
                logging.warning(f"RAFFLE worker[{index}] exec long time: {cost_time:.3f}")

    async def work(self):
        self._workers = [
            asyncio.create_task(self.process_one(index))
            for index in range(8)
        ]
        for t in self._workers:
            await t


async def main():
    logging.info(f"\n{'-' * 80}\nLT PROC_RAFFLE started!\n{'-' * 80}")
    await objects.connect()
    await mq_server.start_listen()

    processor = RaffleProcessor()
    await asyncio.gather(
        processor.receive(),
        processor.work()
    )


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
