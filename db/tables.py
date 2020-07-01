import time
from db import RWSchema
from pydantic import Field
from typing import Optional
from config.log4 import lt_server_logger as logging


class DMKSource(RWSchema):

    # T: tv
    # Z: 总督
    prize_type: str = Field(..., regex="^(T|Z)$")
    room_id: int


class RaffleBroadCast(RWSchema):
    __key__ = "LTS:RF_BR"

    raffle_type: str       # "guard"
    ts: int                # int(time.time())
    real_room_id: int      # room_id
    raffle_id: int         # raffle_id
    gift_name: str         # gift_name
    gift_type: Optional[str]  # gift_type
    time_wait: Optional[int]  # info["time_wait"]
    max_time:  Optional[int]  # info["max_time"]

    def __str__(self):
        return f"<RfBrCst {self.raffle_type}-{self.real_room_id}.{self.raffle_id}>"

    def __repr__(self):
        return self.__str__()

    async def save(self, redis):
        result = await redis.zset_zadd(
            key=self.__key__,
            member_pairs=(self, time.time())
        )
        logging.info(f"RaffleBroadCast saved! {self} -> {result}")
