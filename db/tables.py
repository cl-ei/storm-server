import time
from db import RWSchema
from pydantic import Field
from typing import Optional


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

    async def save(self, redis):
        await redis.zset_zadd(
            key=self.__key__,
            member_to_score={self: time.time()}
        )
