import peewee
import asyncio
import datetime
import aiomysql
from random import randint

from peewee_async import Manager, PooledMySQLDatabase
from config import MYSQL_CONFIG

mysql_db = PooledMySQLDatabase(**MYSQL_CONFIG)

loop = asyncio.get_event_loop()
objects = Manager(mysql_db, loop=loop)


class MonitorWsClient(peewee.Model):
    update_time = peewee.DateTimeField(index=True)
    name = peewee.CharField()
    value = peewee.FloatField()

    class Meta:
        database = mysql_db

    @classmethod
    async def record(cls, params):
        valid_names = (
            "valuable room",
            "api room cnt",
            "active clients",
            "broken clients",
            "total clients",
            "target clients",
            "valuable hit rate",
            "msg speed",
            "msg peak speed",
            "TCP ESTABLISHED",
            "TCP TIME_WAIT",
        )
        update_time = params.get("update_time") or datetime.datetime.now()
        insert_params = []
        for key in params:
            if key in valid_names:
                insert_params.append({"update_time": update_time, "name": key, "value": params[key]})

        if insert_params:
            await objects.execute(MonitorWsClient.insert_many(insert_params))
            return True
        else:
            return False


class AsyncMySQL:

    @classmethod
    async def execute(cls, *args, _commit=False, **kwargs):
        conn = await aiomysql.connect(
            host=MYSQL_CONFIG["host"],
            port=MYSQL_CONFIG["port"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"],
            db=MYSQL_CONFIG["database"],
            loop=asyncio.get_event_loop()
        )

        async with conn.cursor() as cursor:
            await cursor.execute(*args, **kwargs)
            if _commit:
                await conn.commit()
            r = await cursor.fetchall()
        conn.close()
        return r


def random_datetime():
    return datetime.datetime.now() - datetime.timedelta(days=randint(3600, 4000))


class BiliUser(peewee.Model):
    uid = peewee.IntegerField(unique=True, null=True, index=True)
    name = peewee.CharField(index=True)
    face = peewee.CharField()
    user_info_update_time = peewee.DateTimeField(default=random_datetime)

    short_room_id = peewee.IntegerField(null=True, unique=True)
    real_room_id = peewee.IntegerField(null=True, unique=True, index=True)
    title = peewee.CharField(default="")
    create_at = peewee.CharField(default="2010-12-00 00:00:00")
    attention = peewee.IntegerField(default=0)
    guard_count = peewee.IntegerField(default=0)
    room_info_update_time = peewee.DateTimeField(index=True, default=random_datetime)

    class Meta:
        database = mysql_db

    @classmethod
    async def get_uid_by_name(cls, name):
        try:
            user = await objects.get(cls, name=name)
            return user.uid
        except peewee.DoesNotExist:
            return None

    @classmethod
    async def get_or_update(cls, uid, name, face=""):
        if uid is None:
            try:
                return await objects.get(BiliUser, name=name)

            except peewee.DoesNotExist:
                user_obj = await objects.create(
                    BiliUser,
                    name=name,
                    face=face,
                    user_info_update_time=datetime.datetime.now()
                )
                return user_obj

        try:
            user_obj = await objects.get(BiliUser, uid=uid)

            if user_obj.name != name:
                user_obj.name = name
                user_obj.face = face
                user_obj.user_info_update_time = datetime.datetime.now()
                await objects.update(user_obj, only=("name", "face", "user_info_update_time"))

            return user_obj

        except peewee.DoesNotExist:

            # 不能通过uid来获取user obj， 但name不为空， 库中可能存在user name跟此条相同的记录
            # 直接创建的话，可能会造成很多重复记录  因此找出已经存在的记录 更新之
            try:
                existed_user_obj = await objects.get(BiliUser, name=name)

                existed_user_obj.uid = uid
                existed_user_obj.face = face
                existed_user_obj.user_info_update_time = datetime.datetime.now()
                await objects.update(existed_user_obj, only=("uid", "face", "user_info_update_time"))

                return existed_user_obj

            except peewee.DoesNotExist:

                # 既不能通过uid来获取该记录，也没有存在的name，则完整创建
                user_obj = await objects.create(
                    BiliUser,
                    name=name,
                    uid=uid,
                    face=face,
                    user_info_update_time=datetime.datetime.now()
                )
                return user_obj

    @classmethod
    async def get_by_uid(cls, uid):
        objs = await objects.execute(BiliUser.select().where(BiliUser.uid == uid))
        return objs[0] if objs else None

    @classmethod
    async def full_create_or_update(
            cls, uid, name, face, user_info_update_time, short_room_id, real_room_id, title,
            create_at, attention, guard_count, room_info_update_time
    ):
        obj = await cls.get_by_uid(uid)
        if not obj:
            objs = await objects.execute(cls.select().where(cls.real_room_id == real_room_id))
            if objs:
                obj = objs[0]
        if obj:
            obj.name = name
            obj.face = face
            obj.user_info_update_time = user_info_update_time
            obj.short_room_id = short_room_id
            obj.real_room_id = real_room_id
            obj.title = title
            obj.create_at = create_at
            obj.attention = attention
            obj.guard_count = guard_count
            obj.room_info_update_time = room_info_update_time

            await objects.update(obj)
        else:
            obj = await objects.create(
                cls,
                uid=uid,
                name=name,
                face=face,
                user_info_update_time=user_info_update_time,
                short_room_id=short_room_id,
                real_room_id=real_room_id,
                title=title,
                create_at=create_at,
                attention=attention,
                guard_count=guard_count,
                room_info_update_time=room_info_update_time
            )
        return obj


class Guard(peewee.Model):
    id = peewee.IntegerField(primary_key=True)
    room_id = peewee.IntegerField(index=True)
    gift_name = peewee.CharField()

    sender_obj_id = peewee.IntegerField(index=True)
    # 仅为送礼时的用户名，方便查询历史用户名
    sender_name = peewee.CharField()

    created_time = peewee.DateTimeField(default=datetime.datetime.now)
    expire_time = peewee.DateTimeField(default=datetime.datetime.now, index=True)

    class Meta:
        database = mysql_db

    @classmethod
    async def create(cls, gift_id, room_id, gift_name, sender_uid, sender_name, sender_face, created_time, expire_time):

        sender = await BiliUser.get_or_update(uid=sender_uid, name=sender_name, face=sender_face)
        try:
            return await objects.create(
                Guard,
                id=gift_id,
                room_id=room_id,
                gift_name=gift_name,
                sender_obj_id=sender.id,
                sender_name=sender_name,
                created_time=created_time,
                expire_time=expire_time,
            )
        except peewee.IntegrityError as e:
            error_msg = f"{e}"
            if "Duplicate entry" in error_msg:
                old_rec = await objects.get(Guard, id=gift_id)
                old_rec.room_id = room_id
                old_rec.gift_name = gift_name
                old_rec.sender_obj_id = sender.id
                old_rec.sender_name = sender_name
                old_rec.created_time = created_time
                old_rec.expire_time = expire_time

                await objects.update(old_rec)
                return old_rec
            return None


class Raffle(peewee.Model):
    id = peewee.IntegerField(primary_key=True)
    room_id = peewee.IntegerField(index=True)
    gift_name = peewee.CharField()
    gift_type = peewee.CharField()

    sender_obj_id = peewee.IntegerField(index=True)
    sender_name = peewee.CharField(null=True)
    winner_obj_id = peewee.IntegerField(null=True, index=True)
    winner_name = peewee.CharField(null=True)

    prize_gift_name = peewee.CharField(null=True)
    prize_count = peewee.IntegerField(null=True)

    created_time = peewee.DateTimeField(default=random_datetime, index=True)
    expire_time = peewee.DateTimeField(default=random_datetime, index=True)

    raffle_result_danmaku = peewee.CharField(null=True, max_length=20480)

    class Meta:
        database = mysql_db

    @classmethod
    async def get_by_id(cls, raffle_id):
        try:
            return await objects.get(Raffle, id=raffle_id)
        except peewee.DoesNotExist:
            return None

    @classmethod
    async def record_raffle_before_result(
        cls, raffle_id, room_id, gift_name, gift_type, sender_uid, sender_name, sender_face, created_time, expire_time,
    ):
        sender = await BiliUser.get_or_update(uid=sender_uid, name=sender_name, face=sender_face)
        try:
            return await objects.create(
                cls,
                id=raffle_id,
                room_id=room_id,
                gift_name=gift_name,
                gift_type=gift_type,
                sender_obj_id=sender.id,
                sender_name=sender_name,
                created_time=created_time,
                expire_time=expire_time,
            )
        except peewee.IntegrityError as e:
            error_msg = f"{e}"
            if "Duplicate entry" in error_msg:
                old_rec = await objects.get(Raffle, id=raffle_id)
                old_rec.room_id = room_id
                old_rec.gift_name = gift_name
                old_rec.gift_type = gift_type
                old_rec.sender_obj_id = sender.id
                old_rec.sender_name = sender_name
                old_rec.created_time = created_time
                old_rec.expire_time = expire_time

                await objects.update(old_rec)
                return old_rec
            return None

    @classmethod
    async def create(
        cls,
        raffle_id,
        room_id,
        gift_name,
        gift_type,
        sender_uid,
        sender_name,
        sender_face,
        created_time,
        expire_time,
        prize_gift_name,
        prize_count,
        winner_uid,
        winner_name,
        winner_face,
        **kw
    ):
        sender = await BiliUser.get_or_update(uid=sender_uid, name=sender_name, face=sender_face)
        winner = await BiliUser.get_or_update(uid=winner_uid, name=winner_name, face=winner_face)
        try:
            return await objects.create(
                cls,
                id=raffle_id,
                room_id=room_id,
                gift_name=gift_name,
                gift_type=gift_type,
                sender_obj_id=sender.id,
                sender_name=sender_name,
                winner_obj_id=winner.id,
                winner_name=winner_name,
                prize_gift_name=prize_gift_name,
                prize_count=prize_count,
                created_time=created_time,
                expire_time=expire_time
            )
        except peewee.IntegrityError as e:
            if "Duplicate entry" in f"{e}":
                old_rec = await objects.get(Raffle, id=raffle_id)
                old_rec.room_id = room_id
                old_rec.prize_gift_name = prize_gift_name
                old_rec.prize_count = prize_count
                old_rec.sender_obj_id = sender.id
                old_rec.winner_obj_id = winner.id

                await objects.update(
                    obj=old_rec,
                    only=("room_id", "prize_gift_name", "prize_count", "sender_obj_id", "winner_obj_id")
                )

                return old_rec
            return None

    @classmethod
    async def update_raffle_result(
            cls, raffle_obj, prize_gift_name, prize_count, winner_uid, winner_name, winner_face, danmaku_json_str=""
    ):
        sender = await BiliUser.get_or_update(uid=winner_uid, name=winner_name, face=winner_face)
        raffle_obj.prize_gift_name = prize_gift_name
        raffle_obj.prize_count = prize_count
        raffle_obj.winner_obj_id = sender.id
        raffle_obj.winner_name = winner_name
        raffle_obj.raffle_result_danmaku = danmaku_json_str

        await objects.update(
            obj=raffle_obj,
            only=("prize_gift_name", "prize_count", "winner_obj_id", "winner_name", "raffle_result_danmaku")
        )

        return raffle_obj
