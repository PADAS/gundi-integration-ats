import json
import stamina
import httpx
import redis.asyncio as redis
from app import settings


class IntegrationStateManager:

    def __init__(self, **kwargs):
        host = kwargs.get("host", settings.REDIS_HOST)
        port = kwargs.get("port", settings.REDIS_PORT)
        db = kwargs.get("db", settings.REDIS_STATE_DB)
        self.db_client = redis.StrictRedis(host=host, port=port, db=db, encoding="utf-8", decode_responses=True)

    async def get_state(self, integration_id: str, action_id: str, source_id: str = "no-source") -> dict:
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                json_value = await self.db_client.get(f"integration_state.{integration_id}.{action_id}.{source_id}")
        value = json.loads(json_value) if json_value else {}
        return value

    async def set_state(self, integration_id: str, action_id: str, state: dict, source_id: str = "no-source"):
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.set(
                    f"integration_state.{integration_id}.{action_id}.{source_id}",
                    json.dumps(state, default=str)
                )

    async def delete_state(self, integration_id: str, action_id: str, source_id: str = "no-source"):
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.db_client.delete(
                    f"integration_state.{integration_id}.{action_id}.{source_id}"
                )

    async def group_add(self, group_name: str, values: list):
        # Adds values to a group. The group is created if it does not exist.
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30,
                                             wait_jitter=3.0):
            with attempt:
                return await self.db_client.sadd(group_name, *values)

    async def group_ismember(self, group_name: str, value:str):
        # Return true if the value is in the group, false otherwise.
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30,
                                             wait_jitter=3.0):
            with attempt:
                return bool(await self.db_client.sismember(group_name, value))

    async def group_get(self, group_name: str):
        # Gets all values in a group.
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30,
                                             wait_jitter=3.0):
            with attempt:
                return await self.db_client.smembers(group_name)

    async def group_move(self, from_group: str, to_group: str, values: list):
        # Moves values from one group to another.
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30,
                                             wait_jitter=3.0):
            with attempt:
                return await self.db_client.smove(from_group, to_group, *values)

    async def group_remove(self, group_name: str, values: list):
        # Removes values from a group.
        for attempt in stamina.retry_context(on=redis.RedisError, attempts=5, wait_initial=1.0, wait_max=30,
                                             wait_jitter=3.0):
            with attempt:
                return await self.db_client.srem(group_name, *values)


    def __str__(self):
        return f"IntegrationStateManager(host={self.db_client.host}, port={self.db_client.port}, db={self.db_client.db})"

    def __repr__(self):
        return self.__str__()
