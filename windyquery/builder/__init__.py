from typing import Any, Tuple
import asyncpg

from windyquery.collector import Collector
from windyquery.combiner import Combiner
from windyquery.connection import Connection
from windyquery.listener import Listener
from .select import Select
from .update import Update
from .insert import Insert
from .delete import Delete
from .create import Create
from .drop import Drop
from .alter import Alter
from .raw import Raw
from .rrule import Rrule


class DB(Select, Update, Insert, Delete, Create, Drop, Alter, Rrule, Raw):
    """DB class"""

    def __init__(self):
        super().__init__()
        self.pool_connection = Connection()
        self.default_connection = None
        self.mode = None
        self._reset()

    def _reset(self):
        self.collector = Collector()
        self.combiner = Combiner(self.collector)
        self.pool = None

    def _get_pool(self):
        if self.pool:
            return self.pool
        elif self.default_connection:
            self.connection(self.default_connection)
            return self.pool
        raise UserWarning('no connection set up for the DB instance')

    def toSql(self) -> Tuple[str, Any]:
        try:
            args = []
            if self.mode == 'crud':
                sql, args = self.build_crud()
            elif self.mode == 'schema':
                sql = self.build_schema()
            elif self.mode == 'raw':
                sql, args = self.build_raw()
            else:
                raise UserWarning('the sql build is incomplete')
            return str(sql), args
        finally:
            self._reset()

    async def exec(self):
        pool = self._get_pool()
        sql, args = self.toSql()
        async with pool.acquire() as conn:
            return await conn.fetch(sql, *args)

    def __await__(self):
        return self.exec().__await__()

    # connection interface
    async def connect(self, connection_name: str, config, default=False, min_size=10, max_size=10, max_queries=50000, max_inactive_connection_lifetime=300.0, setup=None, init=None, loop=None, connection_class=asyncpg.connection.Connection, **connect_kwargs):
        self.pool = await self.pool_connection.connect(connection_name, config, min_size, max_size, max_queries,
                                                       max_inactive_connection_lifetime, setup, init, loop, connection_class, **connect_kwargs)
        if default or self.default_connection is None:
            self.default_connection = connection_name
        return self

    async def disconnect(self, connection_name):
        await self.pool_connection.disconnect(connection_name)
        if connection_name == self.default_connection:
            self.default_connection = None

    async def stop(self):
        await self.pool_connection.stop()

    def connection(self, connection_name):
        self.pool = self.pool_connection.connection(connection_name)
        return self

    def listen(self, channel: str):
        pool = self._get_pool()
        return Listener(pool, channel)
