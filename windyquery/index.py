import asyncpg

from .builder import Builder
from .builder import ColumnBuilder
from .builder import IndexBuilder


class Connection:
    """The base class for DB connection"""

    def __init__(self):
        self.default = ''
        self.builder = None
        self.conn_pools = {}

    async def connect(self, connection_name, config, default=False, min_size=10, max_size=10, max_queries=50000, max_inactive_connection_lifetime=300.0, setup=None, init=None, loop=None, connection_class=asyncpg.connection.Connection, **connect_kwargs):
        if connection_name in self.conn_pools:
            raise UserWarning("connection: {} already exists".format(connection_name))

        dsn= "postgresql://{}:{}@{}:{}/{}".format(
            config['username'],
            config['password'],
            config['host'],
            config['port'],
            config['database']
        )
        self.conn_pools[connection_name] = await asyncpg.create_pool(
            dsn=dsn,
            min_size=min_size,
            max_size=max_size,
            max_queries=max_queries,
            max_inactive_connection_lifetime=max_inactive_connection_lifetime,
            setup=setup,
            init=init,
            loop=loop,
            connection_class=connection_class,
            **connect_kwargs)
        if default:
            self.default = connection_name

    async def stop(self):
        for name, pool in self.conn_pools.items():
            await pool.close()
            pool.terminate()
        self.default = ''
        self.conn_pools = {}

    def get_builder(self):
        if self.builder is None:
            pool = self.conn_pools[self.default]
            builder = Builder(pool)
        else:
            builder = self.builder
            self.builder = None
        return builder

    def connection(self, connection_name):
        if connection_name not in self.conn_pools:
            raise UserWarning("connection: {} does not exists".format(connection_name))
        pool = self.conn_pools[connection_name]
        self.builder = Builder(pool)
        return self

    def raw(self, query, args=None):
        if args is None:
            args = []
        return self.get_builder().raw(query, args)


class DB(Connection):
    """DB class for CRUD"""

    def table(self, table_name):
        return self.get_builder().table(table_name)


class Schema(Connection):
    """DB class for migrations"""

    def get_column_items(self, table_name, columns):
        items = {}
        items['table'] = table_name.strip()
        items["columns"] = []
        items["primary"] = None
        items["uniques"] = []
        items["indexes"] = []
        for col in columns:
            col(items)
        return items

    async def create(self, table_name, *columns):
        items = self.get_column_items(table_name, columns)
        builder = self.get_builder()
        await builder.create(items)
        for idx_cols in items['indexes']:
            builder.reset()
            items['index_columns'] = idx_cols
            await builder.create_index(items)

    async def rename(self, table_name_old, table_name_new):
        return await self.get_builder().rename_table(table_name_old, table_name_new)

    async def drop(self, table_name):
        return await self.get_builder().drop_table(table_name)

    async def drop_if_exists(self, table_name):
        return await self.get_builder().drop_table(table_name, True)

    async def table(self, table_name, *columns):
        items = self.get_column_items(table_name, columns)
        builder = self.get_builder()
        if len(items['columns'])  > 0 or items['primary'] is not None:
            await builder.alter(items)
        if len(items['indexes']) > 0 or len(items['uniques']) > 0:
            builder.reset()
            await builder.alter_index(items)

    async def dropIndex(self, index_name):
        return await self.get_builder().drop_index(index_name)

    async def dropPrimaryKey(self, table_name, pkey=None):
        if pkey is None:
            pkey = table_name+'_pkey'
        return await self.get_builder().drop_primary_key(table_name, pkey)

    async def dropConstraint(self, table_name, constraint_name):
        return await self.get_builder().drop_constraint(table_name, constraint_name)

    def column(cls, name, **attr):
        return ColumnBuilder(name, **attr)

    def primary_key(cls, *columns):
        return IndexBuilder(columns, type='primary')

    def unique(cls, *columns):
        return IndexBuilder(columns, type='unique')

    def index(cls, *columns):
        return IndexBuilder(columns, type='index')
