import asyncpg

from .builder import Builder


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
        for _, pool in self.conn_pools.items():
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
