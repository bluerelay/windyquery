import asyncpg

from .builder import ColumnBuilder
from .builder import IndexBuilder
from .connection import Connection


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

    def column(self, name, **attr):
        return ColumnBuilder(name, **attr)

    def primary_key(self, *columns):
        return IndexBuilder(columns, type='primary')

    def unique(self, *columns):
        return IndexBuilder(columns, type='unique')

    def index(self, *columns):
        return IndexBuilder(columns, type='index')
