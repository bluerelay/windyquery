import re
import json

from asyncpg import utils

from .parser import CRUD

class Builder:
    """collect building blocks - items; choose cmposer; exec final sql from composer"""

    def __init__(self, pool):
        self.composer = None
        self.items = {}
        self.pool = pool
        self._first = False

    def reset(self):
        self.composer = None
        self.items = {}
        self._first = False

    def _stripItems(self, items):
        return [val.strip() if isinstance(val, str) else val for val in items]

    async def toSql(self):
        sql, args = self.composer.compose(self.items)
        async with self.pool.acquire() as connection:
            sqlStr = await utils._mogrify(connection, sql, args)
            return sqlStr

    async def exec(self):
        sql, args = self.composer.compose(self.items)
        async with self.pool.acquire() as connection:
            if self._first:
                return await connection.fetchrow(sql, *args)
            else:
                return await connection.fetch(sql, *args)

    def __await__(self):
        return self.exec().__await__()

    def raw(self, query, args):
        if not isinstance(self.composer, Raw):
            self.composer = Raw()
        self.items['sql'] = query
        self.items['args'] = args
        return self

    def table(self, table):
        self.items['table'] = table.strip()
        return self

    def first(self):
        self._first = True
        return self

    def select(self, *items):
        if not isinstance(self.composer, Select):
            self.composer = Select()
        if 'select' not in self.items:
            self.items['select'] = []
        self.items['select'] += self._stripItems(items)
        return self

    def select_raw(self, *items):
        if not isinstance(self.composer, SelectRaw):
            self.composer = SelectRaw()
        if 'select' not in self.items:
            self.items['select'] = []
        self.items['select'] += self._stripItems(items)
        return self

    def join(self, *clause):
        if 'join' not in self.items:
            self.items['join'] = []
        self.items['join'].append(self._stripItems(clause))
        return self

    def where(self, *clause):
        if 'where' not in self.items:
            self.items['where'] = []
        self.items['where'].append(self._stripItems(clause))
        return self

    def where_raw(self, query):
        if 'where_raw' not in self.items:
            self.items['where_raw'] = []
        self.items['where_raw'].append(query)
        return self

    def order_by(self, *items):
        if 'order_by' not in self.items:
            self.items['order_by'] = []
        self.items['order_by'] += self._stripItems(items)
        return self

    def order_by_raw(self, query):
        if 'order_by_raw' not in self.items:
            self.items['order_by_raw'] = []
        self.items['order_by_raw'].append(query)
        return self

    def group_by(self, *items):
        if 'group_by' not in self.items:
            self.items['group_by'] = []
        self.items['group_by'] += self._stripItems(items)
        return self

    def limit(self, val):
        self.items['limit'] = int(val)
        return self

    def update(self, fields):
        if not isinstance(self.composer, Update):
            self.composer = Update()
        if 'update' not in self.items:
            self.items['update'] = {}
        self.items['update'].update(fields)
        return self
    
    def update_from(self, tableName):
        self.items['update_from'] = tableName.strip()
        return self

    def insert(self, *rows):
        if not isinstance(self.composer, Insert):
            self.composer = Insert()
        if 'insert_keys' not in self.items:
            self.items['insert_keys'] = self._stripItems(rows[0].keys())
        if 'insert_values' not in self.items:
            self.items['insert_values'] = []
        for row in rows:
            self.items['insert_values'].append(list(row.values()))
        return self

    def returning(self, *items):
        if 'returning' not in self.items:
            self.items['returning'] = []
        self.items['returning'] += self._stripItems(items)
        return self

    def insertRaw(self, sql, args):
        if not isinstance(self.composer, InsertRaw):
            self.composer = InsertRaw()
        self.items['sql'] = sql
        self.items['args'] = args
        return self

    def delete(self):
        if not isinstance(self.composer, Delete):
            self.composer = Delete()
        return self

    async def create(self, items):
        self.composer = Create()
        self.items = items
        return await self.exec()

    async def create_index(self, items):
        self.composer = CreateIndex()
        self.items = items
        return await self.exec()

    async def rename_table(self, table_name_old, table_name_new):
        self.composer = RenameTable()
        self.items = {'table': table_name_old, 'rename': table_name_new}
        return await self.exec()

    async def drop_table(self, table_name, ifExists=False):
        self.composer = DropTable()
        self.items = {'table': table_name, 'if_exists': ifExists}
        return await self.exec()

    async def alter(self, items):
        self.composer = Alter()
        self.items = items
        return await self.exec()

    async def alter_index(self, items):
        self.composer = AlterIndex()
        self.items = items
        return await self.exec()

    async def drop_index(self, index_name):
        self.composer = DropIndex()
        self.items = {'index': index_name}
        return await self.exec()

    async def drop_primary_key(self, table_name, pkey):
        self.composer = DropPrimaryKey()
        self.items = {'table': table_name, 'pkey': pkey}
        return await self.exec()

    async def drop_constraint(self, table_name, constraint_name):
        self.composer = DropConstraint()
        self.items = {'table': table_name, 'constraint_name': constraint_name}
        return await self.exec()

class Statement:
    """prepare sql scaffold, each step compose a piece in sql, add args and add build parser tree"""

    def __init__(self):
        self.sql = ''
        self.parser = None
        self.args = []
        self.steps = []

    def compose(self, items):
        self.sql = ''
        self.parser = CRUD()
        self.args = []
        for step in self.steps:
            step = getattr(self, step)
            step(items)
        params = self.parser.compile()
        return self.sql.format(**params), self.args

    def get_field_sql(self, item):
        # check table.field
        pair = item.split('.')
        if len(pair) > 2:
            raise UserWarning("Identifier has more than two dots")
        if len(pair) == 2:
            full_field = self.parser('full_field')
            full_field.add(self.parser('identifier', pair[0]))
            full_field.add(self.get_field_sql(pair[1]))
            return full_field

        # field
        if '->>' in item:
            field, *attrs = item.split('->')
            item_sql = self.parser('jsonb_text')
            item_sql.add(self.parser('identifier', field))
            for attr in attrs:
                attr = attr.lstrip('>')
                item_sql.add(self.parser('literal', attr))
        elif '->' in item:
            field, *attrs = item.split('->')
            item_sql = self.parser('jsonb')
            item_sql.add(self.parser('identifier', field))
            for attr in attrs:
                item_sql.add(self.parser('literal', attr))
        elif item == '*':
            item_sql = self.parser('const', '*')
        else:
            item_sql = self.parser('identifier', item)
        return item_sql


class Raw(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['raw']

    def raw(self, items):
        self.sql = items['sql']
        self.args = items['args']
        return self


class Select(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['select', 'join', 'where', 'order_by', 'group_by', 'limit']

    def select(self, items):
        self.sql = 'SELECT {select} FROM {table}'
        self.parser('identifier', items['table'], data_key='table')
        select = self.parser('select', data_key='select')
        fields = items['select']
        if len(fields) == 0:
            fields = ['*']
        for field in fields:
            if isinstance(field, str):
                select.add(self.select_item(field))
            else:
                raise UserWarning("Not implemented")
        return self

    def select_item(self, item):
        # check field AS alias
        pair = re.split('\s+as\s+', item, flags=re.IGNORECASE)
        if len(pair) > 2:
            raise UserWarning("Too many 'AS' in select() argument")
        if len(pair) == 2:
            select_as = self.parser('select_as')
            select_as.add(self.get_field_sql(pair[0]))
            select_as.add(self.parser('identifier', pair[1]))
            return select_as
        return self.get_field_sql(item)

    def join(self, items):
        if 'join' in items:
            self.sql += ' {join}'
            join = self.parser('join', data_key='join')
            for clause in items['join']:
                if len(clause) != 4:
                    raise UserWarning("Invalid arguments for join()")
                join.add(self.join_item(*clause))
        return self

    def join_item(self, table_name, left, op, right):
        join_item = self.parser('join_item')
        join_item.add(self.parser('identifier', table_name))
        join_item.add(self.get_field_sql(left))
        join_item.add(self.parser('const', op))
        join_item.add(self.get_field_sql(right))
        return join_item

    def where(self, items):
        where = None
        if 'where' in items:
            self.sql += ' WHERE {where}'
            where = self.parser('where', data_key='where')
            for clause in items['where']:
                if len(clause) == 2:
                    if isinstance(clause[1], list):
                        where.add(self.where_item(clause[0], 'IN', clause[1]))
                    else:
                        where.add(self.where_item(clause[0], '=', clause[1]))
                elif len(clause) == 3:
                    where.add(self.where_item(*clause))
                else:
                    raise UserWarning("Invalid arguments for DB.where")
        if 'where_raw' in items:
            if where is None:
                self.sql += ' WHERE {where}'
                where = self.parser('where', data_key='where')
            for query in items['where_raw']:
                where.add(self.parser('raw', query))
        return self

    def where_item(self, field, op, val):
        if op.upper() == 'IN':
            where_item = self.parser('where_in_item')
            where_item.add(self.get_field_sql(field))
            idx = []
            for v in val:
                self.args.append(v)
                idx.append(len(self.args))
            where_item.add(idx)
        elif op.upper() == 'NOT IN':
            where_item = self.parser('where_not_in_item')
            where_item.add(self.get_field_sql(field))
            idx = []
            for v in val:
                self.args.append(v)
                idx.append(len(self.args))
            where_item.add(idx)
        else:
            where_item = self.parser('where_item')
            where_item.add(op)
            where_item.add(self.get_field_sql(field))
            if '->' in field and '->>' not in field:
                val = json.dumps(val)
            if val is None:
                where_item.add(None)
            else:
                self.args.append(val)
                where_item.add(len(self.args))
        return where_item

    def order_by(self, items):
        order_by = None
        if 'order_by' in items:
            self.sql += ' ORDER BY {order_by}'
            order_by = self.parser('order_by', data_key='order_by')
            for item in items['order_by']:
                if isinstance(item, str):
                    order_by.add(self.order_by_item(item))
                else:
                    raise UserWarning("order_by(): invalid arguments")
        if 'order_by_raw' in items:
            if order_by is None:
                self.sql += ' ORDER BY {order_by}'
                order_by = self.parser('order_by', data_key='order_by')
            for query in items['order_by_raw']:
                order_by.add(self.parser('raw', query))
        return self

    def order_by_item(self, item):
        if item.upper().endswith(' ASC'):
            order_by_item = self.parser('order_by_item')
            order_by_item.add(self.get_field_sql(item[:-4].strip()))
            order_by_item.add('ASC')
        elif item.upper().endswith(' DESC'):
            order_by_item = self.parser('order_by_item')
            order_by_item.add(self.get_field_sql(item[:-5].strip()))
            order_by_item.add('DESC')
        else:
            order_by_item = self.parser('order_by_item', self.get_field_sql(item))
        return order_by_item

    def group_by(self, items):
        if 'group_by' in items:
            self.sql += ' GROUP BY {group_by}'
            group_by = self.parser('group_by', data_key='group_by')
            for item in items['group_by']:
                if isinstance(item, str):
                    group_by.add(self.get_field_sql(item))
                else:
                    raise UserWarning("group_by(): invalid arguments")
        return self

    def limit(self, items):
        if 'limit' in items:
            self.sql += ' LIMIT {limit}'
            self.args.append(items['limit'])
            self.parser('limit', len(self.args), data_key='limit')
        return self


class SelectRaw(Select):

    def select(self, items):
        self.sql = 'SELECT {select} FROM {table}'
        self.parser('identifier', items['table'], data_key='table')
        select = self.parser('select', data_key='select')
        fields = items['select']
        if len(fields) == 0:
            fields = ['*']
        for field in fields:
            if isinstance(field, str):
                select.add(self.parser('raw', field))
            else:
                raise UserWarning("Not implemented")
        return self


class Update(Select):

    def __init__(self):
        super().__init__()
        self.steps = ['update', 'where']

    def update(self, items):
        self.sql = 'UPDATE {table} SET {update}'
        self.parser('identifier', items['table'], data_key='table')
        from_table = None
        if 'update_from' in items:
            self.sql += ' FROM {update_from}'
            self.parser('identifier', items['update_from'], data_key='update_from')
            from_table = items['update_from']+'.'
        update = self.parser('update', data_key='update')
        for item, value in items['update'].items():
            if isinstance(item, str):
                item = item.strip()
                if from_table is not None and isinstance(value, str) and from_table in value:
                    update.add(self.update_from_item(item, value.strip()))
                else:
                    update.add(self.update_item(item, value))
            else:
                raise UserWarning("Not implemented")
        return self

    def update_from_item(self, item, val):
        if '->>' in item:
            raise UserWarning("Not implemented")
        elif '->' in item:
            raise UserWarning("Not implemented")
        else:
            update_from_item = self.parser('update_from_item')
            update_from_item.add(self.get_field_sql(item))
            update_from_item.add(self.get_field_sql(val))
            return update_from_item

    def update_item(self, item, val):
        if '->>' in item:
            raise UserWarning("->> is not valid for update()")
        elif '->' in item:
            return self.update_jsonb(item, val)
        else:
            update_item = self.parser('update_item')
            update_item.add(self.get_field_sql(item))
            if isinstance(val, dict):
                val = json.dumps(val)
            if val is None:
                update_item.add(None)
            else:
                self.args.append(val)
                update_item.add(len(self.args))
            return update_item

    def update_jsonb(self, item, val):
        update_jsonb = self.parser('update_jsonb')
        field, *attrs = item.split('->')
        update_jsonb.add(self.parser('identifier', field))
        for attr in attrs:
            update_jsonb.add(attr)
        update_jsonb.add(val)
        return update_jsonb

    def where(self, items):
        if 'where' in items:
            self.sql += ' WHERE {where}'
            from_table = None
            if 'update_from' in items:
                from_table = items['update_from']+'.'
            where = self.parser('where', data_key='where')
            for clause in items['where']:
                if len(clause) == 2:
                    field, value = clause
                    op = '='
                elif len(clause) == 3:
                    field, op, value = clause
                else:
                    raise UserWarning("Invalid arguments for DB.where")
                if from_table is not None and (from_table in field or isinstance(value, str) and from_table in value):
                    where.add(self.where_from_item(field, op, value))
                else:
                    where.add(self.where_item(field, op, value))
        return self

    def where_from_item(self, field, op, value):
        where_from_item = self.parser('where_from_item')
        where_from_item.add(self.get_field_sql(field))
        where_from_item.add(op)
        where_from_item.add(self.get_field_sql(value))
        return where_from_item


class Insert(Select):

    def __init__(self):
        super().__init__()
        self.steps = ['insert', 'returning']

    def insert(self, items):
        self.sql = 'INSERT INTO {table} ({keys}) VALUES {values}'
        self.parser('identifier', items['table'], data_key='table')
        insert_keys = self.parser('insert_keys', data_key='keys')
        for key in items['insert_keys']:
            insert_keys.add(self.parser('identifier', key))
        insert_values = self.parser('insert_values', data_key='values')
        for values in items['insert_values']:
            insert_value = self.parser('insert_value')
            for value in values:
                if isinstance(value, dict):
                    value = json.dumps(value)
                self.args.append(value)
                insert_value.add(len(self.args))
            insert_values.add(insert_value)
        return self

    def returning(self, items):
        if 'returning' in items:
            self.sql += ' RETURNING {returning}'
            returning = self.parser('returning', data_key='returning')
            fields = items['returning']
            if len(fields) == 0:
                fields = ['*']
            for field in fields:
                if isinstance(field, str):
                    returning.add(self.get_field_sql(field))
                else:
                    raise UserWarning("Invalid field in RETURNING - {}".format(field))
        return self


class InsertRaw(Select):

    def __init__(self):
        super().__init__()
        self.steps = ['insert']

    def insert(self, items):
        self.sql = 'INSERT INTO {table} ' + items['sql']
        self.args = items['args']
        self.parser('identifier', items['table'], data_key='table')
        return self


class Create(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['create']

    def create(self, items):
        self.sql = 'CREATE TABLE {table} ({columns})'
        self.parser('identifier', items['table'], data_key='table')
        create_columns = self.parser('create_columns', data_key='columns')
        for column in items['columns']:
            create_column = self.parser('create_column')
            create_column.add(self.parser('identifier', column['name']))
            create_column.add(self.parser('const', column['type']))
            create_column.add(bool(column['nullable']))
            create_column.add(self.create_column_default(column['default']))
            create_column.add(column['primary_key'])
            create_columns.add(create_column)
        for unique_cols in items['uniques']:
            unique_columns = self.parser('unique_columns')
            for unique_col in unique_cols:
                unique_columns.add(self.parser('identifier', unique_col))
            create_columns.add(unique_columns)
        if items['primary']:
            primary_columns = self.parser('primary_columns')
            for primary_col in items['primary']:
                primary_columns.add(self.parser('identifier', primary_col))
            create_columns.add(primary_columns)
        return self

    def create_column_default(self, default_val):
        if default_val is None:
            return None
        if isinstance(default_val, bool):
            val = 'TRUE' if default_val else 'FALSE'
            return self.parser('const', val)
        elif isinstance(default_val, int):
            return default_val
        elif default_val == "NULL" or default_val == 'NOW()':
            return self.parser('const', default_val)
        else:
            if isinstance(default_val, dict):
                default_val = json.dumps(default_val)
            return self.parser('literal', default_val)


class CreateIndex(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['create_index']

    def create_index(self, items):
        self.parser('identifier', items['table'], data_key='table')
        index_col = 'create_index_col'
        idx_cols = items['index_columns']
        self.sql = 'CREATE INDEX ON {table}({'+index_col+'})'
        index_columns = self.parser('index_columns', data_key=index_col)
        for idx_col in idx_cols:
            index_columns.add(self.parser('identifier', idx_col))
        return self


class RenameTable(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['rename']

    def rename(self, items):
        self.sql = 'ALTER TABLE {table} RENAME TO {rename}'
        self.parser('identifier', items['table'], data_key='table')
        self.parser('identifier', items['rename'], data_key='rename')
        return self


class DropTable(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['drop']

    def drop(self, items):
        if items['if_exists']:
            self.sql = 'DROP TABLE IF EXISTS {table}'
        else:
            self.sql = 'DROP TABLE {table}'
        self.parser('identifier', items['table'], data_key='table')
        return self


class DropIndex(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['drop']

    def drop(self, items):
        self.sql = 'DROP INDEX {index}'
        self.parser('identifier', items['index'], data_key='index')
        return self


class DropPrimaryKey(Statement):
    def __init__(self):
        super().__init__()
        self.steps = ['drop']

    def drop(self, items):
        self.sql = 'ALTER TABLE {table} DROP CONSTRAINT {pkey}'
        self.parser('identifier', items['table'], data_key='table')
        self.parser('identifier', items['pkey'], data_key='pkey')
        return self


class DropConstraint(Statement):
    def __init__(self):
        super().__init__()
        self.steps = ['drop']

    def drop(self, items):
        self.sql = 'ALTER TABLE {table} DROP CONSTRAINT {constraint_name}'
        self.parser('identifier', items['table'], data_key='table')
        self.parser('identifier', items['constraint_name'], data_key='constraint_name')
        return self


class Alter(Create):

    def __init__(self):
        super().__init__()
        self.steps = ['alter']

    def alter(self, items):
        self.sql = 'ALTER TABLE {table} {actions}'
        self.parser('identifier', items['table'], data_key='table')
        alter_actions = self.parser('alter_actions', data_key='actions')
        for column in items['columns']:
            alter_column = self.parser('alter_column')
            alter_column.add(column['action'])
            alter_column.add(self.parser('identifier', column['name']))
            if column['type'] is not None:
                alter_column.add(self.parser('const', column['type']))
            else:
                alter_column.add(None)
            alter_column.add(bool(column['nullable']))
            alter_column.add(self.create_column_default(column['default']))
            alter_column.add(column['primary_key'])
            alter_actions.add(alter_column)
        if items['primary']:
            add_primary_columns = self.parser('add_primary_columns')
            for primary_col in items['primary']:
                add_primary_columns.add(self.parser('identifier', primary_col))
            alter_actions.add(add_primary_columns)
        return self


class AlterIndex(Statement):

    def __init__(self):
        super().__init__()
        self.steps = ['alter_index']

    def alter_index(self, items):
        self.parser('identifier', items['table'], data_key='table')
        idx = 1
        # index
        for idx_cols in items['indexes']:
            index_col = 'create_index_col_'+str(idx)
            idx += 1
            if self.sql:
                self.sql += '; '
            self.sql += 'CREATE INDEX ON {table}({'+index_col+'})'
            index_columns = self.parser('index_columns', data_key=index_col)
            for idx_col in idx_cols:
                index_columns.add(self.parser('identifier', idx_col))
        # unique index
        for idx_cols in items['uniques']:
            index_col = 'create_index_col_'+str(idx)
            idx += 1
            if self.sql:
                self.sql += '; '
            self.sql += 'CREATE UNIQUE INDEX ON {table}({'+index_col+'})'
            index_columns = self.parser('index_columns', data_key=index_col)
            for idx_col in idx_cols:
                index_columns.add(self.parser('identifier', idx_col))
        return self


class Delete(Select):

    def __init__(self):
        super().__init__()
        self.steps = ['delete', 'where']

    def delete(self, items):
        self.sql = 'DELETE FROM {table}'
        self.parser('identifier', items['table'], data_key='table')
        return self


class ColumnBuilder:

    def __init__(self, name, type=None, nullable=True, default=None, unsigned=None, primary_key=False):
        self.column = {}
        self.column['name'] = name.strip()
        self.column['type'] = type.strip().upper() if type is not None else type
        self.column['nullable'] = nullable
        self.column['default'] = default
        self.column['unsigned'] = unsigned
        self.column['primary_key'] = primary_key
        self.column['action'] = 'add'
        self.uniques = None
        self.indexes = None

    def __call__(self, items):
        items['columns'].append(self.column)
        if self.indexes:
            items['indexes'].append(self.indexes)
        if self.uniques:
            items['uniques'].append(self.uniques)

    def serial(self):
        self.column["type"] = "SERIAL"
        return self

    def bigserial(self):
        self.column["type"] = "BIGSERIAL"
        return self

    def text(self):
        self.column["type"] = "VARCHAR"
        return self

    def string(self, length=255):
        self.column["type"] = "VARCHAR (%d)" % (int(length))
        return self

    def integer(self):
        self.column["type"] = "INTEGER"
        return self

    def bigint(self):
        self.column["type"] = "BIGINT"
        return self

    def numeric(self, precision, scale):
        self.column["type"] = "NUMERIC(%d, %d)" % (int(precision), int(scale))
        return self

    def timestamp(self):
        self.column["type"] = "TIMESTAMP"
        return self

    def timestamptz(self):
        self.column["type"] = "TIMESTAMPTZ"
        return self

    def boolean(self):
        self.column["type"] = "BOOLEAN"
        return self

    def jsonb(self):
        self.column["type"] = "JSONB"
        return self

    def nullable(self, isNullable=True):
        self.column["nullable"] = isNullable
        return self

    def primary_key(self):
        self.column["primary_key"] = True
        return self

    def unique(self):
        self.uniques = [self.column['name']]
        return self

    def index(self):
        self.indexes = [self.column['name']]
        return self

    def default(self, val):
        if val is None:
            val = 'NULL'
        self.column["default"] = val
        return self

    # ALTER TABLE
    def drop(self):
        self.column['action'] = 'drop'
        return self


class IndexBuilder:
    def __init__(self, columns, type='index'):
        self.type = type
        self.columns = [c.strip() for c in columns]

    def __call__(self, items):
        if self.type == 'unique':
            items['uniques'].append(self.columns)
        elif self.type == 'primary':
            items['primary'] = self.columns
        elif self.type == 'index':
            items['indexes'].append(self.columns)
        else:
            raise UserWarning("Unknown index type: {}".format(self.type))
