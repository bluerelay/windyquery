import json
import re

from asyncpg import utils

class InvalidSqlError(RuntimeError):
    """raise when can not build a valid sql"""


class CRUD:
    """Base class for synthesize CRUD query"""

    def __init__(self):
        self.data = {}

    def compile(self):
        sqlParams = {}
        for name, sql in self.data.items():
            sqlParams[name] = sql()
        return sqlParams

    def __call__(self, sql, *args, data_key=None):
        if data_key is not None:
            if data_key not in self.data:
                self.data[data_key] = SQL(sql, *args)
            return self.data[data_key]
        else:
            return SQL(sql, *args)


class SQL:
    """class used to represent SQL fragment"""

    def __init__(self, action, sql=None):
        self.action = getattr(self, action)
        self.sql = sql

    def add(self, sql):
        if self.sql is None:
            self.sql = []
        self.sql.append(sql)

    def __call__(self):
        if isinstance(self.sql, list):
            return self.action(*(s() if isinstance(s, SQL) else s for s in self.sql))
        elif isinstance(self.sql, SQL):
            return self.action(self.sql())
        else:
            return self.action(self.sql)

    def identifier(self, var):
        parts = var.split('.')
        # information_schema is an object can not be referred by identifier
        if len(parts) == 2 and parts[0] == 'information_schema':
            return var
        else:
            return utils._quote_ident(var)

    def literal(self, var):
        return utils._quote_literal(var)

    def const(self, var):
        allowed = ['*', '=', 'SERIAL', 'BIGSERIAL', 'VARCHAR', 'INTEGER', 'BIGINT', 'NUMERIC', 'TIMESTAMP', 'TIMESTAMPTZ', 'BOOLEAN', 'JSONB', 'TRUE', 'FALSE', 'NULL', 'NOW()']
        if var not in allowed and not re.match(r'VARCHAR \(\d+\)', var) and not re.match(r'NUMERIC\(\d+, \d+\)', var):
            raise InvalidSqlError("not allowed to use raw string: {}".format(var))
        return var

    def raw(self, var):
        return var

    def jsonb_text(self, field, *attrs):
        attrs = list(attrs)
        prefix = '->'.join([field] + attrs[:-1])
        return '{}->>{}'.format(prefix, attrs[-1])

    def jsonb(self, field, *attrs):
        attrs = list(attrs)
        return '->'.join([field]+attrs)

    def select(self, *items):
        return ', '.join(items)

    def select_as(self, field, alias):
        return '{} AS {}'.format(field, alias)

    def full_field(self, table_name, field):
        return '{}.{}'.format(table_name, field)

    def where(self, *items):
        return ' AND '.join(items)

    def where_item(self, op, field, idx):
        allowed = ['=', '<', '<=', '>', '>=', 'IN', 'NOT IN', 'IS', 'IS NOT', 'LIKE']
        op = op.upper()
        if op not in allowed:
            raise InvalidSqlError('invalid operator in where clause: {}'.format(op))
        if idx is None:
            return '{} {} NULL'.format(field, op)
        else:
            return '{} {} ${}'.format(field, op, idx)

    def where_in_item(self, field, idx):
        padded = []
        for id in idx:
            padded.append('${}'.format(id))
        place_hodlers = ', '.join(padded)
        return '{} IN ({})'.format(field, place_hodlers)

    def where_not_in_item(self, field, idx):
        padded = []
        for id in idx:
            padded.append('${}'.format(id))
        place_hodlers = ', '.join(padded)
        return '{} NOT IN ({})'.format(field, place_hodlers)

    def join(self, *items):
        return ' '.join(items)

    def join_item(self, table, left_expr, join_op, right_expr):
        return "JOIN {} ON {} {} {}".format(table, left_expr, join_op, right_expr)

    def order_by(self, *items):
        return ', '.join(items)

    def order_by_item(self, field, dir=None):
        if dir is None:
            return field
        else:
            if dir not in ['ASC', 'DESC']:
                raise InvalidSqlError('invalid order by dir {}'.format(dir))
            return "{} {}".format(field, dir)

    def group_by(self, *items):
        return ', '.join(items)

    def limit(self, idx):
        return "${}".format(idx)

    def update(self, *items):
        return ', '.join(items)

    def update_item(self, field, idx):
        if idx is None:
            return '{} = NULL'.format(field)
        else:
            return "{} = ${}".format(field, idx)

    def update_from_item(self, field, from_field):
        return "{} = {}".format(field, from_field)

    def where_from_item(self, field, op, value):
        allowed = ['=', '<', '<=', '>', '>=', 'IN', 'IS']
        op = op.upper()
        if op not in allowed:
            raise InvalidSqlError('invalid operator in where clause: {}'.format(op))
        return "{} {} {}".format(field, op, value)

    def update_jsonb(self, field, *attrs):
        update = attrs[-1]
        for attr in reversed(attrs[:-1]):
            update = {attr: update}
        return "{} = COALESCE({}, '{}') || '{}'".format(field, field, json.dumps({}), json.dumps(update))

    def insert_keys(self, *keys):
        return ', '.join(keys)

    def insert_value(self, *index):
        insertStr = ', '.join('${}'.format(idx) for idx in index)
        return '({})'.format(insertStr)

    def insert_values(self, *values):
        return ', '.join(values)

    def returning(self, *values):
        return ', '.join(values)

    def create_columns(self, *columns):
        return ', '.join(columns)

    def create_column(self, name, type, nullable, default, primary_key):
        s = "{} {}".format(name, type)
        if not nullable:
            s += " NOT NULL"
        if default is not None:
            s += ' DEFAULT {}'.format(default)
        if primary_key:
            s += " PRIMARY KEY"
        return s

    def unique_columns(self, *columns):
        return 'UNIQUE ({})'.format(', '.join(columns))

    def primary_columns(self, *columns):
        return 'PRIMARY KEY ({})'.format(', '.join(columns))

    def index_columns(self, *columns):
        return ', '.join(columns)

    def alter_actions(self, *columns):
        return ', '.join(columns)

    def alter_column(self, action, name, type, nullable, default, primary_key):
        if action == 'drop':
            s = 'DROP COLUMN IF EXISTS {}'.format(name)
        else: # default to ADD
            s = "ADD COLUMN {} {}".format(name, type)
            if not nullable:
                s += " NOT NULL"
            if default is not None:
                s += ' DEFAULT {}'.format(default)
            if primary_key:
                s += " PRIMARY KEY"
        return s

    def add_primary_columns(self, *columns):
        return 'ADD PRIMARY KEY ({})'.format(', '.join(columns))
