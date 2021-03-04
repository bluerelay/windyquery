from .table import Table
from .select import Select
from .where import Where
from .limit import Limit
from .offset import Offset
from .group_by import GroupBy
from .order_by import OrderBy
from .join import Join
from .update import Update
from .from_table import FromTable
from .insert import Insert
from .returning import Returning
from .delete import Delete
from .schema import Schema
from .create import Create
from .drop import Drop
from .alter import Alter
from .on_conflict import OnConflict
from .raw import Raw
from .rrule import Rrule


tokens = (
    'START_SELECT', 'START_UPDATE', 'START_INSERT', 'START_DELETE', 'START_CREATE', 'START_DROP', 'START_ALTER', 'START_RAW',
    'TABLE', 'SELECT', 'WHERE', 'LIMIT', 'OFFSET', 'GROUP_BY', 'ORDER_BY', 'JOIN', 'UPDATE',
    'FROM_TABLE', 'INSERT', 'RETURNING', 'DELETE', 'SCHEMA', 'CREATE', 'DROP',
    'ALTER', 'ON_CONFLICT', 'RRULE', 'RAW',
)


class Collector(Table, Select, Where, Limit, Offset, GroupBy, OrderBy, Join, Update, FromTable, Insert,
                Returning, Delete, Schema, Create, Drop, Alter, OnConflict, Raw, Rrule):
    """collect user input"""
    pass
