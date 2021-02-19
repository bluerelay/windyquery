from typing import List, Any
from windyquery.ctx import Ctx
from windyquery.utils import process_value

from .field import Field
from .table import Table
from .select import Select
from .where import Where
from .limit import Limit
from .offset import Offset
from .order_by import OrderBy
from .join import Join
from .update import Update
from .value_list import ValueList
from .schema import Schema
from .create import Create
from .alter import Alter

from ._base import ValidationError


_field = Field()
_table = Table()
_select = Select()
_where = Where()
_limit = Limit()
_offset = Offset()
_order_by = OrderBy()
_join = Join()
_update = Update()
_value_list = ValueList()
_schema = Schema()
_create = Create()
_alter = Alter()


class Validator:
    """validate the input"""

    def validate_tablename(self, s: str) -> str:
        return _table.parse(s, Ctx())

    def validate_select(self, s: str) -> str:
        return _select.parse(s, Ctx())

    def validate_order_by(self, s: str) -> str:
        return _order_by.parse(s, Ctx())

    def validate_group_by(self, s: str) -> str:
        return _field.parse(s, Ctx())

    def validate_limit(self, s: str, ctx: Ctx) -> str:
        return _limit.parse(s, ctx)

    def validate_offset(self, s: str, ctx: Ctx) -> str:
        return _offset.parse(s, ctx)

    def validate_where(self, s: str, ctx: Ctx) -> str:
        return _where.parse(s, ctx)

    def validate_join(self, tbl, s: str, ctx: Ctx) -> str:
        tbl = _field.sanitize_identifier(tbl)
        return f'JOIN {tbl} ON ' + _join.parse(s, ctx)

    def validate_update(self, s: str, ctx: Ctx) -> str:
        return _update.parse(s, ctx)

    def validate_insert_columns(self, columns: List[str]) -> str:
        cols = [_field.sanitize_identifier(col) for col in columns]
        return '(' + ', '.join(cols) + ')'

    def validate_insert_values(self, values: List[Any], ctx: Ctx) -> str:
        results = []
        for val in values:
            val, p = process_value(val)
            if p is not None:
                ctx.args.append(p)
            results.append(str(val))
        return '(' + _value_list.parse(','.join(results), ctx) + ')'

    def validate_identifier(self, s: str) -> str:
        return _field.sanitize_identifier(s)

    def validate_schema(self, s: str) -> str:
        return _schema.parse(s, Ctx())

    def validate_create(self, s: str) -> str:
        return _create.parse(s, Ctx())

    def validate_alter(self, s: str) -> str:
        return _alter.parse(s, Ctx())
