from typing import Any
from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from windyquery.utils import process_value
from ._base import Base

TOKEN = 'WHERE'


class WhereToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Where(Base):
    def where(self, _where: str, *items: Any):
        if len(items) == 0 or '?' in _where:
            try:
                ctx = Ctx(self.paramOffset, items)
                sql = self.validator.validate_where(_where, ctx)
            except ValidationError as err:
                raise UserWarning(f'invalid WHERE: {err}') from None
            value = {'sql': sql, 'params': ctx.args}
        elif len(items) <= 2:
            if len(items) == 1:
                if isinstance(items[0], list):
                    operator = 'IN'
                else:
                    operator = '='
                val = items[0]
                if val is None:
                    operator = 'IS'
            else:
                operator = items[0]
                val = items[1]

            _where += f' {operator}'
            if val is None:
                _where += ' NULL'
                params = []
            else:
                jsonbTest = '->' in _where
                if operator == 'IN' or operator == 'NOT IN':
                    params = []
                    vs = []
                    for v in val:
                        processed, _ = process_value(v)
                        if processed == '?' or jsonbTest:
                            params.append(v)
                            vs.append('?')
                        else:
                            vs.append(str(processed))
                    _where += ' (' + ', '.join(vs) + ')'
                else:
                    params = []
                    processed, _ = process_value(val)
                    if processed == '?' or jsonbTest:
                        params.append(val)
                        _where += ' ?'
                    else:
                        _where += f' {processed}'

            try:
                ctx = Ctx(self.paramOffset, params)
                sql = self.validator.validate_where(_where, ctx)
            except ValidationError as err:
                raise UserWarning(f'invalid WHERE: {err}') from None
            value = {'sql': sql, 'params': ctx.args}
        else:
            raise UserWarning(f"Invalid WHERE: {_where} {items}")

        self.paramOffset += len(value['params'])
        self.append(WhereToken(value))
