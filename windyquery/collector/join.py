from typing import Any
from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base

TOKEN = 'JOIN'


class JoinToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Join(Base):
    def join(self, tbl: str, *cond: Any):
        if len(cond) == 0:
            raise UserWarning(f'JOIN cannot be empty')

        if len(cond) == 1 or '?' in cond[0]:
            _join = cond[0]
            params = cond[1:]
        elif len(cond) == 3:
            _join = f'{cond[0]} {cond[1]} {cond[2]}'
            params = []
        else:
            raise UserWarning(f"Invalid JOIN: {tbl} {cond}")

        try:
            ctx = Ctx(self.paramOffset, params)
            sql = self.validator.validate_join(tbl, _join, ctx)
        except ValidationError as err:
            raise UserWarning(f'invalid JOIN: {err}') from None
        value = {'sql': sql, 'params': ctx.args}

        self.paramOffset += len(value['params'])
        self.append(JoinToken(value))
