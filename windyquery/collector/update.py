from typing import Any
from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base, StartUpdateToken

TOKEN = 'UPDATE'


class UpdateToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Update(Base):
    def update(self, sql: str, *params: Any):
        try:
            ctx = Ctx(self.paramOffset, params)
            sql = self.validator.validate_update(sql, ctx)
            self.paramOffset += len(ctx.args)
        except ValidationError as err:
            raise UserWarning(f'invalid UPDATE: {err}') from None
        value = {'sql': sql, 'params': ctx.args}

        self.append(UpdateToken(value))
        self.add_start(StartUpdateToken())
