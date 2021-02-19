from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base

TOKEN = 'LIMIT'


class LimitToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Limit(Base):
    def limit(self, size: int):
        try:
            ctx = Ctx(self.paramOffset, [size])
            sql = self.validator.validate_limit(str(size), ctx)
        except ValidationError as err:
            raise UserWarning(f'invalid LIMIT: {err}') from None

        self.paramOffset += 1
        self.append(LimitToken({'sql': sql, 'params': ctx.args}))
