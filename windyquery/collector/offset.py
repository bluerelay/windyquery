from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base

TOKEN = 'OFFSET'


class OffsetToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Offset(Base):
    def offset(self, size: int):
        try:
            ctx = Ctx(self.paramOffset, [size])
            sql = self.validator.validate_offset(str(size), ctx)
        except ValidationError as err:
            raise UserWarning(f'invalid OFFSET: {err}') from None

        self.paramOffset += 1
        self.append(OffsetToken({'sql': sql, 'params': ctx.args}))
