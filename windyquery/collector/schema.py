from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base

TOKEN = 'SCHEMA'


class SchemaToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Schema(Base):
    def schema(self, s: str):
        try:
            s = self.validator.validate_schema(s)
        except ValidationError as err:
            raise UserWarning(f'invalid schema: {err}') from None
        self.append(SchemaToken(s))
