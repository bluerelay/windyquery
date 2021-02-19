from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base

TOKEN = 'FROM_TABLE'


class FromTableToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class FromTable(Base):
    def from_table(self, name: str):
        try:
            name = self.validator.validate_tablename(name)
        except ValidationError as err:
            raise UserWarning(f'invalid table name: {err}') from None
        self.append(FromTableToken(name))
