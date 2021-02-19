from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base

TOKEN = 'TABLE'


class TableToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Table(Base):
    def table(self, name: str):
        try:
            name = self.validator.validate_tablename(name)
        except ValidationError as err:
            raise UserWarning(f'invalid table name: {err}') from None
        self.append(TableToken(name))
