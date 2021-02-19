from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base, StartAlterToken


TOKEN = 'ALTER'


class AlterToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Alter(Base):
    def alter(self, *items: str):
        try:
            _items = [self.validator.validate_alter(item) for item in items]
        except ValidationError as err:
            raise UserWarning(f'invalid ALTER: {err}') from None
        self.append(AlterToken(_items))
        self.add_start(StartAlterToken())
