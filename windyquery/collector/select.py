from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base, StartSelectToken


TOKEN = 'SELECT'


class SelectToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Select(Base):
    def select(self, *items: str):
        try:
            _items = [self.validator.validate_select(item) for item in items]
        except ValidationError as err:
            raise UserWarning(f'invalid SELECT: {err}') from None

        self.append(SelectToken(_items))
        self.add_start(StartSelectToken())
