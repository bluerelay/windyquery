from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base, StartCreateToken


TOKEN = 'CREATE'


class CreateToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Create(Base):
    def create(self, *items: str):
        try:
            _items = [self.validator.validate_create(item) for item in items]
        except ValidationError as err:
            raise UserWarning(f'invalid CREATE: {err}') from None
        self.append(CreateToken(_items))
        self.add_start(StartCreateToken())
