from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base


TOKEN = 'RETURNING'


class ReturningToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Returning(Base):
    def returning(self, *items: str):
        try:
            _items = [self.validator.validate_select(item) for item in items]
        except ValidationError as err:
            raise UserWarning(f'invalid RETURNING: {err}') from None

        self.append(ReturningToken(_items))
