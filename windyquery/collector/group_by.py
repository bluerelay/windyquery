from ply.lex import LexToken

from windyquery.validator import ValidationError
from ._base import Base

TOKEN = 'GROUP_BY'


class GroupByToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class GroupBy(Base):
    def group_by(self,  *items: str):
        try:
            _items = [self.validator.validate_group_by(item) for item in items]
        except ValidationError as err:
            raise UserWarning(f'invalid ORDER BY: {err}') from None

        self.append(GroupByToken(_items))
