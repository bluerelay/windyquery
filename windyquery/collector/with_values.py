from typing import List, Any
from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base


TOKEN = 'WITH_VALUES'


class WithValuesToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class WithValues(Base):
    def with_values(self, name: str, columns: List[str], values: List[Any]):
        try:
            name = self.validator.validate_identifier(name)
            sqlColumns = self.validator.validate_with_columns(columns)
            sqlValues = []
            args = []
            for row in values:
                ctx = Ctx(self.paramOffset, [])
                sqlVal = self.validator.validate_with_values(ctx, row)
                if sqlVal:
                    sqlValues.append(sqlVal)
                    self.paramOffset += len(ctx.args)
                    args += ctx.args
        except ValidationError as err:
            raise UserWarning(f'invalid WITH VALUES: {err}') from None

        self.append(WithValuesToken(
            {'name': name, 'columns': sqlColumns, 'values': ', '.join(sqlValues), 'params': args}))
