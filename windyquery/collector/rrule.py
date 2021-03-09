from typing import List, Any
from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base


TOKEN = 'RRULE'


class RruleToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Rrule(Base):
    def rrule(self, name: str, rrulepos: int, slicepos: int, columns: List[str], values: List[Any]):
        try:
            name = self.validator.validate_identifier(name)
            if slicepos is not None:
                del columns[slicepos]
            sqlColumns = self.validator.validate_rrule_columns(columns)
            sqlValues = []
            args = []
            for row in values:
                ctx = Ctx(self.paramOffset, [])
                occurrences = slice(100000)
                if slicepos is not None:
                    if row[slicepos] != 'NULL':
                        occurrences = row[slicepos]
                    del row[slicepos]
                sqlVal = self.validator.validate_rrule_values(
                    rrulepos, row, ctx, occurrences)
                if sqlVal:
                    sqlValues.append(sqlVal)
                    self.paramOffset += len(ctx.args)
                    args += ctx.args
        except ValidationError as err:
            raise UserWarning(f'invalid RRULE: {err}') from None

        self.append(RruleToken(
            {'name': name, 'columns': sqlColumns, 'values': ', '.join(sqlValues), 'params': args}))
