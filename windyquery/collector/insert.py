from typing import List, Any
from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base, StartInsertToken


TOKEN = 'INSERT'


class InsertToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Insert(Base):
    def insert(self, columns: List[str], values: List[Any]):
        try:
            sqlColumns = self.validator.validate_insert_columns(columns)
            sqlValues = []
            args = []
            for row in values:
                ctx = Ctx(self.paramOffset, [])
                sqlValues.append(
                    self.validator.validate_insert_values(row, ctx))
                self.paramOffset += len(ctx.args)
                args += ctx.args
        except ValidationError as err:
            raise UserWarning(f'invalid INSERT: {err}') from None

        columns.sort()
        key = ','.join(columns)
        self.append(InsertToken(
            {'columns': sqlColumns, 'values': ', '.join(sqlValues), 'params': args, 'key': key}))
        self.add_start(StartInsertToken())
