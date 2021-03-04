from typing import Any
from ply.lex import LexToken

from ._base import Base, StartRawToken


TOKEN = 'RAW'


class RawToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Raw(Base):
    def raw(self, sql: str, *params: Any):
        args = list(params)
        self.paramOffset += len(args)
        value = {'sql': sql, 'params': args}
        self.append(RawToken(value))
        self.add_start(StartRawToken())
