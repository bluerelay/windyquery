import json
from windyquery.ctx import Ctx

from ._base import Base, PARAM, JSONB


class Param(Base):
    def __init__(self, ctx: Ctx):
        super().__init__(ctx)
        self.kind = PARAM
        self.pos = 0

    def __str__(self):
        return f'${self.pos + self.ctx.param_offset}'

    def set_pos(self, pos):
        self.pos = pos
        return self

    def match(self, kind):
        if kind == JSONB:
            value = self.ctx.args[self.pos]
            self.ctx.args[self.pos] = json.dumps(value)
        return self
