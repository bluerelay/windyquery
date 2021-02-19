from windyquery.ctx import Ctx

from ._base import Base, PARAM, FIELDLIST


class Biop(Base):
    def __init__(self, ctx: Ctx, op, l, r):
        super().__init__(ctx)
        self.op = op
        self.l = l
        self.r = r

    def __str__(self):
        if self.l.kind in (PARAM, FIELDLIST):
            self.l.match(self.r.kind)
        elif self.r.kind in (PARAM, FIELDLIST):
            self.r.match(self.l.kind)
        return f'{self.l} {self.op} {self.r}'
