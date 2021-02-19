from windyquery.ctx import Ctx

from ._base import Base, PARAM, FIELDLIST


class Fieldlist(Base):
    def __init__(self, ctx: Ctx, items):
        super().__init__(ctx)
        self.kind = FIELDLIST
        self.fields = list(items)

    def __str__(self):
        return '(' + ', '.join([str(i) for i in self.fields]) + ')'

    def append(self, i):
        self.fields.append(i)
        return self

    def match(self, kind):
        for i in self.fields:
            if i.kind == PARAM:
                i.match(kind)
        return self
