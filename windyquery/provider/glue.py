from windyquery.ctx import Ctx

from ._base import Base


class Glue(Base):
    def __init__(self, ctx: Ctx, items):
        super().__init__(ctx)
        self.items = list(filter(None, items))
        self._sep = ' '

    def __str__(self):
        return self._sep.join([str(i) for i in self.items])

    def append(self, i):
        if i is not None:
            self.items.append(i)
        return self

    def separator(self, s: str):
        self._sep = s
        return self
