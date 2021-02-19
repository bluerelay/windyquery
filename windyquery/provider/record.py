from windyquery.ctx import Ctx

from ._base import Base


class Record(Base):
    def __init__(self, ctx: Ctx, value, kind):
        super().__init__(ctx)
        self.kind = kind
        self.value = value

    def __str__(self):
        return str(self.value)
