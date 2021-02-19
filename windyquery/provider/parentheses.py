from windyquery.ctx import Ctx

from ._base import Base


class Parentheses(Base):
    def __init__(self, ctx: Ctx, wrapped):
        super().__init__(ctx)
        self.wrapped = wrapped

    def __str__(self):
        return f'({self.wrapped})'
