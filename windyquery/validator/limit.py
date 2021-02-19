from ._base import Base, _rule
from .number import Number


class Limit(Base, Number):
    reserved = {**Base.reserved, **Number.reserved}
    tokens = Base.tokens + Number.tokens

    # rules
    _start = 'limit'

    @_rule('limit : NUMBER')
    def p_limit(self, p):
        param = self.provider.new_param()
        p[0] = self.provider.new_glue('LIMIT', param)
