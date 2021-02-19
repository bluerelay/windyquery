from ._base import Base, _rule
from .number import Number


class Offset(Base, Number):
    reserved = {**Base.reserved, **Number.reserved}
    tokens = Base.tokens + Number.tokens

    # rules
    _start = 'offset'

    @_rule('offset : NUMBER')
    def p_offset(self, p):
        param = self.provider.new_param()
        p[0] = self.provider.new_glue('OFFSET', param)
