from ._base import _rule
from .alias import Alias
from .field import Field


class Select(Field, Alias):
    reserved = {**Field.reserved, **Alias.reserved}
    tokens = Field.tokens + Alias.tokens

    # rules
    _start = 'select'

    @_rule('select : field')
    def p_select_field(self, p):
        p[0] = p[1]

    @_rule('select : field NAME')
    def p_select_field_as_name(self, p):
        p2 = self.sanitize_identifier(p[2])
        p[0] = self.provider.new_glue(p[1], p2)

    @_rule('select : field AS NAME')
    def p_select_field_as_name2(self, p):
        p3 = self.sanitize_identifier(p[3])
        p[0] = self.provider.new_glue(p[1], 'AS', p3)
