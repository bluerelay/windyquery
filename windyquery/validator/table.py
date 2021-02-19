from ._base import _rule
from .fullname import Fullname
from .alias import Alias


class Table(Fullname, Alias):
    reserved = {**Fullname.reserved, **Alias.reserved}
    tokens = Fullname.tokens + Alias.tokens

    # rules
    _start = 'table'

    @_rule('table : fullname')
    def p_table_name(self, p):
        p[0] = self.provider.new_record(p[1])

    @_rule('table : fullname NAME')
    def p_table_name_as(self, p):
        p2 = self.sanitize_identifier(p[2])
        p[0] = self.provider.new_glue(p[1], p2)

    @_rule('table : fullname AS NAME')
    def p_table_name_as2(self, p):
        p3 = self.sanitize_identifier(p[3])
        p[0] = self.provider.new_glue(p[1], 'AS', p3)
