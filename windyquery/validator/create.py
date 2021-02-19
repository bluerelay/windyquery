from ._base import _rule
from .constraint import Constraint


class Create(Constraint):
    reserved = {
        **Constraint.reserved,
    }

    tokens = Constraint.tokens

    precedence = Constraint.precedence

    # rules
    _start = 'create'

    @_rule('create : column')
    def p_create_column(self, p):
        p[0] = p[1]

    @_rule('create : constraintname tableconstraint')
    def p_create_tableconstraint(self, p):
        p[0] = self.provider.new_glue(p[1], p[2])

    @_rule('create : LIKE fullname')
    def p_create_like(self, p):
        p[0] = f'LIKE {p[2]}'

    @_rule('create : fullname_json')
    def p_create_index(self, p):
        p[0] = p[1]

    @_rule('create : LPAREN fullname_json RPAREN')
    def p_create_index_paren(self, p):
        p[0] = self.provider.new_glue('(', p[2], ')').separator('')
