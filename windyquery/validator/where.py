from ._base import _rule
from .expr import Expr


class Where(Expr):
    reserved = {**Expr.reserved,
                'AND': 'AND',
                'OR': 'OR'}
    tokens = Expr.tokens + ('AND', 'OR')

    # Tokens
    t_AND = r'AND'
    t_OR = r'OR'

    # rules
    _start = 'where'

    precedence = Expr.precedence + (
        ('left', 'OR'),
        ('left', 'AND'),
    )

    @_rule('''where : where AND where
                    | where OR where''')
    def p_where_and(self, p):
        p[0] = self.provider.new_glue(p[1], p[2].upper(), p[3])

    @_rule('where : LPAREN where RPAREN')
    def p_where_group(self, p):
        p[0] = self.provider.new_parentheses(p[2])

    @_rule('where : expr')
    def p_where_op(self, p):
        p[0] = p[1]
