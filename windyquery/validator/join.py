from ._base import _rule
from .expr import Expr


class Join(Expr):
    reserved = {**Expr.reserved,
                'AND': 'AND',
                'OR': 'OR'}
    tokens = Expr.tokens + ('AND', 'OR')

    # Tokens
    t_AND = r'AND'
    t_OR = r'OR'

    # rules
    _start = 'join'

    precedence = Expr.precedence + (
        ('left', 'OR'),
        ('left', 'AND'),
    )

    @_rule('''join : join AND join
                   | join OR join''')
    def p_join_and(self, p):
        p[0] = self.provider.new_glue(p[1], f'{p[2].upper()}', p[3])

    @_rule('join : LPAREN join RPAREN')
    def p_join_group(self, p):
        p[0] = self.provider.new_parentheses(p[2])

    @_rule('join : expr')
    def p_join_op(self, p):
        p[0] = p[1]
