from ._base import _rule
from .field import Field
from .operators.operator import Operator


class Expr(Field, Operator):
    reserved = {**Field.reserved, **Operator.reserved}
    tokens = Field.tokens + Operator.tokens
    precedence = Field.precedence + Operator.precedence

    # rules
    _start = 'expr'

    @_rule('''expr : expr EQ expr
                   | expr NE expr
                   | expr NN expr
                   | expr LE expr
                   | expr LS expr
                   | expr GE expr
                   | expr GT expr
                   | expr IS expr
                   | expr LIKE expr
                   | expr ILIKE expr
                   | expr DPIPE expr
                   | expr MINUS expr
                   | expr PLUS expr
                   | expr MULTI expr
                   | expr DIVIDE expr
                   | expr MODULAR expr''')
    def p_expr(self, p):
        p[0] = self.provider.new_biop(p[2].upper(), p[1], p[3])

    @_rule('expr : expr IS NOT expr')
    def p_expr2(self, p):
        p[0] = self.provider.new_biop('IS NOT', p[1], p[4])

    @_rule('expr : expr IS DISTINCT FROM expr')
    def p_expr3(self, p):
        p[0] = self.provider.new_biop('IS DISTINCT FROM', p[1], p[5])

    @_rule('expr : expr IS NOT DISTINCT FROM expr')
    def p_expr4(self, p):
        p[0] = self.provider.new_biop('IS NOT DISTINCT FROM', p[1], p[6])

    @_rule('expr : expr IN LPAREN exprs RPAREN')
    def p_expr5(self, p):
        p[0] = self.provider.new_biop('IN', p[1], p[4])

    @_rule('expr : expr NOT IN LPAREN exprs RPAREN')
    def p_expr6(self, p):
        p[0] = self.provider.new_biop('NOT IN', p[1], p[5])

    @_rule('exprs : expr')
    def p_exprs_expr(self, p):
        p[0] = self.provider.new_fieldlist(p[1])

    @_rule('exprs : exprs COMMA expr')
    def p_exprs_comma_expr(self, p):
        p[0] = p[1].append(p[3])

    @_rule('expr : field')
    def p_expr_field(self, p):
        p[0] = p[1]
