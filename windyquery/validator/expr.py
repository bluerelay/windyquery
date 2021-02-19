from ._base import _rule, ValidationError
from .field import Field
from .operators.operator import Operator


class Expr(Field, Operator):
    reserved = {**Field.reserved, **Operator.reserved}
    tokens = Field.tokens + Operator.tokens
    precedence = Operator.precedence

    # rules
    _start = 'expr'

    @_rule('''expr : field EQ field
                   | field NE field
                   | field NN field
                   | field LE field
                   | field LS field
                   | field GE field
                   | field GT field
                   | field IS field
                   | field LIKE field
                   | field ILIKE field''')
    def p_expr(self, p):
        p[0] = self.provider.new_biop(p[2].upper(), p[1], p[3])

    @_rule('expr : field IS NOT field')
    def p_expr2(self, p):
        p[0] = self.provider.new_biop('IS NOT', p[1], p[4])

    @_rule('expr : field IS DISTINCT FROM field')
    def p_expr3(self, p):
        p[0] = self.provider.new_biop('IS DISTINCT FROM', p[1], p[5])

    @_rule('expr : field IS NOT DISTINCT FROM field')
    def p_expr4(self, p):
        p[0] = self.provider.new_biop('IS NOT DISTINCT FROM', p[1], p[6])

    @_rule('expr : field IN LPAREN fields RPAREN')
    def p_expr5(self, p):
        p[0] = self.provider.new_biop('IN', p[1], p[4])

    @_rule('expr : field NOT IN LPAREN fields RPAREN')
    def p_expr6(self, p):
        p[0] = self.provider.new_biop('NOT IN', p[1], p[5])

    @_rule('fields : field')
    def p_fields_field(self, p):
        p[0] = self.provider.new_fieldlist(p[1])

    @_rule('fields : fields COMMA field')
    def p_fields_comma_field(self, p):
        p[0] = p[1].append(p[3])
