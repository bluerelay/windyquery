from windyquery.provider._base import JSONB
from ._base import _rule
from .fullname import Fullname
from .number import Number
from .operators.minus import Minus


class FullnameJson(Fullname, Number, Minus):
    reserved = {**Fullname.reserved, **Number.reserved, **Minus.reserved}
    tokens = Fullname.tokens + Number.tokens + \
        Minus.tokens + ('ARROW', 'DARROW',)
    precedence = Fullname.precedence + Minus.precedence

    # Tokens
    t_ARROW = r'->'
    t_DARROW = r'->>'

    # rules
    _start = 'fullname_json'

    @_rule('fullname_json : fullname attribute')
    def p_fullname_json(self, p):
        p[0] = self.provider.new_record(f'{p[1]}{p[2]}', p[2].kind)
        p[0].path = [p[1]] + p[2].path

    @_rule('attribute : ARROW NAME attribute')
    def p_attribute(self, p):
        p2 = self.sanitize_literal(p[2])
        if p[3].value:
            kind = p[3].kind
            path = [p[2]] + p[3].path
        else:
            kind = JSONB
            path = [p[2]]
        p[0] = self.provider.new_record(f'->{p2}{p[3]}', kind)
        p[0].path = path

    @_rule('attribute : ARROW NUMBER attribute')
    def p_attribute_num(self, p):
        if p[3].value:
            kind = p[3].kind
            path = [f'{p[2]}'] + p[3].path
        else:
            kind = JSONB
            path = [f'{p[2]}']
        p[0] = self.provider.new_record(f'->{p[2]}{p[3]}', kind)
        p[0].path = path

    @_rule('attribute : ARROW MINUS NUMBER attribute')
    def p_attribute_minus_num(self, p):
        if p[4].value:
            kind = p[4].kind
            path = [f'-{p[3]}'] + p[4].path
        else:
            kind = JSONB
            path = [f'-{p[3]}']
        p[0] = self.provider.new_record(f'->-{p[3]}{p[4]}', kind)
        p[0].path = path

    @_rule('attribute : DARROW NAME')
    def p_attribute_darrow(self, p):
        p2 = self.sanitize_literal(p[2])
        p[0] = self.provider.new_record(f'->>{p2}')
        p[0].path = [p[2]]

    @_rule('attribute : DARROW NUMBER')
    def p_attribute_darrow_num(self, p):
        p[0] = self.provider.new_record(f'->>{p[2]}')
        p[0].path = [f'{p[2]}']

    @_rule('attribute : DARROW MINUS NUMBER')
    def p_attribute_darrow_minus_num(self, p):
        p[0] = self.provider.new_record(f'->>-{p[3]}')
        p[0].path = [f'-{p[3]}']

    @ _rule('attribute : empty')
    def p_attribute_empty(self, p):
        p[0] = self.provider.new_record('')
        p[0].path = []
