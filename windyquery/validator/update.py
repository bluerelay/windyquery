import json
from ._base import _rule
from .expr import Expr
from windyquery.provider._base import JSONB
from windyquery.utils import unquote_literal


class Update(Expr):
    reserved = {**Expr.reserved, }
    tokens = Expr.tokens
    precedence = Expr.precedence

    # rules
    _start = 'updates'

    @_rule('updates : update')
    def p_updates_update(self, p):
        p[0] = p[1]

    @_rule('updates : updates COMMA update')
    def p_updates_comma_update(self, p):
        p[0] = self.provider.new_glue(p[1], p[3]).separator(', ')

    @_rule('''update : field EQ expr''')
    def p_update(self, p):
        if p[2] == '=' and p[1].kind == JSONB:
            jsonbCol = p[1].path[0]
            jsonbPath = '{' + ', '.join(p[1].path[1:]) + '}'
            jsonbVal = p[3].value
            if isinstance(jsonbVal, str):
                jsonbVal = unquote_literal(jsonbVal)
            jsonbVal = json.dumps(jsonbVal)
            p[0] = self.provider.new_glue(
                jsonbCol, p[2], f'jsonb_set({jsonbCol}, \'{jsonbPath}\', \'{jsonbVal}\')')
        else:
            p[0] = self.provider.new_glue(p[1], p[2].upper(), p[3])
