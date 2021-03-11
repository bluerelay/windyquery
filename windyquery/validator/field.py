from ._base import Base, _rule
from .fullname_json import FullnameJson
from .values.text_val import TextVal
from .values.null import NULL
from .values.holder import Holder
from .values.true import TRUE
from .values.false import FALSE


class Field(FullnameJson, TextVal, NULL, Holder, TRUE, FALSE):
    reserved = {**Base.reserved, **TextVal.reserved,
                **FullnameJson.reserved, **NULL.reserved, **Holder.reserved, **TRUE.reserved, **FALSE.reserved}
    tokens = Base.tokens + TextVal.tokens + \
        FullnameJson.tokens + NULL.tokens + Holder.tokens + TRUE.tokens + FALSE.tokens
    precedence = FullnameJson.precedence

    # Tokens

    # rules
    _start = 'field'

    @_rule('''field : STAR
                    | NUMBER
                    | TEXTVAL
                    | NULL
                    | TRUE
                    | FALSE''')
    def p_field_items(self, p):
        p[0] = self.provider.new_record(p[1])

    @_rule('field : HOLDER')
    def p_field_param(self, p):
        p[0] = self.provider.new_param()

    @_rule('field : fullname_json')
    def p_field_name(self, p):
        p[0] = p[1]
