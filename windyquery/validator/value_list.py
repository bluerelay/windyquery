from ._base import Base, _rule
from .number import Number
from .values.text_val import TextVal
from .values.null import NULL
from .values.default import Default
from .values.holder import Holder
from .values.true import TRUE
from .values.false import FALSE
from .operators.comma import Comma


class ValueList(Base, Number, TextVal, NULL, Default, Holder, TRUE, FALSE, Comma):
    reserved = {**Base.reserved, **Number.reserved, **TextVal.reserved,
                **NULL.reserved, **Default.reserved, **Holder.reserved,
                **TRUE.reserved, **FALSE.reserved, **Comma.reserved}
    tokens = Base.tokens + Number.tokens + TextVal.tokens + \
        NULL.tokens + Default.tokens + Holder.tokens + Comma.tokens + \
        TRUE.tokens + FALSE.tokens

    # Tokens

    # rules
    _start = 'values'

    @_rule('''value : DEFAULT
                    | NUMBER
                    | TEXTVAL
                    | TRUE
                    | FALSE
                    | NULL''')
    def p_value_items(self, p):
        p[0] = self.provider.new_record(p[1])

    @_rule('value : HOLDER')
    def p_value_holder(self, p):
        p[0] = self.provider.new_param()

    @_rule('values : value')
    def p_values_value(self, p):
        p[0] = self.provider.new_glue(p[1]).separator(', ')

    @_rule('values : values COMMA value')
    def p_values_comma(self, p):
        p[0] = p[1].append(p[3])
