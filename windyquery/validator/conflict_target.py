from ._base import _rule
from .operators.paren import Paren
from .fullname import Fullname


class ConflictTarget(Fullname, Paren):
    reserved = {
        **Fullname.reserved, **Paren.reserved, 'ON': 'ON', 'CONSTRAINT': 'CONSTRAINT',
    }

    tokens = Fullname.tokens + Paren.tokens + ('ON', 'CONSTRAINT',)

    precedence = Fullname.precedence

    # rules
    _start = 'conflicttarget'

    @_rule('conflicttarget : ON CONSTRAINT fullname')
    def p_conflicttarget_on_constraint(self, p):
        p[0] = f'ON CONSTRAINT {p[3]}'

    @_rule('conflicttarget : LPAREN fullname RPAREN')
    def p_tableconstraint_index_name(self, p):
        p[0] = f'({p[2]})'
