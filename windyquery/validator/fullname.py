from ._base import Base, _rule
from .name import Name
from .empty import Empty
from .operators.dot import Dot


class Fullname(Base, Name, Empty, Dot):
    reserved = {**Base.reserved, **Name.reserved,
                **Empty.reserved, **Dot.reserved}
    tokens = Base.tokens + Name.tokens + \
        Empty.tokens + Dot.tokens + ('QUOTED_NAME', 'STAR',)

    # Tokens
    t_QUOTED_NAME = r'"(?:.(?!"))*.?"'
    t_STAR = r'\*'

    # rules
    _start = 'fullname'

    @_rule('fullname : unitname dotname')
    def p_fullname(self, p):
        p[0] = self.provider.new_glue(p[1], p[2]).separator('')

    @_rule('unitname : NAME')
    def p_unitname_name(self, p):
        p[0] = self.sanitize_identifier(p[1])

    @_rule('unitname : QUOTED_NAME')
    def p_unitname_quoted_name(self, p):
        p[0] = p[1]

    @_rule('dotname : DOT unitname dotname')
    def p_dotname_dot(self, p):
        p[0] = self.provider.new_glue('.', p[2], p[3]).separator('')

    @_rule('dotname : DOT STAR')
    def p_dotname_star(self, p):
        p[0] = f'.*'

    @_rule('dotname : empty')
    def p_dotname_empty(self, p):
        p[0] = None
