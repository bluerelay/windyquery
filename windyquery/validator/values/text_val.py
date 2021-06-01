from .._base import _rule
from windyquery.utils import prefix_E


class TextVal:
    reserved = {}
    tokens = ('TEXTVAL',)

    @_rule(r"''|('|E')(?:.(?!(?<!')'(?!')))*.?'")
    def t_TEXTVAL(self, t):
        t.value = prefix_E(t.value)
        return t
