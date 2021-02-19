from ._base import _rule


class Number:
    reserved = {}
    tokens = ('NUMBER',)

    # Tokens
    @_rule(r'\d+')
    def t_NUMBER(self, t):
        t.value = int(t.value)
        return t
