from ._base import _rule


class Name:
    reserved = {}
    tokens = ('NAME',)

    # Tokens
    @_rule(r'[a-zA-Z_][a-zA-Z0-9_]*')
    def t_NAME(self, t):
        t.type = self.reserved.get(t.value.upper(), 'NAME')
        return t
