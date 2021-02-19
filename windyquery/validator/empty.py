from ._base import _rule


class Empty:
    reserved = {}
    tokens = ()

    @_rule('empty :')
    def p_empty(self, p):
        pass
