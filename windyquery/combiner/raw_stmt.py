from ._base import _rule


class RawStmt:
    @_rule('raw_stmt : RAW')
    def p_raw_raw(self, p):
        p.lexer.append('RAW', p[1], 1)

    @_rule('raw_stmt : RRULE')
    def p_raw_rrule(self, p):
        p.lexer.append('RRULE', p[1])

    @_rule('''raw_stmts : raw_stmt
                        | raw_stmt raw_stmts''')
    def p_raw_stmts(self, p):
        pass
