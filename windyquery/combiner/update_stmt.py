from ._base import _rule


class UpdateStmt:
    @_rule('update_stmt : TABLE')
    def p_update_table(self, p):
        p.lexer.append('TABLE', p[1], 1)

    @_rule('update_stmt : UPDATE')
    def p_update_update(self, p):
        p.lexer.append('UPDATE', p[1])

    @_rule('update_stmt : FROM_TABLE')
    def p_update_from(self, p):
        p.lexer.append('FROM_TABLE', p[1], 1)

    @_rule('update_stmt : JOIN')
    def p_update_join(self, p):
        p.lexer.append('JOIN', p[1])

    @_rule('update_stmt : WHERE')
    def p_update_where(self, p):
        p.lexer.append('WHERE', p[1])

    @_rule('update_stmt : RRULE')
    def p_update_rrule(self, p):
        p.lexer.append('RRULE', p[1])

    @_rule('update_stmt : RETURNING')
    def p_update_returning(self, p):
        p.lexer.append('RETURNING', p[1], 1)

    @_rule('''update_stmts : update_stmt update_stmts
                           | update_stmt''')
    def p_update_stmts(self, p):
        pass
