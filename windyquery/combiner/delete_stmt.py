from ._base import _rule


class DeleteStmt:
    @_rule('delete_stmt : TABLE')
    def p_delete_table(self, p):
        p.lexer.append('TABLE', p[1], 1)

    @_rule('delete_stmt : DELETE')
    def p_delete_delete(self, p):
        p.lexer.append('DELETE', p[1], 1)

    @_rule('delete_stmt : WHERE')
    def p_delete_where(self, p):
        p.lexer.append('WHERE', p[1])

    @_rule('delete_stmt : RETURNING')
    def p_delete_returning(self, p):
        p.lexer.append('RETURNING', p[1], 1)

    @_rule('''delete_stmts : delete_stmt delete_stmts
                           | delete_stmt''')
    def p_delete_stmts(self, p):
        pass
