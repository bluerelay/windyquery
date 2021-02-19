from ._base import _rule


class InsertStmt:
    @_rule('insert_stmt : TABLE')
    def p_insert_table(self, p):
        p.lexer.append('TABLE', p[1], 1)

    @_rule('insert_stmt : INSERT')
    def p_insert_insert(self, p):
        p.lexer.append('INSERT', p[1])

    @_rule('insert_stmt : RETURNING')
    def p_insert_returning(self, p):
        p.lexer.append('RETURNING', p[1], 1)

    @_rule('''insert_stmts : insert_stmt insert_stmts
                           | insert_stmt''')
    def p_insert_stmts(self, p):
        pass
