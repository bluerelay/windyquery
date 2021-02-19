from ._base import _rule


class DropStmt:
    @_rule('drop_stmt : SCHEMA')
    def p_drop_schema(self, p):
        p.lexer.append('SCHEMA', p[1], 1)

    @_rule('drop_stmt : DROP')
    def p_drop_drop(self, p):
        p.lexer.append('DROP', p[1], 1)

    @_rule('''drop_stmts : drop_stmt drop_stmts
                         | drop_stmt''')
    def p_drop_stmts(self, p):
        pass
