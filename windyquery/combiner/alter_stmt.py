from ._base import _rule


class AlterStmt:
    @_rule('alter_stmt : SCHEMA')
    def p_alter_schema(self, p):
        p.lexer.append('SCHEMA', p[1], 1)

    @_rule('alter_stmt : ALTER')
    def p_alter_alter(self, p):
        p.lexer.append('ALTER', p[1], 1)

    @_rule('''alter_stmts : alter_stmt alter_stmts
                          | alter_stmt''')
    def p_alter_stmts(self, p):
        pass
