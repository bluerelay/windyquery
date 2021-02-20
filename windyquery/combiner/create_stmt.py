from ._base import _rule


class CreateStmt:
    @_rule('create_stmt : SCHEMA')
    def p_create_schema(self, p):
        p.lexer.append('SCHEMA', p[1], 1)

    @_rule('create_stmt : CREATE')
    def p_create_create(self, p):
        p.lexer.append('CREATE', p[1], 1)

    @_rule('create_stmt : WHERE')
    def p_create_where(self, p):
        p.lexer.append('WHERE', p[1])

    @_rule('''create_stmts : create_stmt create_stmts
                           | create_stmt''')
    def p_create_stmts(self, p):
        pass
