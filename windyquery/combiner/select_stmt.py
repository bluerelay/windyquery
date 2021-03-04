from ._base import _rule


class SelectStmt:
    @_rule('select_stmt : WHERE')
    def p_select_where(self, p):
        p.lexer.append('WHERE', p[1])

    @_rule('select_stmt : ORDER_BY')
    def p_select_order_by(self, p):
        p.lexer.append('ORDER_BY', p[1])

    @_rule('select_stmt : GROUP_BY')
    def p_select_group_by(self, p):
        p.lexer.append('GROUP_BY', p[1])

    @_rule('select_stmt : JOIN')
    def p_select_join(self, p):
        p.lexer.append('JOIN', p[1])

    @_rule('select_stmt : LIMIT')
    def p_select_limit(self, p):
        p.lexer.append('LIMIT', p[1], 1)

    @_rule('select_stmt : OFFSET')
    def p_select_offset(self, p):
        p.lexer.append('OFFSET', p[1], 1)

    @_rule('select_stmt : SELECT')
    def p_select_select(self, p):
        p.lexer.append('SELECT', p[1], 1)

    @_rule('select_stmt : TABLE')
    def p_select_table(self, p):
        p.lexer.append('TABLE', p[1], 1)

    @_rule('select_stmt : RRULE')
    def p_select_rrule(self, p):
        p.lexer.append('RRULE', p[1])

    @_rule('''select_stmts : select_stmt
                           | select_stmt select_stmts''')
    def p_select_stmts(self, p):
        pass
