import ply.yacc as yacc


def _rule(r):
    def decorate(func):
        func.__doc__ = r
        return func
    return decorate


class Base:
    from windyquery.collector import tokens

    @_rule('''sql : START_SELECT select_stmts
                  | START_UPDATE update_stmts
                  | START_INSERT insert_stmts
                  | START_DELETE delete_stmts
                  | START_CREATE create_stmts
                  | START_DROP drop_stmts
                  | START_ALTER alter_stmts
                  | START_RAW raw_stmts''')
    def p_sql(self, p):
        if p[1] == 'START_SELECT':
            p.lexer.set_id('select')
            p.lexer.required('SELECT', 'TABLE')
        elif p[1] == 'START_UPDATE':
            p.lexer.set_id('update')
            p.lexer.required('UPDATE', 'TABLE')
        elif p[1] == 'START_INSERT':
            p.lexer.set_id('insert')
            p.lexer.required('INSERT', 'TABLE')
        elif p[1] == 'START_DELETE':
            p.lexer.set_id('delete')
            p.lexer.required('DELETE', 'TABLE')
        elif p[1] == 'START_CREATE':
            p.lexer.set_id('create')
            p.lexer.required('SCHEMA', 'CREATE')
        elif p[1] == 'START_DROP':
            p.lexer.set_id('drop')
            p.lexer.required('SCHEMA', 'DROP')
        elif p[1] == 'START_ALTER':
            p.lexer.set_id('alter')
            p.lexer.required('SCHEMA', 'ALTER')
        elif p[1] == 'START_RAW':
            p.lexer.set_id('raw')
            p.lexer.required('RAW')
        else:
            raise UserWarning(f'not implemented: {p[1]}')

    def p_error(self, p):
        if p:
            msg = f"invalid using {p.type!r} at pos {p.lexpos!r}"
            p.lexer.result = {'_id': 'error', 'message': msg}
        else:
            raise UserWarning('sql is not complete')

    def __init__(self):
        self._parser = yacc.yacc(module=self, start='sql', debug=False)

    def parse(self, combiner):
        self._parser.parse(lexer=combiner)
