import ply.yacc as yacc
import ply.lex as lex
from asyncpg import utils

from windyquery.ctx import Ctx
from windyquery.provider import Provider


def _rule(r):
    def decorate(func):
        func.__doc__ = r
        return func
    return decorate


class ValidationError(Exception):
    pass


class Base:
    provider: Provider
    reserved = {}
    tokens = ()

    # Ignored characters
    t_ignore = " \t\n"

    def t_error(self, t):
        raise ValidationError(f"Illegal character {t.value[0]!r}")

    # rules
    _start = ''

    def p_error(self, p):
        if p:
            val = p.value
        else:
            val = 'Unknown'
        raise ValidationError(f"error at {val!r}")

    def __init__(self):
        self._lexer = lex.lex(module=self, optimize=1)
        self._parser = yacc.yacc(module=self, start=self._start, debug=False)

    def parse(self, s: str, ctx: Ctx):
        self.provider = Provider(ctx)
        l = self._lexer.clone()
        l.input(s)
        root = self._parser.parse(lexer=l)
        self.provider.process()
        sql = str(root)
        ctx.param_offset += len(ctx.args)
        return sql

    def sanitize_identifier(self, item):
        # do not escape information_schema as identifier
        if item == 'information_schema':
            return item
        elif item == 'EXCLUDED':
            return item
        else:
            return utils._quote_ident(item)

    def sanitize_literal(self, item):
        return utils._quote_literal(item)
