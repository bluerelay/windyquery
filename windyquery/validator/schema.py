from ._base import _rule
from .fullname import Fullname
from .empty import Empty
from .operators.comma import Comma
from .operators.negate import Negate


class Schema(Fullname, Empty, Comma, Negate):
    reserved = {**Fullname.reserved, **Empty.reserved, **Comma.reserved, **Negate.reserved,
                'TABLE': 'TABLE',
                'IF': 'IF',
                'EXISTS': 'EXISTS',
                'ONLY': 'ONLY',
                'ON': 'ON',
                'INDEX': 'INDEX',
                'UNIQUE': 'UNIQUE',
                'CONCURRENTLY': 'CONCURRENTLY',
                'USING': 'USING',
                'BTREE': 'BTREE',
                'HASH': 'HASH',
                'GIST': 'GIST',
                'SPGIST': 'SPGIST',
                'GIN': 'GIN',
                'BRIN': 'BRIN',
                }
    tokens = Fullname.tokens + Empty.tokens + Comma.tokens + Negate.tokens + \
        ('TABLE', 'IF', 'EXISTS', 'ONLY', 'ON', 'INDEX', 'UNIQUE', 'CONCURRENTLY', 'USING',
         'BTREE', 'HASH', 'GIST', 'SPGIST', 'GIN', 'BRIN',)

    precedence = Comma.precedence

    t_DOT = r'\.'

    # rules
    _start = 'schema'

    @_rule('schema : TABLE exists onlys names')
    def p_schema_table(self, p):
        p[0] = self.provider.new_glue('TABLE', p[2], p[3], p[4])

    @_rule('schema : optunique INDEX optconcurrently exists optnames ON onlys fullname optusing')
    def p_schema_index(self, p):
        p[0] = self.provider.new_glue(
            p[1], 'INDEX', p[3], p[4], p[5], 'ON', p[7], p[8], p[9])

    @_rule('schema : optunique INDEX optconcurrently exists optnames')
    def p_schema_index_for_drop(self, p):
        p[0] = self.provider.new_glue(
            p[1], 'INDEX', p[3], p[4], p[5])

    @_rule('optunique : empty')
    def p_optunique_empty(self, p):
        p[0] = None

    @_rule('optunique : UNIQUE')
    def p_optunique(self, p):
        p[0] = 'UNIQUE'

    @_rule('optconcurrently : empty')
    def p_optconcurrently_empty(self, p):
        p[0] = None

    @_rule('optconcurrently : CONCURRENTLY')
    def p_optconcurrently(self, p):
        p[0] = 'CONCURRENTLY'

    @_rule('optnames : empty')
    def p_optname_empty(self, p):
        p[0] = None

    @_rule('optnames : names')
    def p_optname(self, p):
        p[0] = p[1]

    @_rule('optusing : empty')
    def p_optusing_empty(self, p):
        p[0] = None

    @_rule('''optusing : USING BTREE
                       | USING HASH
                       | USING GIST
                       | USING SPGIST
                       | USING GIN
                       | USING BRIN''')
    def p_optusing(self, p):
        p[0] = f'USING {p[2]}'

    @_rule('exists : empty')
    def p_exists_empty(self, p):
        p[0] = None

    @_rule('exists : IF EXISTS')
    def p_exists_exists(self, p):
        p[0] = 'IF EXISTS'

    @_rule('exists : IF NOT EXISTS')
    def p_exists_not_exists(self, p):
        p[0] = 'IF NOT EXISTS'

    @_rule('onlys : empty')
    def p_onlys_empty(self, p):
        p[0] = None

    @_rule('onlys : ONLY')
    def p_onlys_only(self, p):
        p[0] = 'ONLY'

    @_rule('names : fullname')
    def p_names_name(self, p):
        p[0] = self.provider.new_glue(p[1]).separator(', ')

    @_rule('names : names COMMA fullname')
    def p_names_comma_name(self, p):
        p[0] = p[1].append(p[3])
