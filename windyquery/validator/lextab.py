# lextab.py. This file automatically created by PLY (version 3.11). Don't edit!
_tabversion   = '3.10'
_lextokens    = set(('ARROW', 'COMMA', 'DARROW', 'DISTINCT', 'DIVIDE', 'DO', 'DOT', 'DPIPE', 'EQ', 'FALSE', 'FROM', 'GE', 'GT', 'HOLDER', 'ILIKE', 'IN', 'IS', 'LE', 'LIKE', 'LPAREN', 'LS', 'MINUS', 'MODULAR', 'MULTI', 'NAME', 'NE', 'NN', 'NOT', 'NOTHING', 'NULL', 'NUMBER', 'PLUS', 'QUOTED_NAME', 'RPAREN', 'SET', 'STAR', 'TEXTVAL', 'TRUE', 'UPDATE'))
_lexreflags   = 64
_lexliterals  = ''
_lexstateinfo = {'INITIAL': 'inclusive'}
_lexstatere   = {'INITIAL': [('(?P<t_NAME>[a-zA-Z_][a-zA-Z0-9_]*)|(?P<t_NUMBER>\\d+)|(?P<t_TEXTVAL>\'\'|(\'|E\')(?:.(?!(?<!\')\'(?!\')))*.?\')|(?P<t_QUOTED_NAME>"(?:.(?!"))*.?")|(?P<t_DISTINCT>DISTINCT)|(?P<t_ILIKE>ILIKE)|(?P<t_DPIPE>\\|\\|)|(?P<t_FROM>FROM)|(?P<t_LIKE>LIKE)|(?P<t_NULL>NULL)|(?P<t_TRUE>TRUE)|(?P<t_DARROW>->>)|(?P<t_NE>\\!=)|(?P<t_NOT>NOT)|(?P<t_ARROW>->)|(?P<t_DOT>\\.)|(?P<t_GE>>=)|(?P<t_HOLDER>\\?)|(?P<t_IN>IN)|(?P<t_IS>IS)|(?P<t_LE><=)|(?P<t_LPAREN>\\()|(?P<t_MULTI>\\*)|(?P<t_NN><>)|(?P<t_PLUS>\\+)|(?P<t_RPAREN>\\))|(?P<t_STAR>\\*)|(?P<t_COMMA>,)|(?P<t_DIVIDE>/)|(?P<t_EQ>=)|(?P<t_GT>>)|(?P<t_LS><)|(?P<t_MINUS>-)|(?P<t_MODULAR>%)', [None, ('t_NAME', 'NAME'), ('t_NUMBER', 'NUMBER'), ('t_TEXTVAL', 'TEXTVAL'), None, (None, 'QUOTED_NAME'), (None, 'DISTINCT'), (None, 'ILIKE'), (None, 'DPIPE'), (None, 'FROM'), (None, 'LIKE'), (None, 'NULL'), (None, 'TRUE'), (None, 'DARROW'), (None, 'NE'), (None, 'NOT'), (None, 'ARROW'), (None, 'DOT'), (None, 'GE'), (None, 'HOLDER'), (None, 'IN'), (None, 'IS'), (None, 'LE'), (None, 'LPAREN'), (None, 'MULTI'), (None, 'NN'), (None, 'PLUS'), (None, 'RPAREN'), (None, 'STAR'), (None, 'COMMA'), (None, 'DIVIDE'), (None, 'EQ'), (None, 'GT'), (None, 'LS'), (None, 'MINUS'), (None, 'MODULAR')])]}
_lexstateignore = {'INITIAL': ' \t\n'}
_lexstateerrorf = {'INITIAL': 't_error'}
_lexstateeoff = {}
