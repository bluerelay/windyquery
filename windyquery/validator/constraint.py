from ._base import _rule
from .column import Column


class Constraint(Column):
    reserved = {
        **Column.reserved,
        'FILLFACTOR': 'FILLFACTOR',
        'INCLUDE': 'INCLUDE',
        'USING': 'USING',
        'INDEX': 'INDEX',
        'TABLESPACE': 'TABLESPACE',
        'FOREIGN': 'FOREIGN',
    }

    tokens = Column.tokens + \
        ('FILLFACTOR', 'INCLUDE', 'USING', 'INDEX', 'TABLESPACE',  'FOREIGN',)

    precedence = Column.precedence

    # rules
    _start = 'tableconstraint'

    @_rule('tableconstraint : CHECK LPAREN checkexpr RPAREN')
    def p_tableconstraint_check(self, p):
        p[0] = f'CHECK({p[3]})'

    @_rule('tableconstraint : FOREIGN KEY LPAREN namelist RPAREN REFERENCES fullname LPAREN namelist RPAREN referenceaction')
    def p_tableconstraint_references(self, p):
        p[0] = f'FOREIGN KEY({p[4]}) REFERENCES {p[7]} ({p[9]})'
        if p[11]:
            p[0] += f' {p[11]}'

    @_rule('tableconstraint : UNIQUE LPAREN namelist RPAREN indexparameters')
    def p_tableconstraint_unique(self, p):
        p[0] = f'UNIQUE({p[3]})'
        if p[5]:
            p[0] += f' {p[5]}'

    @_rule('tableconstraint : PRIMARY KEY LPAREN namelist RPAREN indexparameters')
    def p_tableconstraint_primary_key(self, p):
        p[0] = f'PRIMARY KEY({p[4]})'
        if p[6]:
            p[0] += f' {p[6]}'

    @_rule('indexparameters : empty')
    def p_index_parameters_empty(self, p):
        p[0] = None

    @_rule('indexparameters : WITH LPAREN FILLFACTOR EQ NUMBER RPAREN')
    def p_index_parameters_with(self, p):
        p[0] = f'WITH (fillfactor={p[5]})'

    @_rule('indexparameters : INCLUDE LPAREN namelist RPAREN')
    def p_index_parameters_include(self, p):
        p[0] = f'INCLUDE ({p[3]})'

    @_rule('indexparameters : USING INDEX TABLESPACE fullname')
    def p_index_parameters_using(self, p):
        p[0] = f'USING INDEX TABLESPACE {p[4]}'
