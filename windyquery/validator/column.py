from ._base import _rule
from .expr import Expr
from .operators.bracket import Bracket


class Column(Expr, Bracket):
    reserved = {
        **Expr.reserved,
        **Bracket.reserved,
        'SMALLINT': 'SMALLINT',
        'INTEGER': 'INTEGER',
        'BIGINT': 'BIGINT',
        'DECIMAL': 'DECIMAL',
        'NUMERIC': 'NUMERIC',
        'REAL': 'REAL',
        'DOUBLE': 'DOUBLE',
        'PRECISION': 'PRECISION',
        'SERIAL': 'SERIAL',
        'BIGSERIAL': 'BIGSERIAL',
        'MONEY': 'MONEY',
        'CHARACTER': 'CHARACTER',
        'VARYING': 'VARYING',
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR',
        'TEXT': 'TEXT',
        'BYTEA': 'BYTEA',
        'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'TIME',
        'WITHOUT': 'WITHOUT',
        'WITH': 'WITH',
        'ZONE': 'ZONE',
        'TIMESTAMPTZ': 'TIMESTAMPTZ',
        'TIMETZ': 'TIMETZ',
        'DATE': 'DATE',
        'INTERVAL': 'INTERVAL',
        'BOOLEAN': 'BOOLEAN',
        'POINT': 'POINT',
        'LINE': 'LINE',
        'LSEG': 'LSEG',
        'BOX': 'BOX',
        'PATH': 'PATH',
        'POLYGON': 'POLYGON',
        'CIRCLE': 'CIRCLE',
        'CIDR': 'CIDR',
        'INET': 'INET',
        'MACADDR': 'MACADDR',
        'BIT': 'BIT',
        'TSVECTOR': 'TSVECTOR',
        'TSQUERY': 'TSQUERY',
        'UUID': 'UUID',
        'XML': 'XML',
        'ARRAY': 'ARRAY',
        'JSON': 'JSON',
        'JSONB': 'JSONB',
        'YEAR': 'YEAR',
        'MONTH': 'MONTH',
        'DAY': 'DAY',
        'HOUR': 'HOUR',
        'MINUTE': 'MINUTE',
        'SECOND': 'SECOND',
        'TO': 'TO',
        'CONSTRAINT': 'CONSTRAINT',
        'CHECK': 'CHECK',
        'DEFAULT': 'DEFAULT',
        'CURRENT_TIMESTAMP': 'CURRENT_TIMESTAMP',
        'NEXTVAL': 'NEXTVAL',
        'UNIQUE': 'UNIQUE',
        'PRIMARY': 'PRIMARY',
        'KEY': 'KEY',
        'AND': 'AND',
        'OR': 'OR',
        'REFERENCES': 'REFERENCES',
        'ON': 'ON',
        'UPDATE': 'UPDATE',
        'DELETE': 'DELETE',
        'SET': 'SET',
        'NO': 'NO',
        'ACTION': 'ACTION',
        'RESTRICT': 'RESTRICT',
        'CASCADE': 'CASCADE',
        'GENERATED': 'GENERATED',
        'AS': 'AS',
        'IDENTITY': 'IDENTITY',
        'ALWAYS': 'ALWAYS',
        'BY': 'BY',
        'START': 'START',
        'INCREMENT': 'INCREMENT',
        'NOW': 'NOW',
    }

    tokens = Expr.tokens + Bracket.tokens + \
        ('SMALLINT', 'INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE', 'PRECISION',
         'SERIAL', 'BIGSERIAL', 'MONEY', 'CHARACTER', 'VARYING', 'VARCHAR', 'CHAR', 'TEXT', 'BYTEA',
         'TIMESTAMP', 'TIME', 'WITHOUT', 'WITH', 'ZONE', 'TIMESTAMPTZ', 'TIMETZ', 'DATE', 'INTERVAL',
         'BOOLEAN', 'POINT', 'LINE', 'LSEG', 'BOX', 'PATH', 'POLYGON', 'CIRCLE', 'CIDR', 'INET',
         'MACADDR', 'BIT', 'TSVECTOR', 'TSQUERY', 'UUID', 'XML', 'ARRAY', 'JSON', 'JSONB', 'YEAR',
         'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'TO', 'CONSTRAINT', 'CHECK', 'DEFAULT',
         'CURRENT_TIMESTAMP', 'NEXTVAL', 'UNIQUE', 'PRIMARY', 'KEY', 'AND', 'OR',
         'REFERENCES', 'ON', 'UPDATE', 'DELETE', 'SET', 'NO', 'ACTION',
         'RESTRICT', 'CASCADE', 'GENERATED', 'AS', 'IDENTITY', 'ALWAYS', 'BY', 'START',
         'INCREMENT', 'NOW',)

    precedence = Expr.precedence + (
        ('left', 'OR'),
        ('left', 'AND'),
    )

    # rules
    _start = 'column'

    @_rule('column : fullname datatype optarray colconstraint')
    def p_column(self, p):
        p[0] = self.provider.new_glue(p[1], p[2], p[3], p[4])

    @_rule('''datatype : SMALLINT
                       | INTEGER
                       | BIGINT
                       | REAL
                       | SERIAL
                       | BIGSERIAL
                       | MONEY
                       | TEXT
                       | BYTEA
                       | TIMESTAMP
                       | TIMESTAMPTZ
                       | TIME
                       | DATE
                       | TIMETZ
                       | INTERVAL
                       | BOOLEAN
                       | POINT
                       | LINE
                       | LSEG
                       | BOX
                       | PATH
                       | POLYGON
                       | CIRCLE
                       | CIDR
                       | INET
                       | MACADDR
                       | TSVECTOR
                       | TSQUERY
                       | UUID
                       | XML
                       | JSON
                       | JSONB''')
    def p_datatype(self, p):
        p[0] = p[1].upper()

    @_rule('datatype : DOUBLE PRECISION')
    def p_datatype_double_precision(self, p):
        p[0] = 'DOUBLE PRECISION'

    @_rule('''datatype : DECIMAL LPAREN NUMBER COMMA NUMBER RPAREN
                       | NUMERIC LPAREN NUMBER COMMA NUMBER RPAREN''')
    def p_datatype_numeric(self, p):
        p[0] = f'{p[1].upper()}({p[3]}, {p[5]})'

    @_rule('''datatype : CHARACTER VARYING LPAREN NUMBER RPAREN
                       | BIT VARYING LPAREN NUMBER RPAREN''')
    def p_datatype_varyingn(self, p):
        p[0] = f'{p[1].upper()} VARYING({p[4]})'

    @_rule('''datatype : VARCHAR LPAREN NUMBER RPAREN
                       | CHARACTER LPAREN NUMBER RPAREN
                       | CHAR LPAREN NUMBER RPAREN
                       | BIT LPAREN NUMBER RPAREN''')
    def p_datatype_sized(self, p):
        p[0] = f'{p[1].upper()}({p[3]})'

    @_rule('''datatype : TIMESTAMP WITHOUT TIME ZONE
                       | TIMESTAMP WITH TIME ZONE
                       | TIME WITHOUT TIME ZONE
                       | TIME WITH TIME ZONE''')
    def p_datatype_timestamp(self, p):
        p[0] = f'{p[1].upper()} {p[2].upper()} TIME ZONE'

    @_rule('''datatype : INTERVAL YEAR
                       | INTERVAL MONTH
                       | INTERVAL DAY
                       | INTERVAL HOUR
                       | INTERVAL MINUTE
                       | INTERVAL SECOND''')
    def p_interval1(self, p):
        p[0] = f'INTERVAL {p[2].upper()}'

    @_rule('''datatype : INTERVAL DAY TO HOUR
                       | INTERVAL DAY TO MINUTE
                       | INTERVAL DAY TO SECOND
                       | INTERVAL HOUR TO MINUTE
                       | INTERVAL HOUR TO SECOND
                       | INTERVAL MINUTE TO SECOND
                       | INTERVAL YEAR TO MONTH''')
    def p_interval2(self, p):
        p[0] = f'INTERVAL {p[2].upper()} {p[3].upper()} {p[4].upper()}'

    @_rule('optarray : empty')
    def p_opt_array_empty(self, p):
        p[0] = None

    @_rule('optarray : ARRAY')
    def p_opt_array_array(self, p):
        p[0] = 'ARRAY'

    @_rule('optarray : ARRAY LBRACKET NUMBER RBRACKET')
    def p_opt_array_array_num(self, p):
        p[0] = f'ARRAY[{p[3]}]'

    @_rule('optarray : dimlist')
    def p_opt_array_dimlist(self, p):
        p[0] = p[1]

    @_rule('dimlist : dim')
    def p_dimlist_dim(self, p):
        p[0] = self.provider.new_glue(p[1]).separator('')

    @_rule('dimlist : dimlist dim')
    def p_dimlist_dimlist_dim(self, p):
        p[0] = p[1].append(p[2])

    @_rule('dim : LBRACKET NUMBER RBRACKET')
    def p_dim_bracket_num(self, p):
        p[0] = f'[{p[2]}]'

    @_rule('dim : LBRACKET RBRACKET')
    def p_dim_bracket(self, p):
        p[0] = '[]'

    @_rule('colconstraint : empty')
    def p_col_constraint_empty(self, p):
        p[0] = None

    @_rule('colconstraint : colconstraint colconstr')
    def p_col_constraint(self, p):
        if p[1]:
            p[0] = p[1].append(p[2])
        else:
            p[0] = self.provider.new_glue(p[2])

    @_rule('colconstr : constraintname colconstraintexpr')
    def p_col_constr(self, p):
        p[0] = self.provider.new_glue(p[1], p[2])

    @_rule('constraintname : empty')
    def p_constraintname_empty(self, p):
        p[0] = None

    @_rule('constraintname : CONSTRAINT fullname')
    def p_constraintname(self, p):
        p[0] = f'CONSTRAINT {p[2]}'

    @_rule('''colconstraintexpr : NULL
                                | UNIQUE''')
    def p_colconstraint_single(self, p):
        p[0] = p[1].upper()

    @_rule('''colconstraintexpr : NOT NULL
                                | PRIMARY KEY''')
    def p_colconstraint_double(self, p):
        p[0] = f'{p[1].upper()} {p[2].upper()}'

    @_rule('colconstraintexpr : REFERENCES fullname LPAREN namelist RPAREN referenceaction')
    def p_colconstraint_references(self, p):
        p[0] = self.provider.new_glue('REFERENCES', p[2], '(', p[4], ')', p[6])

    @_rule('colconstraintexpr : CHECK LPAREN checkexpr RPAREN')
    def p_colconstraint_check(self, p):
        p[0] = f'CHECK({p[3]})'

    @_rule('colconstraintexpr : datadefault')
    def p_colconstraint_default(self, p):
        p[0] = p[1]

    @_rule('''datadefault : DEFAULT TEXTVAL
                          | DEFAULT NUMBER
                          | DEFAULT CURRENT_TIMESTAMP''')
    def p_datadefault_const(self, p):
        p[0] = f'DEFAULT {p[2]}'

    @_rule('datadefault : DEFAULT NOW LPAREN RPAREN')
    def p_datadefault_now(self, p):
        p[0] = 'DEFAULT NOW()'

    @_rule('datadefault : DEFAULT NEXTVAL LPAREN TEXTVAL RPAREN')
    def p_datadefault_nextval(self, p):
        p[0] = f'DEFAULT nextval({p[4]})'

    @_rule('''datadefault : DEFAULT TRUE
                          | DEFAULT FALSE''')
    def p_datadefault_bool(self, p):
        p[0] = f'DEFAULT {p[2]}'

    @_rule('colconstraintexpr : GENERATED generatedoption AS IDENTITY sequenceoption')
    def p_colconstraint_generated(self, p):
        p[0] = self.provider.new_glue('GENERATED', p[2], 'AS IDENTITY', p[5])

    @_rule('''checkexpr : checkexpr AND checkexpr
                        | checkexpr OR checkexpr''')
    def p_checkexpr_and(self, p):
        p[0] = self.provider.new_glue(p[1], p[2].upper(), p[3])

    @_rule('checkexpr : LPAREN checkexpr RPAREN')
    def p_checkexpr_group(self, p):
        p[0] = self.provider.new_parentheses(p[2])

    @_rule('checkexpr : expr')
    def p_checkexpr_op(self, p):
        p[0] = p[1]

    @_rule('referenceaction : empty')
    def p_referenceaction_empty(self, p):
        p[0] = None

    @_rule('referenceaction : ON parentkeychange childkeyaction')
    def p_referenceaction(self, p):
        p[0] = self.provider.new_glue(p[1], p[2], p[3])

    @_rule('''parentkeychange : UPDATE
                              | DELETE''')
    def p_parentkeychangereferenceaction(self, p):
        p[0] = p[1].upper()

    @_rule('''childkeyaction : SET NULL
                             | SET DEFAULT
                             | NO ACTION''')
    def p_childkeyaction_double(self, p):
        p[0] = p[1].upper() + ' ' + p[2].upper()

    @_rule('''childkeyaction : RESTRICT
                             | CASCADE''')
    def p_childkeyaction_single(self, p):
        p[0] = p[1].upper()

    @_rule('generatedoption : ALWAYS')
    def p_generatedoption_always(self, p):
        p[0] = p[1].upper()

    @_rule('generatedoption : BY DEFAULT')
    def p_generatedoption_by_default(self, p):
        p[0] = p[1].upper() + ' ' + p[2].upper()

    @_rule('sequenceoption : empty')
    def p_sequenceoption_empty(self, p):
        p[0] = None

    @_rule('sequenceoption : LPAREN startwith incrementby RPAREN')
    def p_sequenceoption(self, p):
        p[0] = self.provider.new_glue('(', p[2], p[3], ')')

    @_rule('startwith : empty')
    def p_startwith_empty(self, p):
        p[0] = None

    @_rule('startwith : START WITH NUMBER')
    def p_startwith(self, p):
        p[0] = f'START WITH {p[3]}'

    @_rule('incrementby : empty')
    def p_incrementby_empty(self, p):
        p[0] = None

    @_rule('incrementby : INCREMENT BY NUMBER')
    def p_incrementby(self, p):
        p[0] = f'INCREMENT BY {p[3]}'

    @_rule('namelist : fullname')
    def p_namelist_name(self, p):
        p[0] = self.provider.new_glue(p[1]).separator(', ')

    @_rule('namelist : namelist COMMA fullname')
    def p_namelist_namelist(self, p):
        p[0] = p[1].append(p[3])
