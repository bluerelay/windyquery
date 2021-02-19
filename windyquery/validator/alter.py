from ._base import _rule
from .constraint import Constraint


class Alter(Constraint):
    reserved = {
        **Constraint.reserved,
        'RENAME': 'RENAME',
        'COLUMN': 'COLUMN',
        'SCHEMA': 'SCHEMA',
        'ADD': 'ADD',
        'DROP': 'DROP',
        'ALTER': 'ALTER',
        'IF': 'IF',
        'EXISTS': 'EXISTS',
        'TYPE': 'TYPE',
        'DATA': 'DATA',
        'RESTART': 'RESTART',
        'STATISTICS': 'STATISTICS',
        'STORAGE': 'STORAGE',
        'PLAIN': 'PLAIN',
        'EXTERNAL': 'EXTERNAL',
        'EXTENDED': 'EXTENDED',
        'MAIN': 'MAIN',
    }

    tokens = Constraint.tokens + \
        ('RENAME', 'COLUMN', 'SCHEMA', 'ADD', 'DROP', 'ALTER', 'IF', 'EXISTS', 'TYPE',
         'DATA', 'DOUBLE_COLON', 'RESTART', 'STATISTICS', 'PLAIN', 'EXTERNAL',
         'EXTENDED', 'MAIN', 'STORAGE',)

    precedence = Constraint.precedence

    # Tokens
    t_DOUBLE_COLON = r'::'

    # rules
    _start = 'alter'

    @_rule('alter : RENAME TO fullname')
    def p_alter_renametable(self, p):
        p[0] = self.provider.new_glue('RENAME TO', p[3])

    @_rule('alter : RENAME optcolumn fullname TO fullname')
    def p_alter_renamecolumn(self, p):
        p[0] = self.provider.new_glue('RENAME', p[2], p[3], 'TO', p[5])

    @_rule('alter : RENAME CONSTRAINT fullname TO fullname')
    def p_alter_renameconstraint(self, p):
        p[0] = self.provider.new_glue('RENAME CONSTRAINT', p[3], 'TO', p[5])

    @_rule('alter : SET SCHEMA fullname')
    def p_alter_setschema(self, p):
        p[0] = self.provider.new_glue('SET SCHEMA', p[3])

    @_rule('alter : actions')
    def p_alter_actions(self, p):
        p[0] = p[1]

    @_rule('actions : action')
    def p_actions_action(self, p):
        p[0] = self.provider.new_glue(p[1]).separator(', ')

    @_rule('actions : actions action')
    def p_actions_actions(self, p):
        p[0] = p[1].append(p[2])

    @_rule('action : ADD constraintname tableconstraint')
    def p_action_addtableconstraint(self, p):
        p[0] = self.provider.new_glue('ADD', p[2], p[3])

    @_rule('action : ADD optcolumn optifnotexists column')
    def p_action_addcolumn(self, p):
        p[0] = self.provider.new_glue('ADD', p[2], p[3], p[4])

    @_rule('action : DROP optcolumn optifexists fullname dropcond')
    def p_action_dropcolumn(self, p):
        p[0] = self.provider.new_glue('DROP', p[2], p[3], p[4], p[5])

    @_rule('action : DROP CONSTRAINT optifexists unitname dropcond')
    def p_action_dropconstraint(self, p):
        p[0] = self.provider.new_glue('DROP CONSTRAINT', p[3], p[4], p[5])

    @_rule('action : ALTER optcolumn unitname colaction')
    def p_action_colaction(self, p):
        p[0] = self.provider.new_glue('ALTER', p[2], p[3], p[4])

    @_rule('colaction : typekey datatype optusing')
    def p_colaction_altertype(self, p):
        p[0] = self.provider.new_glue(p[1], p[2], p[3])

    @_rule('colaction : SET datadefault')
    def p_colaction_alterdefault(self, p):
        p[0] = self.provider.new_glue('SET', p[2])

    @_rule('colaction : DROP DEFAULT')
    def p_colaction_alterdedropfault(self, p):
        p[0] = 'DROP DEFAULT'

    @_rule('colaction : SET NOT NULL')
    def p_colaction_altersetnotnull(self, p):
        p[0] = 'SET NOT NULL'

    @_rule('colaction : DROP NOT NULL')
    def p_colaction_alterdropnotnull(self, p):
        p[0] = 'DROP NOT NULL'

    @_rule('colaction : ADD GENERATED generatedoption AS IDENTITY sequenceoption')
    def p_colaction_addgenerated(self, p):
        p[0] = self.provider.new_glue(
            'ADD GENERATED', p[3], 'AS IDENTITY', p[6])

    @_rule('colaction : generateditems')
    def p_colaction_setgenerated(self, p):
        p[0] = p[1]

    @_rule('colaction : DROP IDENTITY optifexists')
    def p_colaction_dropidentity(self, p):
        p[0] = self.provider.new_glue('DROP IDENTITY', p[3])

    @_rule('colaction : SET STATISTICS NUMBER')
    def p_colaction_setstatistics(self, p):
        p[0] = self.provider.new_glue('SET STATISTICS', p[3])

    @_rule('colaction : SET STATISTICS MINUS NUMBER')
    def p_colaction_unsetstatistics(self, p):
        p[0] = self.provider.new_glue('SET STATISTICS -', p[4]).separator('')

    @_rule('''colaction : SET STORAGE PLAIN
                        | SET STORAGE EXTERNAL
                        | SET STORAGE EXTENDED
                        | SET STORAGE MAIN''')
    def p_colaction_setstorage(self, p):
        p[0] = f'SET STORAGE {p[3]}'

    @_rule('generateditems : generateditem')
    def p_action_setgenerated_generateditems_item(self, p):
        p[0] = self.provider.new_glue(p[1])

    @_rule('generateditems : generateditems generateditem')
    def p_action_setgenerated_generateditems_items(self, p):
        p[0] = p[1].append(p[2])

    @_rule('generateditem : SET GENERATED generatedoption')
    def p_generateditem1(self, p):
        p[0] = self.provider.new_glue('SET GENERATED', p[3])

    @_rule('generateditem : SET startwith incrementby')
    def p_generateditem2(self, p):
        p[0] = self.provider.new_glue('SET', p[2], p[3])

    @_rule('generateditem : RESTART WITH NUMBER')
    def p_generateditem3(self, p):
        p[0] = self.provider.new_glue('RESTART WITH', p[3])

    @_rule('generateditem : RESTART NUMBER')
    def p_generateditem4(self, p):
        p[0] = self.provider.new_glue('RESTART', p[2])

    @_rule('typekey : TYPE')
    def p_typekey_type(self, p):
        p[0] = 'TYPE'

    @_rule('typekey : SET DATA TYPE')
    def p_typekey_setdatatype(self, p):
        p[0] = 'SET DATA TYPE'

    @_rule('optusing : empty')
    def p_optusing_emtpy(self, p):
        p[0] = None

    @_rule('optusing : USING unitname DOUBLE_COLON datatype')
    def p_optusing(self, p):
        conv = self.provider.new_glue(p[2], '::', p[4]).separator('')
        p[0] = f'USING {conv}'

    @_rule('dropcond : empty')
    def p_dropcond_empty(self, p):
        p[0] = None

    @_rule('''dropcond : RESTRICT
                       | CASCADE''')
    def p_dropcond(self, p):
        p[0] = p[1].upper()

    @_rule('optcolumn : empty')
    def p_optcolumn_emtpy(self, p):
        p[0] = None

    @_rule('optcolumn : COLUMN')
    def p_optcolumn(self, p):
        p[0] = 'COLUMN'

    @_rule('optifexists : empty')
    def p_optifexists_emtpy(self, p):
        p[0] = None

    @_rule('optifexists : IF EXISTS')
    def p_optifexists(self, p):
        p[0] = 'IF EXISTS'

    @_rule('optifnotexists : empty')
    def p_optifnotexists_emtpy(self, p):
        p[0] = None

    @_rule('optifnotexists : IF NOT EXISTS')
    def p_optifnotexists(self, p):
        p[0] = 'IF NOT EXISTS'
