from ._base import _rule
from .update import Update


class ConflictAction(Update):
    reserved = {**Update.reserved, 'DO': 'DO',
                'NOTHING': 'NOTHING', 'UPDATE': 'UPDATE', 'SET': 'SET'}
    tokens = Update.tokens + ('DO', 'NOTHING', 'UPDATE', 'SET',)

    # rules
    _start = 'conflictaction'

    @_rule('conflictaction : DO NOTHING')
    def p_conflictaction_do_nothing(self, p):
        p[0] = 'DO NOTHING'

    @_rule('conflictaction : DO UPDATE SET updates')
    def p_conflictaction_do_update(self, p):
        p[0] = self.provider.new_glue('DO UPDATE SET', p[4])
