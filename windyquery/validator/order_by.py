from ._base import _rule
from .field import Field


class OrderBy(Field):
    reserved = {**Field.reserved, 'ASC': 'ASC', 'DESC': 'DESC'}
    tokens = Field.tokens + ('ASC', 'DESC',)

    # Tokens
    t_ASC = r'ASC'
    t_DESC = r'DESC'

    # rules
    _start = 'order_by'

    @_rule('order_by : field')
    def p_order_by(self, p):
        p[0] = p[1]

    @_rule('''order_by : field ASC
                       | field DESC''')
    def p_order_by_dir(self, p):
        p[0] = self.provider.new_record(f'{p[1]} {p[2]}')
