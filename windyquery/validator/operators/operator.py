from .comma import Comma
from .equal import Equal
from .paren import Paren
from .negate import Negate


class Operator(Comma, Equal, Paren, Negate):
    reserved = {**Comma.reserved,
                **Equal.reserved,
                **Paren.reserved,
                **Negate.reserved,
                'IN': 'IN',
                'IS': 'IS',
                'LIKE': 'LIKE',
                'ILIKE': 'ILIKE',
                'DISTINCT': 'DISTINCT',
                'FROM': 'FROM'}
    tokens = Comma.tokens + \
        Equal.tokens + \
        Paren.tokens + \
        Negate.tokens + \
        ('IN', 'IS', 'LIKE', 'ILIKE', 'DISTINCT', 'FROM',
         'LE', 'LS', 'GE', 'GT', 'NE', 'NN', 'DPIPE',
         'PLUS', 'MULTI', 'DIVIDE', 'MODULAR',)

    # Tokens
    t_IN = r'IN'
    t_IS = r'IS'
    t_LIKE = r'LIKE'
    t_ILIKE = r'ILIKE'
    t_DISTINCT = r'DISTINCT'
    t_FROM = r'FROM'
    t_LE = r'<='
    t_LS = r'<'
    t_GE = r'>='
    t_GT = r'>'
    t_NE = r'\!='
    t_NN = r'<>'
    t_DPIPE = r'\|\|'
    t_PLUS = r'\+'
    t_MULTI = r'\*'
    t_DIVIDE = r'/'
    t_MODULAR = r'%'

    precedence = Comma.precedence + \
        Equal.precedence + \
        Negate.precedence + (
            ('left', 'IN'),
            ('left', 'IS'),
            ('left', 'LIKE'),
            ('left', 'ILIKE'),
            ('left', 'LE'),
            ('left', 'LS'),
            ('left', 'GE'),
            ('left', 'GT'),
            ('left', 'NE'),
            ('left', 'NN'),
            ('left', 'DPIPE'),
            ('left', 'DISTINCT'),
            ('left', 'FROM'),
            ('left', 'PLUS'),
            ('left', 'MULTI'),
            ('left', 'DIVIDE'),
            ('left', 'MODULAR')
        )
