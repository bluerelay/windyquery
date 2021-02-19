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
         'LE', 'LS', 'GE', 'GT', 'NE', 'NN',)

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

    precedence = Comma.precedence + \
        Equal.precedence + \
        Negate.precedence
