class Negate:
    reserved = {
        'NOT': 'NOT',
    }
    tokens = ('NOT',)

    # Tokens
    t_NOT = r'NOT'

    precedence = (
        ('right', 'NOT'),
    )
