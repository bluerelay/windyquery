class Minus:
    reserved = {}
    tokens = ('MINUS',)

    # Tokens
    t_MINUS = r'-'

    precedence = (
        ('left', 'MINUS'),
    )
