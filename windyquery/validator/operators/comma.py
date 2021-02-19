class Comma:
    reserved = {}
    tokens = ('COMMA',)

    # Tokens
    t_COMMA = r','

    precedence = (
        ('left', 'COMMA'),
    )
