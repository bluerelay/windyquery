class Bracket:
    reserved = {}
    tokens = ('LBRACKET', 'RBRACKET',)

    # Tokens
    t_LBRACKET = r'\['
    t_RBRACKET = r'\]'
