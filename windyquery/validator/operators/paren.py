class Paren:
    reserved = {}
    tokens = ('LPAREN', 'RPAREN',)

    # Tokens
    t_LPAREN = r'\('
    t_RPAREN = r'\)'
