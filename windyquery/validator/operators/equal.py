class Equal:
    reserved = {}
    tokens = ('EQ',)

    # Tokens
    t_EQ = r'='

    precedence = (
        ('right', 'EQ'),
    )
