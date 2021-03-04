from ply.lex import LexToken

from windyquery.validator import Validator


class StartSelectToken(LexToken):
    def __init__(self):
        self.type = 'START_SELECT'
        self.value = 'START_SELECT'
        self.lineno = 0
        self.lexpos = 0


class StartUpdateToken(LexToken):
    def __init__(self):
        self.type = 'START_UPDATE'
        self.value = 'START_UPDATE'
        self.lineno = 0
        self.lexpos = 0


class StartInsertToken(LexToken):
    def __init__(self):
        self.type = 'START_INSERT'
        self.value = 'START_INSERT'
        self.lineno = 0
        self.lexpos = 0


class StartDeleteToken(LexToken):
    def __init__(self):
        self.type = 'START_DELETE'
        self.value = 'START_DELETE'
        self.lineno = 0
        self.lexpos = 0


class StartCreateToken(LexToken):
    def __init__(self):
        self.type = 'START_CREATE'
        self.value = 'START_CREATE'
        self.lineno = 0
        self.lexpos = 0


class StartDropToken(LexToken):
    def __init__(self):
        self.type = 'START_DROP'
        self.value = 'START_DROP'
        self.lineno = 0
        self.lexpos = 0


class StartAlterToken(LexToken):
    def __init__(self):
        self.type = 'START_ALTER'
        self.value = 'START_ALTER'
        self.lineno = 0
        self.lexpos = 0


class StartRawToken(LexToken):
    def __init__(self):
        self.type = 'START_RAW'
        self.value = 'START_RAW'
        self.lineno = 0
        self.lexpos = 0


class Base:
    """base class for Markers"""

    def __init__(self):
        self.validator = Validator()
        self.idx = 0
        self.tokens = []
        self.tokenpos = 0
        self.paramOffset = 1
        self.startAdded = False

    def token(self):
        t = None
        if self.idx < len(self.tokens):
            t = self.tokens[self.idx]
            self.idx += 1
        return t

    def append(self, t):
        t.lexpos = self.tokenpos
        self.tokenpos += 1
        self.tokens.append(t)

    def prepend(self, t):
        t.lexpos = self.tokenpos
        self.tokenpos += 1
        self.tokens.insert(0, t)

    def add_start(self, t):
        if not self.startAdded:
            t.lexpos = self.tokenpos
            self.tokenpos += 1
            self.tokens.insert(0, t)
            self.startAdded = True
