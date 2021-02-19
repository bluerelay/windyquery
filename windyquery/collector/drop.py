from ply.lex import LexToken

from ._base import Base, StartDropToken


TOKEN = 'DROP'


class DropToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class Drop(Base):
    def drop(self, *items: str):
        val = None
        if len(items) > 1:
            raise UserWarning(f'Invalid DROP: {items}')
        elif len(items) == 1:
            val = items[0].upper()
            if val not in ('CASCADE', 'RESTRICT'):
                raise UserWarning(f'Invalid DROP: {items[0]}')
        self.append(DropToken(val))
        self.add_start(StartDropToken())
