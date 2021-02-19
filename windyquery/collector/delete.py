from ply.lex import LexToken

from ._base import Base, StartDeleteToken

TOKEN = 'DELETE'


class DeleteToken(LexToken):
    def __init__(self):
        self.type = TOKEN
        self.value = None
        self.lineno = 0
        self.lexpos = 0


class Delete(Base):
    def delete(self):
        self.append(DeleteToken())
        self.add_start(StartDeleteToken())
