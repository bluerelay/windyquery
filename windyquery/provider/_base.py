from windyquery.ctx import Ctx


PARAM = 'PARAM'
JSONB = 'JSONB'
FIELDLIST = 'FIELDLIST'


class Base:
    def __init__(self, ctx: Ctx):
        self.kind = None
        self.ctx = ctx

    def __str__(self):
        return ''
