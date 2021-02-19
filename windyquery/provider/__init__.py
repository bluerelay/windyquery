from typing import List

from windyquery.ctx import Ctx
from .param import Param
from .biop import Biop
from .record import Record
from .fieldlist import Fieldlist
from .parentheses import Parentheses
from .glue import Glue


class ValidationError(Exception):
    pass


class Provider:
    ctx: Ctx
    params: List[Param] = []

    def __init__(self, ctx: Ctx):
        self.ctx = ctx
        self.params = []

    def process(self):
        if len(self.ctx.args) != len(self.params):
            raise ValidationError(
                f"Number of params dose not match number of values: {self.params} {self.ctx.args}")
        for pos, i in enumerate(self.params):
            i.set_pos(pos)

    def new_param(self, prepend=False) -> Param:
        i = Param(self.ctx)
        if prepend:
            self.params.insert(0, i)
        else:
            self.params.append(i)
        return i

    def new_parentheses(self, wrapped) -> Parentheses:
        return Parentheses(self.ctx, wrapped)

    def new_record(self, value, kind=None) -> Record:
        return Record(self.ctx, value, kind)

    def new_biop(self, op, l, r) -> Biop:
        return Biop(self.ctx, op, l, r)

    def new_fieldlist(self, *items) -> Fieldlist:
        return Fieldlist(self.ctx, items)

    def new_glue(self, *items) -> Glue:
        return Glue(self.ctx, items)
