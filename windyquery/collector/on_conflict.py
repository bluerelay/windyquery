from ply.lex import LexToken

from windyquery.ctx import Ctx
from windyquery.validator import ValidationError
from ._base import Base


TOKEN = 'ON_CONFLICT'


class OnConflictToken(LexToken):
    def __init__(self, value):
        self.type = TOKEN
        self.value = value
        self.lineno = 0
        self.lexpos = 0


class OnConflict(Base):
    def on_conflict(self, *items):
        try:
            ctx = Ctx(self.paramOffset, items[2:])
            target = self.validator.validate_conflict_target(items[0])
            action = self.validator.validate_conflict_action(items[1], ctx)
            self.paramOffset += len(ctx.args)
            params = ctx.args
        except ValidationError as err:
            raise UserWarning(f'invalid ON CONFLICT: {err}') from None

        self.append(OnConflictToken(
            {'target': target, 'action': action, 'params': params}))
