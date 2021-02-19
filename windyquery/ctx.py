from typing import List, Any


class Ctx:
    param_offset: int = 1
    args: List[Any] = []

    def __init__(self, param_offset=1, args=[]):
        self.param_offset = param_offset
        self.args = args
