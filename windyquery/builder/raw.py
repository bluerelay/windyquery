class Raw:
    """class for raw queries"""

    def raw(self, s: str, *args):
        self.mode = 'raw'
        self.sql = s
        self.args = list(args)
        return self

    def build_raw(self):
        return self.sql, self.args
