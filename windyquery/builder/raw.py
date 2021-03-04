class Raw:
    """class for raw queries"""

    def raw(self, s: str, *args):
        self.mode = 'raw'
        self.collector.raw(s, *args)
        return self

    def build_raw(self):
        result = self.combiner.run()
        if result['_id'] == 'error':
            raise UserWarning(result['message'])
        if 'RRULE' in result:
            sql = self.build_rrule(
                result['RRULE']) + ' ' + result['RAW']['sql']
        else:
            sql = result['RAW']['sql']
        return sql, result['_params']
