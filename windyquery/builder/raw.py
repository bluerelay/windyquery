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

        sql = result['RAW']['sql']

        # with clause
        withClauses = []
        # RRULE
        if 'RRULE' in result:
            withClauses.append(self.build_rrule(result['RRULE']))
        # CTE using VALUES list
        if 'WITH_VALUES' in result:
            withClauses.append(self.build_with_values(result['WITH_VALUES']))
        if len(withClauses) > 0:
            sql = 'WITH ' + ', '.join(withClauses) + ' ' + sql

        return sql, result['_params']
