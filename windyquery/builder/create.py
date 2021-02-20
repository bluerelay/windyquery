from ._schema_base import SchemaBase


class Create(SchemaBase):

    def create(self, *items: str):
        self.collector.create(*items)
        return self

    def build_create(self, data):
        sql = f'CREATE {data["SCHEMA"]}'
        sql += ' (' + ', '.join(data['CREATE']) + ')'
        # WHERE
        if 'WHERE' in data:
            ws = []
            for w in data['WHERE']:
                ws.append(w['sql'])
            if len(ws) > 0:
                sql += ' WHERE ' + ' AND '.join(ws)
        return sql
