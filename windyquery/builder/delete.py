from ._crud_base import CrudBase


class Delete(CrudBase):

    def delete(self):
        self.collector.delete()
        return self

    def build_delete(self, data):
        # TABLE
        sql = f'DELETE FROM {data["TABLE"]}'
        # WHERE
        if 'WHERE' in data:
            ws = []
            for w in data['WHERE']:
                ws.append(w['sql'])
            if len(ws) > 0:
                sql += ' WHERE ' + ' AND '.join(ws)
        # RETURNING
        if 'RETURNING' in data:
            items = data['RETURNING']
            if len(items) == 0:
                items = ['*']
            sql += ' RETURNING ' + ', '.join(items)
        return sql, data['_params']
