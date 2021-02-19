from typing import Dict

from ._crud_base import CrudBase


class Insert(CrudBase):

    def insert(self, *items: Dict):
        if len(items) == 0:
            raise UserWarning('inserts cannot be empty')
        columns = list(items[0].keys())
        if len(columns) == 0:
            raise UserWarning('insert cannot be empty dict')
        values = []
        for item in items:
            val = []
            for col in columns:
                val.append(item.get(col, 'DEFAULT'))
            values.append(val)
        self.collector.insert(columns, values)
        return self

    def build_insert(self, data):
        # TABLE
        sql = f'INSERT INTO {data["TABLE"]}'
        # COLUMNS
        columns = data['INSERT'][0]['columns']
        sql += f' {columns} VALUES'
        # INSERT
        inserts = []
        key = data['INSERT'][0]['key']
        for ins in data['INSERT']:
            if ins['key'] != key:
                raise UserWarning(
                    f'different inserts found: {key} and {ins["key"]}')
            inserts.append(ins['values'])
        sql += ' ' + ', '.join(inserts)
        # RETURNING
        if 'RETURNING' in data:
            items = data['RETURNING']
            if len(items) == 0:
                items = ['*']
            sql += ' RETURNING ' + ', '.join(items)
        return sql, data['_params']
