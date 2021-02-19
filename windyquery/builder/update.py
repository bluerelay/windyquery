import json
from typing import Any, Dict

from windyquery.utils import process_value
from ._crud_base import CrudBase


class Update(CrudBase):

    def update(self, *items: Any):
        if len(items) == 1 and isinstance(items[0], Dict):
            _sqls = []
            params = []
            for name, val in items[0].items():
                val, p = process_value(val)
                if p is not None:
                    params.append(p)
                _sqls.append(f'{name} = {val}')
            sql = ', '.join(_sqls)
        elif len(items) > 0:
            sql = items[0]
            params = items[1:]
        else:
            raise UserWarning(f'not valid updates: {items}')
        self.collector.update(sql, *params)
        return self

    def from_table(self, name: str):
        self.collector.from_table(name)
        return self

    def build_update(self, data):
        sql = f'UPDATE {data["TABLE"]}'
        _updates = []
        for item in data['UPDATE']:
            _updates.append(item['sql'])
        sql += ' SET ' + ', '.join(_updates)
        # FROM
        if 'FROM_TABLE' in data:
            sql += f' FROM {data["FROM_TABLE"]}'
        # JOIN
        if 'JOIN' in data:
            jns = []
            for jn in data['JOIN']:
                jns.append(jn['sql'])
            if len(jns) > 0:
                sql += ' ' + ' '.join(jns)
        # WHERE
        if 'WHERE' in data:
            ws = []
            for w in data['WHERE']:
                ws.append(w['sql'])
            if len(ws) > 0:
                sql += ' WHERE ' + ' AND '.join(ws)
        return sql, data['_params']
