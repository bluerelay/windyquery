from typing import Any

from ._crud_base import CrudBase


class Select(CrudBase):

    def select(self, *items: str):
        self.collector.select(*items)
        return self

    def limit(self, size: int):
        self.collector.limit(size)
        return self

    def offset(self, size: int):
        self.collector.offset(size)
        return self

    def order_by(self, *items: str):
        self.collector.order_by(*items)
        return self

    def group_by(self, *items: str):
        self.collector.group_by(*items)
        return self

    def join(self, tbl: str, *cond: Any):
        self.collector.join(tbl, *cond)
        return self

    def build_select(self, data):
        # SELECT
        sql = 'SELECT '
        items = data['SELECT']
        if len(items) == 0:
            sql += '*'
        else:
            sql += ', '.join(items)
        # TABLE
        sql += ' FROM ' + data['TABLE']
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
        # GROUP BY
        if 'GROUP_BY' in data:
            gps = []
            for gp in data['GROUP_BY']:
                gps += gp
            if len(gps) > 0:
                sql += ' GROUP BY ' + ', '.join(gps)
        # ORDER BY
        if 'ORDER_BY' in data:
            ods = []
            for od in data['ORDER_BY']:
                ods += od
            if len(ods) > 0:
                sql += ' ORDER BY ' + ', '.join(ods)
        # LIMIT
        if 'LIMIT' in data and data['LIMIT']:
            lmt = data['LIMIT']
            sql += ' ' + lmt['sql']
        # OFFSET
        if 'OFFSET' in data and data['OFFSET']:
            ofs = data['OFFSET']
            sql += ' ' + ofs['sql']
        return sql, data['_params']
