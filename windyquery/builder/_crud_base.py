from typing import Any

from windyquery.collector import Collector
from windyquery.combiner import Combiner


class CrudBase:
    """Base class for crud builder methods"""
    collector: Collector
    combiner: Combiner

    def table(self, name: str):
        self.mode = 'crud'
        self.collector.table(name)
        return self

    def where(self, sql: str, *items: Any):
        self.collector.where(sql, *items)
        return self

    def returning(self, *items: str):
        self.collector.returning(*items)
        return self

    def build_select(self, _):
        return None, None

    def build_update(self, _):
        return None, None

    def build_insert(self, _):
        return None, None

    def build_delete(self, _):
        return None, None

    def build_rrule(self, _):
        return None, None

    def build_crud(self):
        result = self.combiner.run()
        if result['_id'] == 'select':
            sql, args = self.build_select(result)
        elif result['_id'] == 'update':
            sql, args = self.build_update(result)
        elif result['_id'] == 'insert':
            sql, args = self.build_insert(result)
        elif result['_id'] == 'delete':
            sql, args = self.build_delete(result)
        elif result['_id'] == 'error':
            raise UserWarning(result['message'])
        else:
            raise Exception(f"not implemented: {result['_id']!r}")
        # RRULE
        if 'RRULE' in result:
            sql = self.build_rrule(result['RRULE']) + ' ' + sql
        return sql, args
