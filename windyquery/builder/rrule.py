from typing import Dict
from dateutil import rrule
from windyquery.exceptions import RruleNoResults

from ._crud_base import CrudBase


class Rrule(CrudBase):

    def rrule(self, name: str, *items: Dict):
        if len(items) == 0:
            raise UserWarning('rrule cannot be empty')
        columns = list(items[0].keys())
        if len(columns) == 0:
            raise UserWarning('rrule cannot be empty dict')
        if 'rrule' not in columns:
            raise UserWarning('the input dict must contain a "rrule" field')
        rrulepos = columns.index('rrule')
        if 'rrule_slice' in columns:
            slicepos = columns.index('rrule_slice')
        else:
            slicepos = None
        values = []
        for item in items:
            val = []
            for col in columns:
                colVal = item.get(col, 'NULL')
                if col == 'rrule':
                    try:
                        colVal = rrule.rrulestr(colVal)
                    except:
                        raise UserWarning(f'invalid rrule: {colVal}') from None
                val.append(colVal)
            values.append(val)
        self.collector.rrule(name, rrulepos, slicepos, columns, values)
        return self

    def build_rrule(self, items) -> str:
        parsedItems = []
        for item in items:
            name = item['name']
            columns = item['columns']
            values = item['values']
            if not values:
                raise RruleNoResults(
                    f'the rrule for {name} returns no results')
            parsedItem = f'{name} {columns} AS (VALUES {values})'
            parsedItems.append(parsedItem)
        sql = 'WITH ' + ', '.join(parsedItems)
        return sql
