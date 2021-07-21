from typing import Dict

from ._crud_base import CrudBase


class WithValues(CrudBase):

    def with_values(self, name: str, *values: Dict):
        if len(values) == 0:
            raise UserWarning('with_values cannot be empty')
        columns = list(values[0].keys())
        if len(columns) == 0:
            raise UserWarning('with_values cannot be empty dict')
        # faltten the Dict's into List's
        valuesList = []
        for val in values:
            row = []
            for col in columns:
                row.append(val.get(col, 'NULL'))
            valuesList.append(row)
        self.collector.with_values(name, columns, valuesList)
        return self

    def build_with_values(self, items) -> str:
        parsedItems = []
        for item in items:
            name = item['name']
            columns = item['columns']
            values = item['values']
            parsedItem = f'{name} {columns} AS (VALUES {values})'
            parsedItems.append(parsedItem)
        sql = ', '.join(parsedItems)
        return sql
