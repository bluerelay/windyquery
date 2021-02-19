from ._schema_base import SchemaBase


class Drop(SchemaBase):

    def drop(self, *items: str):
        self.schema_collector.drop(*items)
        return self

    def build_drop(self, data):
        sql = f'DROP {data["SCHEMA"]}'
        if 'DROP' in data and data['DROP'] is not None:
            sql += f' {data["DROP"]}'
        return sql
