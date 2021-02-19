from ._schema_base import SchemaBase


class Create(SchemaBase):

    def create(self, *items: str):
        self.schema_collector.create(*items)
        return self

    def build_create(self, data):
        sql = f'CREATE {data["SCHEMA"]}'
        sql += ' (' + ', '.join(data['CREATE']) + ')'
        return sql
