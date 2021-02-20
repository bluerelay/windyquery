from ._schema_base import SchemaBase


class Alter(SchemaBase):

    def alter(self, *items: str):
        self.collector.alter(*items)
        return self

    def build_alter(self, data):
        # ALTER
        alters = ', '.join(data["ALTER"])
        sql = f'ALTER {data["SCHEMA"]} {alters}'

        return sql
