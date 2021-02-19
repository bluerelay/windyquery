from windyquery.collector import Collector
from windyquery.combiner import Combiner


class SchemaBase:
    """Base class for schema builder methods"""

    def _reset_schema(self):
        self.schema_collector = Collector()
        self.schema_combiner = Combiner(self.schema_collector)

    def schema(self, s: str):
        self.mode = 'schema'
        self.schema_collector.schema(s)
        return self

    def build_create(self, _):
        return None, None

    def build_drop(self, _):
        return None, None

    def build_alter(self, _):
        return None, None

    def build_schema(self):
        result = self.schema_combiner.run()
        if result['_id'] == 'create':
            sql = self.build_create(result)
        elif result['_id'] == 'drop':
            sql = self.build_drop(result)
        elif result['_id'] == 'alter':
            sql = self.build_alter(result)
        elif result['_id'] == 'error':
            raise UserWarning(result['message'])
        else:
            raise Exception(f"not implemented: {result['_id']!r}")
        return sql
