import asyncio
import json
from json.decoder import JSONDecodeError
import re
from rx.subject import BehaviorSubject

from .db import DB
from .builder import Builder


class Event:
    """hold events"""

    db = BehaviorSubject(None)


class Column:
    """represents a DB column"""

    def __init__(self, name, type, ordinal):
        self.name = name
        self.type = type
        self.ordinal = ordinal


class ModelMeta(type):
    """add table name and columns to Model"""

    def __init__(cls, name, bases, attr_dict):
        super().__init__(name, bases, attr_dict)
        if cls.__name__ == 'Model':
            return

        initialized = False

        def setup(db):
            nonlocal cls
            nonlocal initialized
            cls.db = db
            if initialized or db is None:
                return
            if not hasattr(cls, 'table'):
                s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', cls.__name__)
                s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
                cls.table = s2 if s2.endswith('s') else s2+'s'
            if hasattr(cls, 'columns'):
                cols = cls.columns
            else:
                cols = asyncio.get_event_loop().run_until_complete(cls.db.raw(
                    "SELECT column_name, ordinal_position, data_type FROM information_schema.columns WHERE table_name = '{}'".format(cls.table)))
            cls.columns = tuple(Column(
                col['column_name'], col['data_type'], col['ordinal_position']) for col in cols)
            if not hasattr(cls, 'id'):
                row = asyncio.get_event_loop().run_until_complete(cls.db.raw(
                    "SELECT column_name FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.table_name = '{}' AND t.constraint_type = 'PRIMARY KEY'".format(cls.table)).first())
                if row and 'column_name' in row:
                    cls.id = row['column_name']
                else:
                    cls.id = None
            initialized = True
        Event.db.subscribe(setup)


class Model(metaclass=ModelMeta):
    """represents a DB record with querry functions"""

    connection = None

    def __getitem__(self, k):
        return getattr(self, k)

    def __setitem__(self, k, v):
        setattr(self, k, v)

    def __delitem__(self, k):
        delattr(self, k)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__.items())

    def __contains__(self, item):
        return item in self.__dict__

    @classmethod
    def set(cls, instance, record):
        record = dict(record)
        for col in cls.columns:
            if col.name in record:
                val = record[col.name]
                if col.type == 'jsonb' and isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except JSONDecodeError:
                        pass
                setattr(instance, col.name, val)
        return instance

    def __init__(self, *records, **record):
        cls = type(self)
        if cls.id is not None:
            setattr(self, cls.id, None)
        for r in records:
            cls.set(self, r)
        cls.set(self, record)

    @classmethod
    def builder(cls):
        if cls.connection is None:
            pool = cls.db.conn_pools[cls.db.default]
        else:
            pool = cls.db.conn_pools[cls.connection]
        return ModelBuilder(pool, cls).table(cls.table).select()

    @classmethod
    def all(cls):
        """Retrieve all instances"""

        return cls.builder()

    @classmethod
    def find(cls, id):
        """Retrieve a model instance by id"""

        builder = cls.builder().where(cls.id, id)
        if not isinstance(id, list):
            builder.first()
        return builder

    @classmethod
    def where(cls, *args):
        """Retrieve a model instance by where"""

        return cls.builder().where(*args)

    async def save(self):
        """Save to DB"""

        record = self.__dict__.copy()
        for col in type(self).columns:
            if col.type == 'jsonb' and col.name in record:
                record[col.name] = json.dumps(record[col.name])

        builder = type(self).builder()
        id_name = type(self).id
        id = getattr(self, id_name) if id_name else None
        if id:
            await builder.where(id_name, id).update(record)
        else:
            if id_name:
                del record[id_name]
            model = await builder.insert(record).returning().first()
            if model:
                type(self).set(self, model)
        return self

    async def delete(self):
        """Delete from DB"""

        builder = type(self).builder()
        id_name = type(self).id
        id = getattr(self, id_name) if id_name else None
        if id:
            await builder.where(id_name, id).delete()


class ModelBuilder(Builder):
    """wrap the Builder class to return a Model instance after exec"""

    def __init__(self, pool, model_cls):
        super().__init__(pool)
        self.model_cls = model_cls
        self._new_if_not_found = False

    def reset(self):
        super().reset()
        self._new_if_not_found = False

    async def exec(self):
        if self.composer is None:
            raise UserWarning("SQL Builder is not complete")
        rows = await super().exec()
        if isinstance(rows, list):
            result = [self.model_cls.set(self.model_cls(), row)
                      for row in rows]
        else:
            if rows:
                result = self.model_cls.set(self.model_cls(), rows)
            else:
                result = self.model_cls() if self._new_if_not_found else None
        self.reset()
        return result

    def first_or_new(self):
        self._first = True
        self._new_if_not_found = True
        return self
