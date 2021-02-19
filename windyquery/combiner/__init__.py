from typing import Any, MutableMapping

from ._base import Base
from .select_stmt import SelectStmt
from .update_stmt import UpdateStmt
from .insert_stmt import InsertStmt
from .delete_stmt import DeleteStmt
from .create_stmt import CreateStmt
from .drop_stmt import DropStmt
from .alter_stmt import AlterStmt


class RuleParser(Base, SelectStmt, UpdateStmt, InsertStmt, DeleteStmt, CreateStmt, DropStmt, AlterStmt):
    pass


_parser = RuleParser()


class Combiner:
    """combine tokens"""
    result: MutableMapping[str, Any]

    def __init__(self, collector):
        self.collector = collector
        self.result = {'_id': '', '_params': []}
        self.occurred = {}

    def set_id(self, _id: str):
        self.result['_id'] = _id

    def required(self, *items: str):
        for item in items:
            if item not in self.occurred:
                raise UserWarning(f'{item} is required')

    def append(self, typ, val, limit=None):
        if limit == 1:
            if typ in self.result:
                raise UserWarning(
                    f'cannot have more than {limit}: {typ} - {val}')
            self.result[typ] = val
        else:
            if typ in self.result:
                self.result[typ].append(val)
            else:
                self.result[typ] = [val]
            if limit is not None and len(self.result[typ]) > limit:
                raise UserWarning(
                    f'cannot have more than {limit}: {typ} - {val}')
        self.occurred[typ] = self.occurred.get(typ, 0) + 1
        if isinstance(val, dict) and 'params' in val:
            self.result['_params'] += val['params']
        return self.result

    def prepend(self, typ, val, limit=None):
        if limit == 1:
            if typ in self.result:
                raise UserWarning(
                    f'cannot have more than {limit}: {typ} - {val}')
            self.result[typ] = val
        else:
            if typ in self.result:
                self.result[typ].insert(0, val)
            else:
                self.result[typ] = [val]
            if limit is not None and len(self.result[typ]) > limit:
                raise UserWarning(
                    f'cannot have more than {limit}: {typ} - {val}')
        self.occurred[typ] = self.occurred.get(typ, 0) + 1
        if isinstance(val, dict) and 'params' in val:
            self.result['_params'] = val['params'] + self.result['_params']
        return self.result

    def token(self):
        return self.collector.token()

    def run(self):
        _parser.parse(self)
        return self.result
