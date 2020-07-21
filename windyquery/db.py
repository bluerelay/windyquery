import asyncpg

from .connection import Connection
from .listener import Listener


class DB(Connection):
    """DB class for CRUD"""

    def table(self, table_name):
        return self.get_builder().table(table_name)

    def listen(self, channel):
        return Listener(self, channel)
