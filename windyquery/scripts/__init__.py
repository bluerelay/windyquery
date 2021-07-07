import os
import asyncio
import fire

from .migration import make_migration
from .migration import init_db
from .migration import ensure_migrations_table
from .migration import migrate


class Scripts:
    """the command line interface included with windyquery"""

    default_migrations_dir = ['database', 'migrations']

    def get_migrations_dir(self, migrations_dir):
        if migrations_dir:
            migrations_dir = migrations_dir.split('/')
        else:
            migrations_dir = self.default_migrations_dir
        return migrations_dir

    def example(self):
        """show usage examples"""
        print('---------- make an empty migration file ----------')
        print('wq make_migration --name=create_my_table')
        print('\n---------- make a migration file with templates for "create table" ----------')
        print('wq make_migration --name=create_my_table --template="create table"')
        print('\n---------- make a migration file with all available templates ----------')
        print('wq make_migration --name=create_my_table --template=all')
        print('\n---------- run outstanding migrations ----------')
        print('wq migrate --host=localhost --port=5432 --database=my-db --username=my-name --password=my-pass')
        print('\n---------- run outstanding migrations (DB configured by environment variables) ----------')
        print('DB_HOST=localhost DB_PORT=5432 DB_DATABASE=my-db DB_USERNAME=my-name DB_PASSWORD=my-pass wq migrate')
        print('\n---------- make a migration file in a custom directory ----------')
        print('wq make_migration --name=create_my_table --migrations_dir="my_db_work/migrations"')
        print('\n---------- run outstanding migrations in a custom directory ----------')
        print('wq migrate --host=localhost --port=5432 --database=my-db --username=my-name --password=my-pass --migrations_dir="my_db_work/migrations"')
        print('\n---------- run outstanding migrations and store finished ones in a custom migrations table ----------')
        print('wq migrate --host=localhost --port=5432 --database=my-db --username=my-name --password=my-pass --migrations_table=my_migrations')

    def make_migration(self, name, template=None, migrations_dir=None):
        """generate a database migration file"""
        return make_migration(name, template, self.get_migrations_dir(migrations_dir))

    def migrate(self, host=None, port=None, database=None, username=None, password=None, migrations_dir=None, migrations_table=None, loop=None):
        """run all of the outstanding migrations"""
        host = host if host else os.getenv('DB_HOST')
        port = port if port else os.getenv('DB_PORT')
        database = database if database else os.getenv('DB_DATABASE')
        username = username if username else os.getenv('DB_USERNAME')
        password = password if password else os.getenv('DB_PASSWORD')
        migrations_table = migrations_table if migrations_table else 'migrations'

        async def run():
            db = await init_db(host, port, database, username, password)
            await ensure_migrations_table(db, migrations_table)
            result = await migrate(db, self.get_migrations_dir(migrations_dir), migrations_table)
            await db.stop()
            return result
        if loop:
            return loop.run_until_complete(run())
        else:
            return asyncio.run(run())


def main():
    fire.Fire(Scripts)
