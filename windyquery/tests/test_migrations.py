import os
import glob
import asyncio
import time

from windyquery import DB
from .conftest import Config
from windyquery.scripts import Scripts

loop = asyncio.get_event_loop()
s = Scripts()


def test_make_migration():
    s.make_migration('create_my_table', migrations_dir='test_tmp/migrations')
    files = glob.glob(os.path.join(
        'test_tmp', 'migrations', '20[1-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9]_*.py'))
    assert len(files) == 1
    assert 'test_tmp' in files[0]
    assert 'migrations' in files[0]
    assert 'create_my_table' in files[0]
    # cleanup
    os.remove(files[0])
    os.rmdir(os.path.join('test_tmp', 'migrations'))
    os.rmdir('test_tmp')


def test_migrate(db: DB, config: Config):
    s.make_migration('create_test_table', template='test_create table',
                     migrations_dir='test_tmp/migrations')
    files = glob.glob(os.path.join(
        'test_tmp', 'migrations', '20[1-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9]_*.py'))
    assert len(files) == 1
    s.migrate(config.DB_HOST, config.DB_PORT, config.DB_TEST, config.DB_USER, config.DB_PASS,
              migrations_dir='test_tmp/migrations', migrations_table='test_tmp_migrations', loop=loop)
    # wait at least 1 sec for a different migration timestamp
    time.sleep(1)
    s.make_migration('drop_test_table', template='test_drop table',
                     migrations_dir='test_tmp/migrations')
    s.migrate(config.DB_HOST, config.DB_PORT, config.DB_TEST, config.DB_USER, config.DB_PASS,
              migrations_dir='test_tmp/migrations', migrations_table='test_tmp_migrations', loop=loop)
    files = glob.glob(os.path.join(
        'test_tmp', 'migrations', '20[1-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9]_*.py'))
    assert len(files) == 2
    # cleanup
    os.remove(files[0])
    os.remove(files[1])
    os.rmdir(os.path.join('test_tmp', 'migrations'))
    os.rmdir('test_tmp')

    # drop the test migrations table
    async def drop_table():
        await db.schema('TABLE test_tmp_migrations').drop()
    loop.run_until_complete(drop_table())
