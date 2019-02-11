import asyncio
import pytest

from windyquery import DB, Schema

@pytest.fixture(scope="module")
def config():
    class Config:
        DB_HOST = "localhost",
        DB_PORT = "5432",
        DB_TEST = "db_test",
        DB_USER = "tester name",
        DB_PASS = "tester password"
    yield Config

@pytest.fixture(scope="module")
def db(config):
    app_db = DB()
    async def init_db():
        return await app_db.connect('db_test', {
            'host': config.DB_HOST,
            'port': config.DB_PORT,
            'database': config.DB_TEST,
            'username': config.DB_USER,
            'password': config.DB_PASS
        }, default=True)
    asyncio.get_event_loop().run_until_complete(init_db())
    yield app_db
    asyncio.get_event_loop().run_until_complete(app_db.stop())

@pytest.fixture(scope="module")
def schema(config):
    app_schema = Schema()
    async def init_schema():
        return await app_schema.connect('db_test', {
            'host': config.DB_HOST,
            'port': config.DB_PORT,
            'database': config.DB_TEST,
            'username': config.DB_USER,
            'password': config.DB_PASS
        }, default=True)
    asyncio.get_event_loop().run_until_complete(init_schema())
    yield app_schema
    asyncio.get_event_loop().run_until_complete(app_schema.stop())

@pytest.fixture(scope="module")
def model(config):
    from windyquery.model import Event
    model_db = DB()
    conn_coro = model_db.connect('db_test', {
        'host': config.DB_HOST,
        'port': config.DB_PORT,
        'database': config.DB_TEST,
        'username': config.DB_USER,
        'password': config.DB_PASS
    }, default=True)
    asyncio.get_event_loop().run_until_complete(conn_coro)
    Event.db.on_next(model_db)
    yield model_db
    asyncio.get_event_loop().run_until_complete(model_db.stop())