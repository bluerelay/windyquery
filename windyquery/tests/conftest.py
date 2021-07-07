import asyncio
import pytest

from windyquery import DB


class Config:
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_TEST = "windyquery-test"
    DB_USER = "windyquery-test"
    DB_PASS = "windyquery-test"


@pytest.fixture(scope="module")
def config():
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
