import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_order_by(db: DB):
    async def order_by():
        return await db.table('users').select().order_by('id ASC', 'email  ASC', 'password DESC')
    rows = loop.run_until_complete(order_by())
    row = rows[0]
    assert row['email'] == 'test@example.com'
    assert row['id'] == 1


def test_order_by_with_table(db: DB):
    async def order_by_with_table():
        return await db.table('users').select().order_by('users.id ASC', 'users.email  ASC', 'password DESC')
    rows = loop.run_until_complete(order_by_with_table())
    row = rows[0]
    assert row['email'] == 'test@example.com'
    assert row['id'] == 1


def test_order_by_with_jsonb(db: DB):
    async def order_by_with_jsonb():
        return await db.table('cards').select('data->>name AS name').order_by('cards.data->name', 'id DESC')
    rows = loop.run_until_complete(order_by_with_jsonb())
    row = rows[0]
    assert row['name'] == 'Cook lunch'


def test_limit(db: DB):
    rows = loop.run_until_complete(db.table('cards').select().limit(3))
    assert len(rows) == 3
