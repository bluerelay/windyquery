import asyncio


loop = asyncio.get_event_loop()

def test_order_by(db):
    async def order_by():
        return await db.table('users').select().order_by('id ASC', 'email  ASC', 'password DESC').first()
    row = loop.run_until_complete(order_by())
    assert row['email'] == 'test@example.com'
    assert row['id'] == 1

def test_order_by_with_table(db):
    async def order_by_with_table():
        return await db.table('users').select().order_by('users.id ASC', 'users.email  ASC', 'password DESC').first()
    row = loop.run_until_complete(order_by_with_table())
    assert row['email'] == 'test@example.com'
    assert row['id'] == 1

def test_order_by_with_jsonb(db):
    async def order_by_with_jsonb():
        return await db.table('cards').select('data->>name AS name').order_by('cards.data->name', 'id DESC').first()
    row = loop.run_until_complete(order_by_with_jsonb())
    assert row['name'] == 'Cook lunch'

def test_limit(db):
    async def limit():
        return await db.table('cards').select().limit(3)
    rows = loop.run_until_complete(limit())
    assert len(rows) == 3

def test_limit_str(db):
    async def limit_str():
        return await db.table('cards').select().limit('5')
    rows = loop.run_until_complete(limit_str())
    assert len(rows) == 5
