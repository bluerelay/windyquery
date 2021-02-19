import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_single_where(db: DB):
    rows = loop.run_until_complete(
        db.table('users').select().where('email', 'test@example.com'))
    assert rows[0]['email'] == 'test@example.com'


def test_josnb_where(db: DB):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->tags').where("data->name", 'Cook lunch')
    rows = loop.run_until_complete(jsonb_where())
    row = rows[0]
    assert row['id'] == 2
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'


def test_josnb_text_where(db: DB):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->>tags').where("data->name", 'Cook lunch')
    rows = loop.run_until_complete(jsonb_where())
    row = rows[0]
    assert row['id'] == 2
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'


def test_multi_where(db: DB):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->>tags').where("data->>name", 'Cook lunch').where('board_id', '=', 7)
    rows = loop.run_until_complete(jsonb_where())
    assert rows[0]['?column?'] == '["Cook", "Kitchen", "Tacos"]'


def test_where_in(db: DB):
    rows = loop.run_until_complete(
        db.table('cards').select().where("id", 'IN', [5, 3]))
    assert len(rows) == 2


def test_where_in_implicit(db: DB):
    rows = loop.run_until_complete(
        db.table('cards').select().where("id", [5, 3]))
    assert len(rows) == 2


def test_where_in_by_params(db: DB):
    rows = loop.run_until_complete(
        db.table('cards').select().where("id IN (?, ?)", 5, 3))
    assert len(rows) == 2
