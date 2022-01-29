import asyncio
import asyncpg

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


def test_where_with_uuid(db: DB):
    async def insert_fn():
        results = await db.table('tasks_uuid_pkey').insert({
            'name': 'test'
        }).returning('id')
        return results

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId1 = rows[0]['id']
    assert isinstance(insertedId1, asyncpg.pgproto.pgproto.UUID)

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId2 = rows[0]['id']
    assert isinstance(insertedId2, asyncpg.pgproto.pgproto.UUID)

    rows = loop.run_until_complete(
        db.table('tasks_uuid_pkey').select().where("id", insertedId1))
    assert len(rows) == 1

    rows = loop.run_until_complete(
        db.table('tasks_uuid_pkey').select().where("id = ? OR id = ?", insertedId1, insertedId2))
    assert len(rows) == 2

    rows = loop.run_until_complete(
        db.table('tasks_uuid_pkey').where('id', insertedId1).delete().returning())
    assert len(rows) == 1

    rows = loop.run_until_complete(
        db.table('tasks_uuid_pkey').where('id', insertedId2).delete().returning())
    assert len(rows) == 1
