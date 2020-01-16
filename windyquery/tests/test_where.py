import asyncio


loop = asyncio.get_event_loop()

def test_single_where(db):
    row = loop.run_until_complete(db.table('users').select().where('email', 'test@example.com').first())
    assert row['email'] == 'test@example.com'
 

def test_josnb_where(db):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->tags').where("data->name", 'Cook lunch').first()
    row = loop.run_until_complete(jsonb_where())
    assert row['id'] == 2
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'


def test_josnb_text_where(db):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->>tags').where("data->name", 'Cook lunch').first()
    row = loop.run_until_complete(jsonb_where())
    assert row['id'] == 2
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'


def test_multi_where(db):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->>tags').where("data->>name", 'Cook lunch').where('board_id', '=', 7).first()
    row = loop.run_until_complete(jsonb_where())
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'


def test_where_in(db):
    rows = loop.run_until_complete(db.table('cards').select().where("id", 'IN', [5, 3]))
    assert len(rows) == 2


def test_where_in_implicit(db):
    rows = loop.run_until_complete(db.table('cards').select().where("id", [5, 3]))
    assert len(rows) == 2
