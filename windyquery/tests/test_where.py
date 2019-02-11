import asyncio


loop = asyncio.get_event_loop()

def test_single_where(db):
    async def single_where():
        return await db.table('test').select().where("name", 'test').first()
    row = loop.run_until_complete(single_where())
    assert row['name'] == 'test'
    assert row['id'] == 1

def test_josnb_where(db):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->tags').where("data->name", 'Cook lunch').first()
    row = loop.run_until_complete(jsonb_where())
    assert row['id'] == 3
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'

def test_josnb_text_where(db):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->>tags').where("data->name", 'Cook lunch').first()
    row = loop.run_until_complete(jsonb_where())
    assert row['id'] == 3
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'

def test_multi_where(db):
    async def jsonb_where():
        return await db.table('cards').select('id', 'data->>tags').where("data->>name", 'Cook lunch').where('board_id', '=', 3).first()
    row = loop.run_until_complete(jsonb_where())
    assert row['?column?'] == '["Cook", "Kitchen", "Tacos"]'

def test_where_in(db):
    async def where_in():
        return await db.table('boards').select().where("id", 'IN', [1, 3])
    rows = loop.run_until_complete(where_in())
    assert len(rows) == 2

def test_where_list(db):
    async def where_list():
        return await db.table('boards').select().where("id", [1, 3])
    rows = loop.run_until_complete(where_list())
    assert len(rows) == 2
