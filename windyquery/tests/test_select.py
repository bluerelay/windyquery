import asyncio


loop = asyncio.get_event_loop()

def test_db_test_connected(db):
    assert 'db_test' in db.conn_pools

def test_raw_connection(db):
    async def select_cards():
        async with db.conn_pools['db_test'].acquire() as connection:
            return await connection.fetchrow('SELECT * FROM test')
    row = loop.run_until_complete(select_cards())
    assert row['name'] == 'test'

def test_select_by_builder_toSql(db):
    async def select_by_builder():
        return await db.table('test').select().toSql()
    sql = loop.run_until_complete(select_by_builder())
    assert sql == 'SELECT * FROM "test"'

def test_select_by_builder(db):
    async def select_by_builder():
        return await db.table('test').select().first()
    row = loop.run_until_complete(select_by_builder())
    assert row['name'] == 'test'

def test_select_with_alias(db):
    async def select_with_alias():
        return await db.table('test').select('test.id AS name1', 'test.name').first()
    row = loop.run_until_complete(select_with_alias())
    assert row['name1'] == 1
    assert row['name'] == 'test'

def test_select_with_jsonb(db):
    async def select_with_jsonb():
        return await db.table('cards').select('data->name AS name', 'data->>name AS name_text', 'data->tags AS tags', 'data->finished').where('id', 2).first()
    row = loop.run_until_complete(select_with_jsonb())
    assert row['name'] == '"Wash dishes"'
    assert row['name_text'] == 'Wash dishes'
    assert row['tags'] == '["Clean", "Kitchen"]'
    assert row['?column?'] == 'false'

def test_select_nested_jsonb(db):
    async def select_nested_jsonb():
        return await db.table('cards').select('data->address->>city AS city').where('id', 8).first()
    row = loop.run_until_complete(select_nested_jsonb())
    assert row['city'] == 'Chicago'
