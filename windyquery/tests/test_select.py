import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_select_toSql(db: DB):
    sql, _ = db.table('test').select().toSql()
    assert sql == 'SELECT * FROM "test"'


def test_simple_select(db: DB):
    rows = loop.run_until_complete(db.table('test').select())
    assert rows[0]['name'] == 'test'


def test_select_with_alias(db: DB):
    async def select_with_alias():
        return await db.table('test').select('test.id AS name1', 'test.name')
    rows = loop.run_until_complete(select_with_alias())
    row = rows[0]
    assert row['name1'] == 1
    assert row['name'] == 'test'


def test_select_with_jsonb(db: DB):
    async def select_with_jsonb():
        return await db.table('cards').select('data->name AS name', 'data->>name AS name_text', 'data->tags AS tags', 'data->finished').where('id', 1)
    rows = loop.run_until_complete(select_with_jsonb())
    row = rows[0]
    assert row['name'] == '"Wash dishes"'
    assert row['name_text'] == 'Wash dishes'
    assert row['tags'] == '["Clean", "Kitchen"]'
    assert row['?column?'] == 'false'


def test_select_nested_jsonb(db: DB):
    async def select_nested_jsonb():
        return await db.table('cards').select('data->address->>city AS city').where('id', 6)
    rows = loop.run_until_complete(select_nested_jsonb())
    assert rows[0]['city'] == 'Chicago'


def test_select1(db: DB):
    async def select_fn():
        result = await db.table('cards').\
            select('*').\
            where('1 = 1').\
            order_by('id DESC').\
            order_by('board_id ASC', 'cards.data->address->city').\
            group_by('cards.id', 'board_id', 'cards.data')
        return result
    rows = loop.run_until_complete(select_fn())
    assert len(rows) > 2
    assert int(rows[0]['id']) > int(rows[1]['id'])


def test_select2(db: DB):
    async def select_fn():
        result = await db.table('cards').\
            limit(100).\
            select('cards.data->address->>city as b', '   cards.data->address->city c').\
            where('id = ? AND 1 = ?', 6, 1).\
            where('data->address->city', 'NOT IN', ['Chicago1', 'Denvor']).\
            where('data->address->city', 'IS NOT', None).\
            where('data->address->>city', 'LIKE', 'C%').\
            where('data->address->>city', 'ilike', 'c%').\
            where('(1 = 1) and (2 > 3 or 3 > 2)').\
            where('1', '=', 1).\
            where('2', '!=', 3).\
            where('2', '<>', 3).\
            where('1', '<', 2).\
            where('1', '<=', 2).\
            where('2', '>', 1).\
            where('2', '>=', 1).\
            offset(0)
        return result
    rows = loop.run_until_complete(select_fn())
    assert len(rows) > 0
    assert rows[0]['b'] == 'Chicago'
    assert rows[0]['c'] == '"Chicago"'


def test_select3(db: DB):
    async def select_fn():
        result = await db.table('cards').select('data->tags->1 as b').where('id = ? AND 1 = ?', 4, 1)
        return result[0]

    async def select_fn1():
        result = await db.table('cards').select('data->tags->-1 as b').where('id = ? AND 1 = ?', 4, 1)
        return result[0]
    row = loop.run_until_complete(select_fn())
    assert row['b'] == '"Office"'
    row = loop.run_until_complete(select_fn1())
    assert row['b'] == '"Office"'
