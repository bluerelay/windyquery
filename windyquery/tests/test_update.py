import asyncio
import datetime

from windyquery import DB

loop = asyncio.get_event_loop()


def test_simple_update(db: DB):
    async def get_board_id():
        return await db.table('cards').select('board_id').where('id', 9)

    async def update_board_id(test_id):
        return await db.table('cards').where('id', 9).update({'board_id': test_id})
    # update it to 99
    loop.run_until_complete(update_board_id(99))
    rows = loop.run_until_complete(get_board_id())
    assert rows[0]['board_id'] == 99
    # change it back to 3
    loop.run_until_complete(update_board_id(3))
    rows = loop.run_until_complete(get_board_id())
    assert rows[0]['board_id'] == 3


def test_simple_update_jsonb(db: DB):
    async def get_cards_city():
        return await db.table('cards').select('data->address->>city AS city').where('id', 6)

    async def update_cards_city(city):
        return await db.table('cards').where('id', 6).update({'data->address->city': city})
    # change it to 'New York'
    loop.run_until_complete(update_cards_city('New York'))
    rows = loop.run_until_complete(get_cards_city())
    assert rows[0]['city'] == 'New York'
    # change it back to 'Chicago'
    loop.run_until_complete(update_cards_city('Chicago'))
    rows = loop.run_until_complete(get_cards_city())
    assert rows[0]['city'] == 'Chicago'


def test_path_jsonb(db):
    async def get_cards_skill():
        return await db.table('cards').select('data->skill->>java AS java').where('id', 7)

    async def update_cards_skill(level):
        return await db.table('cards').where('id', 7).update({'data->skill->java': level})
    # change it to 'Good'
    loop.run_until_complete(update_cards_skill('Good'))
    rows = loop.run_until_complete(get_cards_skill())
    assert rows[0]['java'] == 'Good'
    # change it back to 'entry level'
    loop.run_until_complete(update_cards_skill('entry level'))
    rows = loop.run_until_complete(get_cards_skill())
    assert rows[0]['java'] == 'entry level'


def test_set_whole_jsonb(db):
    async def get_cards_address():
        return await db.table('cards').select('data->address->>city AS city').where('id', 6)

    async def update_cards_data(data):
        return await db.table('cards').where('id', 6).update({'data': data})
    # change it to {"address": {"city": "New York"}}
    loop.run_until_complete(update_cards_data(
        {'address': {'city': 'New York'}}))
    rows = loop.run_until_complete(get_cards_address())
    assert rows[0]['city'] == 'New York'
    # change it to {"address": {"city": "Chicago"}}
    loop.run_until_complete(update_cards_data(
        {'address': {'city': 'Chicago'}}))
    rows = loop.run_until_complete(get_cards_address())
    assert rows[0]['city'] == 'Chicago'


def test_update_from(db):
    async def get_pass(user_id):
        rows = await db.table('users').select('password').where('id', user_id)
        return rows[0]

    async def update_pass(user_id):
        return await db.table('users').\
            update('password = boards.location').\
            from_table('boards').\
            where('boards.user_id = users.id').\
            where('users.id', user_id)
    # update the users' passwords to their associated table's field - boards.location
    loop.run_until_complete(update_pass(2))
    row = loop.run_until_complete(get_pass(2))
    assert row['password'] == 'dining room'
    loop.run_until_complete(update_pass(3))
    row = loop.run_until_complete(get_pass(3))
    assert row['password'] == 'south door'
    # change the passwords back
    loop.run_until_complete(db.table('users').update(
        {'password': 'mypass2'}).where('id', 2))
    row = loop.run_until_complete(get_pass(2))
    assert row['password'] == 'mypass2'
    loop.run_until_complete(db.table('users').update(
        {'password': 'mypass3'}).where('id', 3))
    row = loop.run_until_complete(get_pass(3))
    assert row['password'] == 'mypass3'


def test_update1(db: DB):
    async def update_fn():
        await db.table('users us').update({
            'email': "this is '3rd user' \nemail"
        }).update('admin = ?, password = cards.data->>name', True).\
            where('us.id', 2).\
            from_table('boards AS bds').\
            join('cards', 'bds.id', '=', 'cards.board_id').\
            where('us.id = bds.user_id and cards.id = ?', 1).\
            where('us.password IS DISTINCT FROM cards.data->>name')
        result = await db.table('users').where('id', 2).select()
        return result[0]
    row = loop.run_until_complete(update_fn())
    assert row['email'] == "this is '3rd user' \nemail"
    assert row['admin'] == True
    assert row['password'] == 'Wash dishes'


def test_update2(db: DB):

    async def insert_fn(test_id):
        result = await db.table('tasks').insert({
            'id': test_id,
            'name': 'test-update2',
        }).returning('id')
        return result

    async def delete_fn(test_id):
        await db.table('tasks').where('id', test_id).delete()
        return await db.table('tasks').select().where('id', test_id)

    async def update_plus(test_id):
        await db.table('tasks').update('id = id + 1').where('id', test_id)
        result = await db.table('tasks').where('id', test_id+1).select()
        return result[0]

    async def update_minus(test_id):
        await db.table('tasks').update('id = id - 1').where('id', test_id)
        result = await db.table('tasks').where('id', test_id-1).select()
        return result[0]

    async def update_multi(test_id):
        await db.table('tasks').update('id = id * 2').where('id', test_id)
        result = await db.table('tasks').where('id', test_id * 2).select()
        return result[0]

    async def update_divide(test_id):
        await db.table('tasks').update('id = id / 2').where('id', test_id)
        result = await db.table('tasks').where('id', int(test_id / 2)).select()
        return result[0]

    async def update_mod(test_id):
        await db.table('tasks').update('id = id % 10').where('id', test_id)
        result = await db.table('tasks').where('id', test_id % 10).select()
        return result[0]

    testID = 8630
    rows = loop.run_until_complete(insert_fn(testID))
    assert len(rows) == 1
    row = loop.run_until_complete(update_plus(testID))
    assert row['id'] == testID + 1
    testID = row['id']
    row = loop.run_until_complete(update_minus(testID))
    assert row['id'] == testID - 1
    testID = row['id']
    row = loop.run_until_complete(update_multi(testID))
    assert row['id'] == testID * 2
    testID = row['id']
    row = loop.run_until_complete(update_divide(testID))
    assert row['id'] == testID / 2
    testID = row['id']
    row = loop.run_until_complete(update_mod(testID))
    assert row['id'] == testID % 10
    rows = loop.run_until_complete(delete_fn(row['id']))
    assert len(rows) == 0


def test_update_newline(db: DB):
    createdAt = datetime.datetime(
        2021, 5, 31, 15, 25, tzinfo=datetime.timezone.utc)

    async def insert_fn():
        results = ['started', 'finished']
        results = await db.table('task_results').insert({
            'task_id': 171,
            'created_at': createdAt,
            'result': '\n'.join(results)
        }).returning('id')
        return results

    async def update_fn(taskID):
        results = ['started2', 'finished2']
        results = await db.table('task_results').update({
            'result': '\n'.join(results)
        }).where('id', taskID)
        return results

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId = rows[0]['id']
    loop.run_until_complete(update_fn(insertedId))
    rows = loop.run_until_complete(
        db.table('task_results').where('id', insertedId).select())
    assert len(rows) == 1
    assert '\nfinished2' in rows[0]['result']
    rows = loop.run_until_complete(
        db.table('task_results').where('id', insertedId).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == insertedId


def test_update_tab(db: DB):
    createdAt = datetime.datetime(
        2021, 5, 31, 20, 5, tzinfo=datetime.timezone.utc)

    async def insert_fn():
        results = ['started', 'finished']
        results = await db.table('task_results').insert({
            'task_id': 172,
            'created_at': createdAt,
            'result': '\t'.join(results)
        }).returning('id')
        return results

    async def update_fn(taskID):
        results = ['started2', 'finished2']
        results = await db.table('task_results').update({
            'result': '\t'.join(results)
        }).where('id', taskID)
        return results

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId = rows[0]['id']
    loop.run_until_complete(update_fn(insertedId))
    rows = loop.run_until_complete(
        db.table('task_results').where('id', insertedId).select())
    assert len(rows) == 1
    assert '\tfinished2' in rows[0]['result']
    rows = loop.run_until_complete(
        db.table('task_results').where('id', insertedId).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == insertedId


def test_update_contain_empty_text(db: DB):
    createdAt = datetime.datetime(
        2021, 5, 31, 22, 11, tzinfo=datetime.timezone.utc)

    async def insert_fn():
        results = ['started', 'finished']
        results = await db.table('task_results').insert({
            'task_id': 173,
            'created_at': createdAt,
            'result': '\t'.join(results)
        }).returning('id')
        return results

    async def update_fn(taskID):
        results = await db.table('task_results').update({
            'created_at': createdAt,
            'result': '',
            'task_id': 137,
        }).where('id', taskID)
        return results

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId = rows[0]['id']
    loop.run_until_complete(update_fn(insertedId))
    rows = loop.run_until_complete(
        db.table('task_results').where('id', insertedId).select())
    assert len(rows) == 1
    assert rows[0]['result'] == ''
    rows = loop.run_until_complete(
        db.table('task_results').where('id', insertedId).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == insertedId


def test_update_returning(db: DB):
    createdAt = datetime.datetime(
        2021, 6, 19, 19, 46, tzinfo=datetime.timezone.utc)

    async def insert_fn():
        results = ['add', 'missing', 'update', 'returning']
        results = await db.table('task_results').insert({
            'task_id': 179,
            'created_at': createdAt,
            'result': '\n'.join(results)
        }).returning('id')
        return results

    async def update_fn(taskID):
        results = await db.table('task_results').update({
            'created_at': createdAt,
            'result': 'update returning',
            'task_id': 129,
        }).where('id', taskID).returning()
        return results

    async def update_fn2(taskID):
        results = await db.table('task_results').update({
            'result': '',
            'task_id': 139,
        }).where('id', taskID).returning('id', 'task_id')
        return results

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId = rows[0]['id']
    rows = loop.run_until_complete(update_fn(insertedId))
    assert len(rows) == 1
    assert rows[0]['result'] == 'update returning'
    assert rows[0]['task_id'] == 129
    rows = loop.run_until_complete(update_fn2(insertedId))
    assert len(rows) == 1
    assert 'result' not in rows[0]
    assert rows[0]['task_id'] == 139
    rows = loop.run_until_complete(
        db.table('task_results').where('id', insertedId).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == insertedId
