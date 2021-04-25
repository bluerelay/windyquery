import asyncio
import string
import random
import json
import datetime

from windyquery import DB

loop = asyncio.get_event_loop()


def test_insert_user(db: DB):
    async def insert_user(email1, email2):
        return await db.table('users').insert(
            {'email': email1, 'password': 'my precious'},
            {'email': email2, 'password': 'my precious'}
        )

    async def get_user(email):
        return await db.table('users').select().where('email', email)

    email1 = ''.join(random.choice(string.ascii_letters) for i in range(6))
    email2 = ''.join(random.choice(string.ascii_letters) for i in range(6))
    loop.run_until_complete(insert_user(email1, email2))
    rows1 = loop.run_until_complete(get_user(email1))
    rows2 = loop.run_until_complete(get_user(email2))
    loop.run_until_complete(db.table('users').where('email', email1).delete())
    loop.run_until_complete(db.table('users').where('email', email2).delete())
    assert rows1[0]['email'] == email1
    assert rows2[0]['email'] == email2


def test_insert_jsonb(db: DB):
    async def insert_jsonb(test_id):
        return await db.table('cards').insert({
            'id': test_id,
            'board_id': random.randint(1, 100),
            'data': {'name': f'I am {test_id}', 'address': {'city': 'Chicago', 'state': 'IL'}}
        })

    async def get_jsonb(test_id):
        return await db.table('cards').select('data->>name AS name').where('id', test_id)

    test_id = random.randint(10000, 90000)
    loop.run_until_complete(insert_jsonb(test_id))
    rows = loop.run_until_complete(get_jsonb(test_id))
    loop.run_until_complete(db.table('cards').where('id', test_id).delete())
    assert rows[0]['name'] == f'I am {test_id}'


def test_insert1(db: DB):
    async def insert_fn():
        result = await db.table('users').insert({
            'email': 'new_insert@gmail.com',
            'password': 'pwdxxxxx',
            'admin': None
        }, {
            'email': 'new_insert2@gmail.com',
            'password': 'pwdxxxxx2',
            'admin': 'DEFAULT'
        }).insert({
            'email': 'new_insert3@gmail.com',
            'password': 'pwdxxx3',
            'admin': 'DEFAULT'
        }).returning('id', 'email e')
        return result
    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 3
    assert rows[0]['e'] == 'new_insert@gmail.com'
    assert rows[1]['e'] == 'new_insert2@gmail.com'
    assert rows[2]['e'] == 'new_insert3@gmail.com'
    loop.run_until_complete(
        db.table('users').where('id', rows[0]['id']).delete())
    loop.run_until_complete(
        db.table('users').where('id', rows[1]['id']).delete())
    loop.run_until_complete(
        db.table('users').where('id', rows[2]['id']).delete())


def test_insert2(db: DB):
    async def insert_fn():
        result = await db.table('cards').insert({
            'board_id': 2,
            'data': {
                'address': {
                    'city': 'insert Chicago'
                }
            }
        }).returning()
        return result
    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    assert rows[0]['board_id'] == 2
    data = json.loads(rows[0]['data'])
    assert data['address']['city'] == 'insert Chicago'
    cardId = rows[0]['id']
    rows = loop.run_until_complete(
        db.table('cards').where('id', rows[0]['id']).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == cardId


def test_insert3(db: DB):
    insertId = 1000
    email = 'email1000@test.com'

    # first insert a record
    async def insert_fn():
        results = await db.table('users').insert({
            'id': insertId,
            'email': email,
            'password': 'pwd',
            'admin': None
        }).returning('id', 'email')
        return results
    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    assert rows[0]['email'] == email

    # insert the same record with on conflic do nothing
    async def insert_fn2():
        results = await db.table('users').insert({
            'id': insertId,
            'email': f'{email} x2',
            'password': 'pwd',
            'admin': None
        }).on_conflict('(id)', 'DO NOTHING').returning('id', 'email')
        return results
    rows = loop.run_until_complete(insert_fn2())
    assert len(rows) == 0

    # insert the same record with on conflic do update
    async def insert_fn3():
        results = await db.table('users AS u').insert({
            'id': insertId,
            'email': f'{email} x3',
            'password': 'pwd',
            'admin': None
        }).on_conflict('ON CONSTRAINT users_pkey', "DO UPDATE SET email = EXCLUDED.email || ' (formerly ' || u.email || ')'").\
            returning('id', 'email')
        return results
    rows = loop.run_until_complete(insert_fn3())
    assert len(rows) == 1
    assert rows[0]['email'] == f'{email} x3 (formerly {email})'

    rows = loop.run_until_complete(
        db.table('users').where('id', insertId).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == insertId


def test_insert_datetime(db: DB):
    createdAt = datetime.datetime(
        2021, 3, 8, 23, 50, tzinfo=datetime.timezone.utc)

    async def insert_fn():
        results = await db.table('task_results').insert({
            'task_id': 100,
            'created_at': createdAt,
        }).returning('id')
        return results

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId = rows[0]['id']
    rows = loop.run_until_complete(
        db.table('task_results').where('id', rows[0]['id']).select())
    assert len(rows) == 1
    assert rows[0]['created_at'] == createdAt
    rows = loop.run_until_complete(
        db.table('task_results').where('id', rows[0]['id']).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == insertedId


def test_insert_newline(db: DB):
    createdAt = datetime.datetime(
        2021, 3, 8, 23, 50, tzinfo=datetime.timezone.utc)

    async def insert_fn():
        results = ['started', 'finished']
        results = await db.table('task_results').insert({
            'task_id': 100,
            'created_at': createdAt,
            'result': '\n'.join(results)
        }).returning('id')
        return results

    rows = loop.run_until_complete(insert_fn())
    assert len(rows) == 1
    insertedId = rows[0]['id']
    rows = loop.run_until_complete(
        db.table('task_results').where('id', rows[0]['id']).select())
    assert len(rows) == 1
    assert '\n' in rows[0]['result']
    rows = loop.run_until_complete(
        db.table('task_results').where('id', rows[0]['id']).delete().returning())
    assert len(rows) == 1
    assert rows[0]['id'] == insertedId
