import asyncio
import string
import random
import json

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
