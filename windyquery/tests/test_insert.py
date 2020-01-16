import asyncio
import string
import random


loop = asyncio.get_event_loop()


def test_insert_user(db):
    async def insert_user(email1, email2):
        return await db.table('users').insert(
            {'email': email1, 'password': 'my precious'},
            {'email': email2, 'password': 'my precious'}
        )
    async def get_user(email):
        return await db.table('users').select().where('email', email).first()

    email1 = ''.join(random.choice(string.ascii_letters) for i in range(6))
    email2 = ''.join(random.choice(string.ascii_letters) for i in range(6))
    loop.run_until_complete(insert_user(email1, email2))
    row1 = loop.run_until_complete(get_user(email1))
    row2 = loop.run_until_complete(get_user(email2))
    loop.run_until_complete(db.table('users').where('email', email1).delete())
    loop.run_until_complete(db.table('users').where('email', email2).delete())
    assert row1['email'] == email1
    assert row2['email'] == email2


def test_insert_jsonb(db):
    async def insert_jsonb(test_id):
        return await db.table('cards').insert({
            'id': test_id,
            'board_id': random.randint(1, 100),
            'data': {'name': 'I am {}'.format(test_id), 'address': {'city': 'Chicago', 'state': 'IL'}}
        })
    async def get_jsonb(test_id):
        return await db.table('cards').select('data->>name AS name').where('id', test_id).first()

    test_id = random.randint(10000, 90000)
    loop.run_until_complete(insert_jsonb(test_id))
    row = loop.run_until_complete(get_jsonb(test_id))
    loop.run_until_complete(db.table('cards').where('id', test_id).delete())
    assert row['name'] == 'I am '+str(test_id)
