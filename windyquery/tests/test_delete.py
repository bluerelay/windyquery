import asyncio
import string
import random

loop = asyncio.get_event_loop()

def test_delete(db):
    row = loop.run_until_complete(db.table('users').insert({'email': 'test@test.com', 'password': 'test pass'}).returning().first())
    assert row['email'] == 'test@test.com'
    loop.run_until_complete(db.table('users').where('id', row['id']).delete())
    row = loop.run_until_complete(db.table('users').select().where('id', row['id']).first())
    assert row is None
