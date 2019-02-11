import asyncio
import string
import random

loop = asyncio.get_event_loop()

def test_delete(db):
    test_id = 99999
    test_name = 'delete'
    loop.run_until_complete(db.table('test').insert({'id': test_id, 'name': test_name}))
    row = loop.run_until_complete(db.table('test').select().where('id', test_id).first())
    assert row['name'] == test_name
    loop.run_until_complete(db.table('test').where('id', test_id).delete())
    row = loop.run_until_complete(db.table('test').select().where('id', test_id).first())
    assert row is None
