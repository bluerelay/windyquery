import asyncio
import string
import random


loop = asyncio.get_event_loop()

def test_insert(db):
    test_id = 9998
    name = ''.join(random.choice(string.ascii_letters) for i in range(6))
    loop.run_until_complete(db.table('test').insertRaw(
        '("id", "name") SELECT $1, $2 WHERE NOT EXISTS (SELECT "id" FROM test WHERE "id" = $1)', [test_id, name]
    ))
    # insert it again but should fail
    loop.run_until_complete(db.table('test').insertRaw(
        '("id", "name") SELECT $1, $2 WHERE NOT EXISTS (SELECT "id" FROM test WHERE "id" = $1)', [test_id, name]
    ))
    rows = loop.run_until_complete(db.table('test').select().where('id', test_id))
    assert len(rows) == 1
    row = rows[0]
    assert row['name'] == name
    loop.run_until_complete(db.table('test').where('id', test_id).delete())
