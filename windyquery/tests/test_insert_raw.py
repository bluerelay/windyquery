import asyncio
import string
import random


loop = asyncio.get_event_loop()

def test_insert(db):
    user_id = 29998
    location = ''.join(random.choice(string.ascii_letters) for i in range(6))

    # test inserRaw with a rather complex query
    sql = '("user_id", "location") SELECT $1, $2 WHERE NOT EXISTS (SELECT "user_id" FROM boards WHERE "user_id" = $1)'
    loop.run_until_complete(db.table('boards').insertRaw(sql, [user_id, location]))

    # insert it again that has no new row gets inserted
    loop.run_until_complete(db.table('boards').insertRaw(sql, [user_id, location]))

    # verify that only 1 row was inserted
    rows = loop.run_until_complete(db.table('boards').select().where('user_id', user_id))
    loop.run_until_complete(db.table('boards').where('user_id', user_id).delete())
    assert len(rows) == 1
    assert rows[0]['user_id'] == user_id
    assert rows[0]['location'] == location
