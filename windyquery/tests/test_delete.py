import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_delete(db: DB):
    rows = loop.run_until_complete(db.table('users').insert(
        {'email': 'test@test.com', 'password': 'test pass'}).returning())
    assert rows[0]['email'] == 'test@test.com'
    loop.run_until_complete(
        db.table('users').where('id', rows[0]['id']).delete())
    rows = loop.run_until_complete(
        db.table('users').select().where('id', rows[0]['id']))
    assert len(rows) == 0
