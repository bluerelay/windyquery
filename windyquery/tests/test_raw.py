import asyncio
import random
import string

from windyquery import DB

loop = asyncio.get_event_loop()


def test_raw_select(db: DB):
    async def raw_select():
        return await db.raw('SELECT * FROM cards WHERE board_id = $1', 5)
    rows = loop.run_until_complete(raw_select())
    assert rows[0]['id'] == 7


def test_select_raw(db: DB):
    async def select_raw():
        return await db.raw('SELECT ROUND(AVG(board_id),1) AS avg_id, COUNT(1) AS copies FROM cards WHERE id in ($1, $2, $3)', 4, 5, 6)
    rows = loop.run_until_complete(select_raw())
    from decimal import Decimal
    row = rows[0]
    assert row['avg_id'] == Decimal('4.7')
    assert row['copies'] == 3


def test_insert_raw(db: DB):
    user_id = 29998
    location = ''.join(random.choice(string.ascii_letters) for i in range(6))

    # test inserRaw with a rather complex query
    sql = 'INSERT INTO boards ("user_id", "location") SELECT $1, $2 WHERE NOT EXISTS (SELECT "user_id" FROM boards WHERE "user_id" = $1)'

    loop.run_until_complete(db.raw(sql, user_id, location))

    # insert it again that has no new row gets inserted
    loop.run_until_complete(db.raw(sql, user_id, location))

    # verify that only 1 row was inserted
    rows = loop.run_until_complete(
        db.table('boards').select().where('user_id', user_id))
    loop.run_until_complete(
        db.table('boards').where('user_id', user_id).delete())
    assert len(rows) == 1
    assert rows[0]['user_id'] == user_id
    assert rows[0]['location'] == location
