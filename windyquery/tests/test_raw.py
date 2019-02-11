import asyncio


loop = asyncio.get_event_loop()

def test_raw_select(db):
    async def raw_select():
        return await db.raw('SELECT * FROM cards WHERE board_id = $1', [5]).first()
    row = loop.run_until_complete(raw_select())
    assert row['id'] == 9247

def test_select_raw(db):
    async def select_raw():
        return await db.table('cards').select_raw('ROUND(AVG(board_id),1) AS avg_id, COUNT(1) AS copies').where('id', [4,5,6]).first()
    row = loop.run_until_complete(select_raw())
    assert row['avg_id'] == 2.0
    assert row['copies'] == 3