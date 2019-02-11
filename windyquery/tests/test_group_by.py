import asyncio


loop = asyncio.get_event_loop()

def test_group_by(db):
    async def group_by():
        return await db.table('boards').select('user_id').group_by('user_id')
    rows = loop.run_until_complete(group_by())
    assert len(rows) == 2
    assert rows[0]['user_id'] == 2
