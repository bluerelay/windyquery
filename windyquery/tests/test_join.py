import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_simple_join(db: DB):
    async def simple_join():
        # test join with existing data
        return await db.table('cards').join(
            'boards', 'cards.board_id', '=', 'boards.id'
        ).join(
            'users', 'boards.user_id', '=', 'users.id'
        ).select(
            'users.email', 'boards.*'
        ).where("users.id", 1).where('users.admin', '=', True)

    rows = loop.run_until_complete(simple_join())
    row = rows[0]
    assert row['email'] == 'test@example.com'
    assert row['location'] == 'southwest'
    assert row['id'] == 1
    assert row['user_id'] == 1


def test_join1(db: DB):
    async def join_fn():
        result = await db.table('cards').join(
            'boards', 'cards.board_id', '=', 'boards.id'
        ).join(
            'users', 'boards.user_id = ?', 1
        ).select(
            'users.email', 'boards.*'
        ).where("users.id", 1).where('users.admin', '=', True)
        return result
    rows = loop.run_until_complete(join_fn())
    assert len(rows) > 0
    row = rows[0]
    assert row['email'] == 'test@example.com'
    assert row['location'] == 'southwest'
