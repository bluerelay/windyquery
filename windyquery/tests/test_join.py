import asyncio


loop = asyncio.get_event_loop()

def test_simple_join(db):
    async def simple_join():
        return await db.table('cards').join(
                'boards', 'cards.board_id', '=', 'boards.id'
            ).join(
                'users', 'boards.user_id', '=', 'users.id'
            ).select(
                'users.email', 'boards.*'
            ).where("users.id", 2).where('users.admin', '=', True).first()
    row = loop.run_until_complete(simple_join())
    assert row['email'] == 'test@example.com'
    assert row['location'] == 'south door'


