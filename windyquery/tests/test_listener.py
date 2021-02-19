import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_listen_insert(db: DB):

    async def cards_after_insert():
        test_id = 8000
        listener = db.listen('cards')
        await listener.start()
        await db.table('cards').insert({
            'id': test_id,
            'board_id': test_id * 10,
            'data': {'name': 'test'}
        })
        result = await listener.next()
        await db.table('cards').where('id', test_id).delete()
        await listener.stop()
        return result

    result = loop.run_until_complete(cards_after_insert())
    assert result['channel'] == 'cards'
    assert result['payload'] == 'after insert'
    assert result['listener_pid'] > 0
    assert result['notifier_pid'] > 0


def test_listen_insert_with_stmt(db: DB):

    async def cards_after_insert():
        test_id = 8000
        result = None
        async with db.listen('cards') as listener:
            await db.table('cards').insert({
                'id': test_id,
                'board_id': test_id * 10,
                'data': {'name': 'test'}
            })
            result = await listener
            await db.table('cards').where('id', test_id).delete()
        return result

    result = loop.run_until_complete(cards_after_insert())
    assert result['channel'] == 'cards'
    assert result['payload'] == 'after insert'
    assert result['listener_pid'] > 0
    assert result['notifier_pid'] > 0
