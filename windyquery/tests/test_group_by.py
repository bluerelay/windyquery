import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_group_by(db: DB):
    async def create_table():
        return await db.schema('TABLE cards_tmp').create(
            'id serial PRIMARY KEY',
            'board_id integer not null',
        )

    async def drop_table():
        return await db.schema('TABLE cards_tmp').drop()

    loop.run_until_complete(create_table())
    loop.run_until_complete(db.table('cards_tmp').insert(
        {'board_id': 1},
        {'board_id': 1},
        {'board_id': 2},
    ))
    rows = loop.run_until_complete(
        db.table('cards_tmp').select('board_id').group_by('board_id'))
    loop.run_until_complete(drop_table())
    assert len(rows) == 2
    rows.sort(key=lambda x: x['board_id'])
    assert rows[0]['board_id'] == 1
    assert rows[1]['board_id'] == 2
