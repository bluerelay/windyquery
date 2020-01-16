import asyncio


loop = asyncio.get_event_loop()

def test_group_by(schema, db):
    async def create_table():
        return await schema.create('cards_tmp',
            schema.column('id').serial().primary_key(),
            schema.column('board_id').integer().nullable(False)
        )
    async def drop_table():
        return await schema.drop('cards_tmp')

    loop.run_until_complete(create_table())
    loop.run_until_complete(db.table('cards_tmp').insert(
        {'board_id': 1},
        {'board_id': 1},
        {'board_id': 2},
    ))
    rows = loop.run_until_complete(db.table('cards_tmp').select('board_id').group_by('board_id'))
    loop.run_until_complete(drop_table())
    assert len(rows) == 2
    rows.sort(key=lambda x: x['board_id'])
    assert rows[0]['board_id'] == 1
    assert rows[1]['board_id'] == 2
