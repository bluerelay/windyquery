import asyncio

loop = asyncio.get_event_loop()


def test_add_column(schema):
    async def add_col():
        return await schema.table('users',
            schema.column('test_col').string(50).nullable(False).default('test default'),
        )
    async def drop_col():
        return await schema.table('users',
            schema.column('test_col').drop(),
        )
    loop.run_until_complete(add_col())
    loop.run_until_complete(drop_col())
    assert 1 == 1

def test_alter_unique_index(schema):
    async def add_unique():
        return await schema.table('boards',
            schema.unique('user_id', 'location'),
        )
    async def drop_unique():
       return await schema.dropIndex('boards_user_id_location_idx')
    loop.run_until_complete(add_unique())
    loop.run_until_complete(drop_unique())
    assert 1 == 1

def test_alter_index(schema):
    async def add_index():
        return await schema.table('boards',
            schema.index('user_id', 'location'),
        )
    async def drop_index():
      return await schema.dropIndex('boards_user_id_location_idx')
    loop.run_until_complete(add_index())
    loop.run_until_complete(drop_index())
    assert 1 == 1

def test_alter_primary_key(schema):
    async def add_primary():
        return await schema.table('cards_copy',
            schema.primary_key('id', 'board_id'),
        )
    async def drop_primary():
        return await schema.dropPrimaryKey('cards_copy')
    loop.run_until_complete(add_primary())
    loop.run_until_complete(drop_primary())
    assert 1 == 1
