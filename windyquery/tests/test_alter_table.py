import asyncio
import pytest
import asyncpg


loop = asyncio.get_event_loop()


def test_add_column(schema, db):
    async def add_col():
        return await schema.table('users',
            schema.column('test_col').string(50).nullable(False).default('test_col_default'),
        )
    async def drop_col():
        return await schema.table('users',
            schema.column('test_col').drop(),
        )

    loop.run_until_complete(add_col())
    row = loop.run_until_complete(db.table('users').insert({
        'email': 'test@test.com',
        'password': 'my precious'
    }).returning().first())
    assert row['test_col'] == 'test_col_default'

    loop.run_until_complete(drop_col())
    row = loop.run_until_complete(db.table('users').select().first())
    assert not hasattr(row, 'test_col')


def test_alter_unique_index(schema, db):
    async def add_unique():
        return await schema.table('boards',
            schema.unique('user_id', 'location'),
        )
    async def drop_unique():
       return await schema.dropIndex('boards_user_id_location_idx')

    loop.run_until_complete(add_unique())
    row = {'user_id': 1399, 'location': 'northwest'}
    loop.run_until_complete(db.table('boards').insert(row))

    # inserting 2nd time violates unique constraint
    with pytest.raises(asyncpg.exceptions.UniqueViolationError) as excinfo:
        loop.run_until_complete(db.table('boards').insert(row))
    assert type(excinfo.value) is asyncpg.exceptions.UniqueViolationError
    loop.run_until_complete(db.table('boards').where('user_id', 1399).delete())
    loop.run_until_complete(drop_unique())


def test_alter_index(schema, db):
    indexname = 'boards_user_id_location_idx'

    async def add_index():
        return await schema.table('boards',
            schema.index('user_id', 'location'),
        )
    async def drop_index():
      return await schema.dropIndex(indexname)

    loop.run_until_complete(add_index())
    rows = loop.run_until_complete(db.table('pg_indexes').select().where('indexname', indexname))
    assert len(rows) == 1
    loop.run_until_complete(drop_index())


def test_alter_primary_key(schema, db):
    async def add_primary():
        return await schema.table('cards_copy',
            schema.primary_key('id', 'board_id'),
        )
    async def drop_primary():
        return await schema.dropPrimaryKey('cards_copy')

    # pkey is (id, board_id)
    loop.run_until_complete(add_primary())
    row = loop.run_until_complete(db.table('pg_indexes').select().where('tablename', 'cards_copy').first())
    assert row['indexdef'].find('(id, board_id)') != -1

    # pkey is deleted
    loop.run_until_complete(drop_primary())
    rows = loop.run_until_complete(db.table('pg_indexes').select().where('tablename', 'cards_copy'))
    assert len(rows) == 0
