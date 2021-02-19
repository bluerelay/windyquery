import asyncio
import pytest
import asyncpg

from windyquery import DB


loop = asyncio.get_event_loop()


def test_add_column(db: DB):
    async def add_col():
        await db.schema('TABLE users').alter(
            "ADD COLUMN test_col varchar(50) NOT NULL DEFAULT 'test_col_default'",
        )

    async def drop_col():
        return await db.schema('TABLE users').alter(
            'drop COLUMN test_col',
        )

    loop.run_until_complete(add_col())
    rows = loop.run_until_complete(db.table('users').insert({
        'email': 'test@test.com',
        'password': 'my precious'
    }).returning())
    assert rows[0]['test_col'] == 'test_col_default'

    loop.run_until_complete(drop_col())
    rows = loop.run_until_complete(db.table('users').select())
    assert not hasattr(rows[0], 'test_col')


def test_alter_unique_index(db: DB):
    indexname = 'boards_user_id_location_idx'

    async def add_unique():
        return await db.schema(f'UNIQUE INDEX {indexname} ON boards').create(
            'user_id', 'location'
        )

    async def drop_unique():
        return await db.schema(f'INDEX {indexname}').drop()

    loop.run_until_complete(add_unique())
    row = {'user_id': 1399, 'location': 'northwest'}
    loop.run_until_complete(db.table('boards').insert(row))

    # inserting 2nd time violates unique constraint
    with pytest.raises(asyncpg.exceptions.UniqueViolationError) as excinfo:
        loop.run_until_complete(db.table('boards').insert(row))
    assert type(excinfo.value) is asyncpg.exceptions.UniqueViolationError
    loop.run_until_complete(db.table('boards').where('user_id', 1399).delete())
    loop.run_until_complete(drop_unique())


def test_alter_index(db: DB):
    indexname = 'boards_user_id_location_idx'

    async def add_index():
        return await db.schema(f'UNIQUE INDEX {indexname} ON boards').create(
            'user_id', 'location'
        )

    async def drop_index():
        return await db.schema(f'INDEX {indexname}').drop()

    loop.run_until_complete(add_index())
    rows = loop.run_until_complete(
        db.table('pg_indexes').select().where('indexname', indexname))
    assert len(rows) == 1
    loop.run_until_complete(drop_index())


def test_alter_primary_key(db: DB):
    pkname = 'id_board_id_pky'

    async def add_primary():
        return await db.schema('TABLE cards_copy').alter(
            f'add CONSTRAINT {pkname} PRIMARY KEY (id, board_id)',
        )

    async def drop_primary():
        await db.schema('TABLE cards_copy').alter(
            f'DROP CONSTRAINT {pkname}'
        )

    # pkey is (id, board_id)
    loop.run_until_complete(add_primary())
    rows = loop.run_until_complete(
        db.table('pg_indexes').select().where('tablename', 'cards_copy'))
    assert rows[0]['indexdef'].find('(id, board_id)') != -1

    # pkey is deleted
    loop.run_until_complete(drop_primary())
    rows = loop.run_until_complete(
        db.table('pg_indexes').select().where('tablename', 'cards_copy'))
    assert len(rows) == 0
