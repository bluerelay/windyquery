import asyncio

from windyquery import DB

loop = asyncio.get_event_loop()


def test_create_user_tmp(db: DB):
    async def create_user_tmp():
        return await db.schema('TABLE users_tmp').create(
            'id serial PRIMARY KEY',
            'email text not null unique',
            'password text not null',
            'registered_on timestamp not null DEFAULT NOW()',
            'admin boolean not null default false',
        )

    async def drop_table():
        return await db.schema('TABLE users_tmp').drop()

    loop.run_until_complete(create_user_tmp())
    rows = loop.run_until_complete(db.table('information_schema.columns').select(
        'column_name', 'column_default', 'is_nullable', 'data_type'
    ).where('table_schema', 'public').where('table_name', 'users_tmp'))
    loop.run_until_complete(drop_table())

    # verify each column
    assert len(rows) == 5
    assert rows[0]['column_name'] == 'id'
    assert rows[0]['column_default'].find('nextval') != -1
    assert rows[0]['is_nullable'] == 'NO'
    assert rows[0]['data_type'] == 'integer'
    assert rows[1]['column_name'] == 'email'
    assert rows[1]['column_default'] is None
    assert rows[1]['is_nullable'] == 'NO'
    assert rows[1]['data_type'] == 'text'
    assert rows[2]['column_name'] == 'password'
    assert rows[2]['column_default'] is None
    assert rows[2]['is_nullable'] == 'NO'
    assert rows[2]['data_type'] == 'text'
    assert rows[3]['column_name'] == 'registered_on'
    assert rows[3]['column_default'] == 'now()'
    assert rows[3]['is_nullable'] == 'NO'
    assert rows[3]['data_type'] == 'timestamp without time zone'
    assert rows[4]['column_name'] == 'admin'
    assert rows[4]['column_default'] == 'false'
    assert rows[4]['is_nullable'] == 'NO'
    assert rows[4]['data_type'] == 'boolean'


def test_create_unique_index(db: DB):
    uniqueIdx = 'users_tmp_user_id_name_key'

    async def create_unique_index():
        return await db.schema('TABLE users_tmp').create(
            'id serial PRIMARY KEY',
            'name text not null',
            'user_id integer not null',
            f'CONSTRAINT {uniqueIdx} UNIQUE(user_id, name)',
            'created_at timestamp not null DEFAULT NOW()',
            'updated_at timestamp not null DEFAULT NOW()',
            'deleted_at timestamp null',
        )

    async def drop_table():
        return await db.schema('TABLE users_tmp').drop()

    loop.run_until_complete(create_unique_index())
    rows = loop.run_until_complete(db.table('pg_indexes').select().where(
        'indexname', uniqueIdx))
    loop.run_until_complete(drop_table())
    assert rows[0]['indexdef'].find('UNIQUE INDEX') != -1
    assert rows[0]['indexdef'].find('(user_id, name)') != -1


def test_create_primary_key(db: DB):
    async def create_user_tmp():
        return await db.schema('TABLE users_tmp').create(
            'name text not null',
            'email text not null',
            'PRIMARY KEY(name, email)',
            'password text not null',
            'registered_on timestamp not null DEFAULT NOW()',
            'admin boolean not null default false',
        )

    async def drop_table():
        return await db.schema('TABLE users_tmp').drop()

    loop.run_until_complete(create_user_tmp())
    rows = loop.run_until_complete(db.table('pg_indexes').select().where(
        'indexname', 'users_tmp_pkey'))
    loop.run_until_complete(drop_table())
    assert rows[0]['indexdef'].find('(name, email)') != -1


def test_create_index_key(db: DB):
    indexName = 'users_tmp_name_email_idx'

    async def create_user_tmp():
        return await db.schema('TABLE users_tmp').create(
            'name text not null',
            'email text not null',
            f'CONSTRAINT {indexName} UNIQUE(name, email)',
            'password text not null',
            'registered_on timestamp not null DEFAULT NOW()',
            'admin boolean not null default false',
        )

    async def drop_table():
        return await db.schema('TABLE users_tmp').drop()

    loop.run_until_complete(create_user_tmp())
    rows = loop.run_until_complete(db.table('pg_indexes').select().where(
        'indexname', indexName))
    loop.run_until_complete(drop_table())
    assert rows[0]['indexdef'].find('(name, email)') != -1


def test_drop_nonexists(db: DB):
    # create a simple table and test DROP on it
    loop.run_until_complete(
        db.schema('TABLE users_tmp').create('name text not null'))
    rows1 = loop.run_until_complete(db.table('information_schema.columns').select(
        'column_name').where('table_schema', 'public').where('table_name', 'users_tmp'))
    loop.run_until_complete(db.schema('TABLE IF EXISTS users_tmp').drop())
    rows2 = loop.run_until_complete(db.table('information_schema.columns').select(
        'column_name').where('table_schema', 'public').where('table_name', 'users_tmp'))
    assert len(rows1) == 1
    assert len(rows2) == 0


def test_create_jsonb(db: DB):
    async def create_jsonb():
        return await db.schema('TABLE cards_tmp').create(
            'id integer not null',
            'board_id integer not null',
            'data jsonb',
        )

    async def drop_table():
        return await db.schema('TABLE cards_tmp').drop()

    loop.run_until_complete(create_jsonb())
    rows = loop.run_until_complete(db.table('information_schema.columns').select(
        'data_type').where('table_schema', 'public').where('table_name', 'cards_tmp'))
    loop.run_until_complete(drop_table())
    assert len(rows) == 3
    assert rows[2]['data_type'] == 'jsonb'
