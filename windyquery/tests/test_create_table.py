import asyncio

loop = asyncio.get_event_loop()


def test_create_user_tmp(schema, db):
    async def create_user_tmp():
        return await schema.create('users_tmp',
            schema.column('id').serial().primary_key(),
            schema.column('email').string().nullable(False).unique(),
            schema.column('password').string().nullable(False),
            schema.column('registered_on').timestamp().nullable(False).default("NOW()"),
            schema.column('admin').boolean().nullable(False).default(False)
        )
    async def drop_table():
        return await schema.drop('users_tmp')

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
    assert rows[1]['data_type'] == 'character varying'
    assert rows[2]['column_name'] == 'password'
    assert rows[2]['column_default'] is None
    assert rows[2]['is_nullable'] == 'NO'
    assert rows[2]['data_type'] == 'character varying'
    assert rows[3]['column_name'] == 'registered_on'
    assert rows[3]['column_default'] == 'now()'
    assert rows[3]['is_nullable'] == 'NO'
    assert rows[3]['data_type'] == 'timestamp without time zone'
    assert rows[4]['column_name'] == 'admin'
    assert rows[4]['column_default'] == 'false'
    assert rows[4]['is_nullable'] == 'NO'
    assert rows[4]['data_type'] == 'boolean'


def test_create_unique_index(schema, db):
    async def create_unique_index():
        return await schema.create('users_tmp',
            schema.column('id').serial().primary_key(),
            schema.column('name').string().nullable(False),
            schema.column('user_id').integer().nullable(False),
            schema.unique('user_id', 'name'),
            schema.column('created_at').timestamp().nullable(False).default("NOW()"),
            schema.column('updated_at').timestamp().nullable(False).default("NOW()"),
            schema.column('deleted_at').timestamp().nullable(True)
        )
    async def drop_table():
        return await schema.drop('users_tmp')

    loop.run_until_complete(create_unique_index())
    row = loop.run_until_complete(db.table('pg_indexes').select().where('indexname', 'users_tmp_user_id_name_key').first())
    loop.run_until_complete(drop_table())
    assert row['indexdef'].find('UNIQUE INDEX') != -1
    assert row['indexdef'].find('(user_id, name)') != -1


def test_create_primary_key(schema, db):
    async def create_user_tmp():
        return await schema.create('users_tmp',
            schema.column('name').string().nullable(False),
            schema.column('email').string().nullable(False),
            schema.primary_key('name', 'email'),
            schema.column('password').string().nullable(False),
            schema.column('registered_on').timestamp().nullable(False).default("NOW()"),
            schema.column('admin').boolean().nullable(False).default(False)
        )
    async def drop_table():
        return await schema.drop('users_tmp')

    loop.run_until_complete(create_user_tmp())
    row = loop.run_until_complete(db.table('pg_indexes').select().where('indexname', 'users_tmp_pkey').first())
    loop.run_until_complete(drop_table())
    assert row['indexdef'].find('(name, email)') != -1


def test_create_index_key(schema, db):
    async def create_user_tmp():
        return await schema.create('users_tmp',
            schema.column('name').string().nullable(False),
            schema.column('email').string().nullable(False),
            schema.index('name', 'email'),
            schema.column('password').string().nullable(False),
            schema.column('registered_on').timestamp().nullable(False).default("NOW()"),
            schema.column('admin').boolean().nullable(False).default(False)
        )
    async def drop_table():
        return await schema.drop('users_tmp')

    loop.run_until_complete(create_user_tmp())
    row = loop.run_until_complete(db.table('pg_indexes').select().where('indexname', 'users_tmp_name_email_idx').first())
    loop.run_until_complete(drop_table())
    assert row['indexdef'].find('(name, email)') != -1


def test_drop_nonexists(schema, db):
    # create a simple table and test DROP on it
    loop.run_until_complete(schema.create('users_tmp', schema.column('name').string().nullable(False)))
    rows1 = loop.run_until_complete(db.table('information_schema.columns').select('column_name').where('table_schema', 'public').where('table_name', 'users_tmp'))
    loop.run_until_complete(schema.drop_if_exists('users_tmp'))
    rows2 = loop.run_until_complete(db.table('information_schema.columns').select('column_name').where('table_schema', 'public').where('table_name', 'users_tmp'))
    assert len(rows1) == 1
    assert len(rows2) == 0


def test_create_jsonb(schema, db):
    async def create_jsonb():
        return await schema.create('cards_tmp',
            schema.column('id').integer().nullable(False),
            schema.column('board_id').integer().nullable(False),
            schema.column('data').jsonb()
        )
    async def drop_table():
        return await schema.drop('cards_tmp')

    loop.run_until_complete(create_jsonb())
    rows = loop.run_until_complete(db.table('information_schema.columns').select('data_type').where('table_schema', 'public').where('table_name', 'cards_tmp'))
    loop.run_until_complete(drop_table())
    assert len(rows) == 3
    assert rows[2]['data_type'] == 'jsonb'
