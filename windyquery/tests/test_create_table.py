import asyncio

loop = asyncio.get_event_loop()


def test_create_user_tmp(schema):
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
    loop.run_until_complete(drop_table())
    assert 1 == 1

def test_create_unique_index(schema):
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
    loop.run_until_complete(drop_table())
    assert 1 == 1

def test_create_primary_key(schema):
    async def create_user_tmp():
        return await schema.create('users_tmp2',
            schema.column('name').string().nullable(False),
            schema.column('email').string().nullable(False),
            schema.primary_key('name', 'email'),
            schema.column('password').string().nullable(False),
            schema.column('registered_on').timestamp().nullable(False).default("NOW()"),
            schema.column('admin').boolean().nullable(False).default(False)
        )
    async def drop_table():
        return await schema.drop('users_tmp2')
    loop.run_until_complete(create_user_tmp())
    loop.run_until_complete(drop_table())
    assert 1 == 1

def test_create_index_key(schema):
    async def create_user_tmp():
        return await schema.create('users_tmp3',
            schema.column('name').string().nullable(False),
            schema.column('email').string().nullable(False),
            schema.index('name', 'email'),
            schema.column('password').string().nullable(False),
            schema.column('registered_on').timestamp().nullable(False).default("NOW()"),
            schema.column('admin').boolean().nullable(False).default(False)
        )
    async def drop_table():
        return await schema.drop('users_tmp3')
    loop.run_until_complete(create_user_tmp())
    loop.run_until_complete(drop_table())
    assert 1 == 1

def test_drop_nonexists(schema):
    async def drop_table():
        return await schema.drop_if_exists('not_exist_table')
    loop.run_until_complete(drop_table())
    assert 1 == 1

def test_create_jsonb(schema):
    async def create_jsonb():
        return await schema.create('cards_tmp',
            schema.column('id').integer().nullable(False),
            schema.column('board_id').integer().nullable(False),
            schema.column('data').jsonb()
        )
    async def drop_table():
        return await schema.drop('cards_tmp')
    loop.run_until_complete(create_jsonb())
    loop.run_until_complete(drop_table())
    assert 1 == 1
