import asyncio


loop = asyncio.get_event_loop()

def test_simple_update(db):
    async def get_board_id():
        return await db.table('cards').select('board_id').where('id', 2).first()
    async def update_board_id(test_id):
        return await db.table('cards').where('id', 2).update({'board_id': test_id})
    loop.run_until_complete(update_board_id(3))
    row = loop.run_until_complete(get_board_id())
    assert row['board_id'] == 3
    loop.run_until_complete(update_board_id(2))
    row = loop.run_until_complete(get_board_id())
    assert row['board_id'] == 2

def test_simple_update_jsonb(db):
    async def get_cards_city():
        return await db.table('cards').select('data->address->>city AS city').where('id', 8).first()
    async def update_cards_city(city):
        return await db.table('cards').where('id', 8).update({'data->address->city': city})
    loop.run_until_complete(update_cards_city('New York'))
    row = loop.run_until_complete(get_cards_city())
    assert row['city'] == 'New York'
    loop.run_until_complete(update_cards_city('Chicago'))
    row = loop.run_until_complete(get_cards_city())
    assert row['city'] == 'Chicago'

def test_path_jsonb(db):
    async def get_cards_skill():
        return await db.table('cards').select('data->skill->>java AS java').where('id', 8).first()
    async def update_cards_skill(level):
        return await db.table('cards').where('id', 8).update({'data->skill->java': level})
    loop.run_until_complete(update_cards_skill('Good'))
    row = loop.run_until_complete(get_cards_skill())
    assert row['java'] == 'Good'

def test_set_whole_jsonb(db):
    async def get_cards_address():
        return await db.table('cards').select('data->address->>city AS city').where('id', 7).first()
    async def update_cards_address():
        return await db.table('cards').where('id', 7).update({'data': {'address': {'city': 'New York'}}})
    loop.run_until_complete(update_cards_address())
    row = loop.run_until_complete(get_cards_address())
    assert row['city'] == 'New York'

def test_update_from(db):
    async def get_pass(user_id):
        return await db.table('users').select('password').where('id', user_id).first()
    async def update_pass(user_id):
        return await db.table('users').update({'password': 'boards.location'}).update_from('boards').where('boards.user_id', 'users.id').where('users.id', user_id)
    loop.run_until_complete(update_pass(1))
    row = loop.run_until_complete(get_pass(1))
    assert row['password'] == 'dining room'
    loop.run_until_complete(update_pass(2))
    row = loop.run_until_complete(get_pass(2))
    assert row['password'] == 'south door'
