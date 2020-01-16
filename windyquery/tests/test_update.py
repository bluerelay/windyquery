import asyncio


loop = asyncio.get_event_loop()


def test_simple_update(db):
    async def get_board_id():
        return await db.table('cards').select('board_id').where('id', 9).first()
    async def update_board_id(test_id):
        return await db.table('cards').where('id', 9).update({'board_id': test_id})
    # update it to 99
    loop.run_until_complete(update_board_id(99))
    row = loop.run_until_complete(get_board_id())
    assert row['board_id'] == 99
    # change it back to 3
    loop.run_until_complete(update_board_id(3))
    row = loop.run_until_complete(get_board_id())
    assert row['board_id'] == 3


def test_simple_update_jsonb(db):
    async def get_cards_city():
        return await db.table('cards').select('data->address->>city AS city').where('id', 8).first()
    async def update_cards_city(city):
        return await db.table('cards').where('id', 8).update({'data->address->city': city})
    # change it to 'New York'
    loop.run_until_complete(update_cards_city('New York'))
    row = loop.run_until_complete(get_cards_city())
    assert row['city'] == 'New York'
    # change it back to 'Chicago'
    loop.run_until_complete(update_cards_city('Chicago'))
    row = loop.run_until_complete(get_cards_city())
    assert row['city'] == 'Chicago'


def test_path_jsonb(db):
    async def get_cards_skill():
        return await db.table('cards').select('data->skill->>java AS java').where('id', 8).first()
    async def update_cards_skill(level):
        return await db.table('cards').where('id', 8).update({'data->skill->java': level})
    # change it to 'Good'
    loop.run_until_complete(update_cards_skill('Good'))
    row = loop.run_until_complete(get_cards_skill())
    assert row['java'] == 'Good'
    # change it back to 'entry level'
    loop.run_until_complete(update_cards_skill('entry level'))
    row = loop.run_until_complete(get_cards_skill())
    assert row['java'] == 'entry level'


def test_set_whole_jsonb(db):
    async def get_cards_address():
        return await db.table('cards').select('data->address->>city AS city').where('id', 8).first()
    async def update_cards_data(data):
        return await db.table('cards').where('id', 8).update({'data': data})
    # change it to {"address": {"city": "New York"}}
    loop.run_until_complete(update_cards_data({'address': {'city': 'New York'}}))
    row = loop.run_until_complete(get_cards_address())
    assert row['city'] == 'New York'
    # change it to {"address": {"city": "Chicago"}}
    loop.run_until_complete(update_cards_data({'address': {'city': 'Chicago'}}))
    row = loop.run_until_complete(get_cards_address())
    assert row['city'] == 'Chicago'


def test_update_from(db):
    async def get_pass(user_id):
        return await db.table('users').select('password').where('id', user_id).first()
    async def update_pass(user_id):
        return await db.table('users').update({'password': 'boards.location'}).update_from('boards').where('boards.user_id', 'users.id').where('users.id', user_id)
    # update the users' passwords to their associated table's field - boards.location
    loop.run_until_complete(update_pass(2))
    row = loop.run_until_complete(get_pass(2))
    assert row['password'] == 'dining room'
    loop.run_until_complete(update_pass(3))
    row = loop.run_until_complete(get_pass(3))
    assert row['password'] == 'south door'
    # change the passwords back
    loop.run_until_complete(db.table('users').update({'password': 'mypass2'}).where('id', 2))
    row = loop.run_until_complete(get_pass(2))
    assert row['password'] == 'mypass2'
    loop.run_until_complete(db.table('users').update({'password': 'mypass3'}).where('id', 3))
    row = loop.run_until_complete(get_pass(3))
    assert row['password'] == 'mypass3'
