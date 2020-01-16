import asyncio

from windyquery import Model

loop = asyncio.get_event_loop()


def test_read_jsonb(model):
    class Card(Model):
        pass
    card = loop.run_until_complete(Card.find(4))
    assert card.data == {
        "finished": True,
        "name": "Hang paintings",
        "tags": [
            "Improvements",
            "Office"
        ]
    }


def test_write_jsonb(model):
    class Card(Model):
        pass
    card = Card(board_id=1, data=[1,'hi',2])
    card = loop.run_until_complete(card.save())
    assert card.id > 0
    card = loop.run_until_complete(Card.find(card.id))
    assert card.data == [1,'hi',2]
    loop.run_until_complete(Card.where('id', card.id).delete())
    card = Card(board_id=1, data='plain string as json')
    card = loop.run_until_complete(card.save())
    assert card.id > 0
    card = loop.run_until_complete(Card.find(card.id))
    assert card.data == 'plain string as json'
    loop.run_until_complete(Card.where('id', card.id).delete())
