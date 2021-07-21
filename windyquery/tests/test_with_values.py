import asyncio
from dateutil.parser import parse
import datetime

from windyquery import DB

loop = asyncio.get_event_loop()


def test_with_values_select(db: DB):
    async def test_fn():
        result = await db.with_values('my_values', {
            'text_col': 'Tom',
            'bool_col': True,
            'num_col': 2,
            'dict_col': {'id': 1},
            'datetime_col': parse('20210720T100000Z'),
            'null_col': 'null',
            'null_col2': None
        }).table('my_values').select()
        return result
    rows = loop.run_until_complete(test_fn())
    assert len(rows) == 1
    assert rows[0]['text_col'] == 'Tom'
    assert rows[0]['bool_col'] == True
    assert rows[0]['num_col'] == 2
    assert rows[0]['dict_col'] == '{"id": 1}'
    assert rows[0]['datetime_col'] == datetime.datetime(
        2021, 7, 20, 10, 0, 0, 0, tzinfo=datetime.timezone.utc)
    assert rows[0]['null_col'] is None
    assert rows[0]['null_col2'] is None


def test_with_values_join(db: DB):
    async def test_fn():
        result = await db.with_values('workers', {
            'task_id': 1,
            'name': 'Tom'
        }, {
            'task_id': 2,
            'name': 'Jerry'
        }).table('workers').select('workers.name AS worker_name', 'tasks.name AS task_name').\
            join('tasks', 'workers.task_id = tasks.id').order_by('tasks.id')
        return result
    rows = loop.run_until_complete(test_fn())
    assert len(rows) == 2
    assert rows[0]['worker_name'] == 'Tom'
    assert rows[1]['worker_name'] == 'Jerry'


def test_with_values_multi_with(db: DB):
    async def test_fn():
        result = await db.with_values('workers1', {
            'task_id': 1,
            'name': 'Tom'
        }, {
            'task_id': 2,
            'name': 'Jerry'
        }).with_values('workers2', {
            'task_id': 1,
            'name': 'Topsy'
        }, {
            'task_id': 2,
            'name': 'Nibbles'
        }).table('tasks').select(
            'workers1.name AS primary_worker_name',
            'workers2.name AS secondary_worker_name',
            'tasks.name AS task_name'
        ).join('workers1', 'workers1.task_id = tasks.id').\
            join('workers2', 'workers2.task_id = tasks.id').\
            order_by('tasks.id')
        return result
    rows = loop.run_until_complete(test_fn())
    assert len(rows) == 2
    assert rows[0]['primary_worker_name'] == 'Tom'
    assert rows[0]['secondary_worker_name'] == 'Topsy'
    assert rows[1]['primary_worker_name'] == 'Jerry'
    assert rows[1]['secondary_worker_name'] == 'Nibbles'


def test_with_values_update(db: DB):
    async def test_fn():
        result = await db.with_values('workers', {
            'task_id': 3,
            'name': 'Tom'
        }, {
            'task_id': 4,
            'name': 'Jerry'
        }).table('tasks').\
            update("name = tasks.name || ' (worked by ' || workers.name || ')'").\
            from_table('workers').\
            where('workers.task_id = tasks.id').\
            returning('workers.name AS worker_name', 'tasks.name AS task_name')
        return result
    rows = loop.run_until_complete(test_fn())
    assert len(rows) == 2
    assert rows[0]['worker_name'] == 'Tom'
    assert rows[0]['task_name'] == 'Tom task (worked by Tom)'
    assert rows[1]['worker_name'] == 'Jerry'
    assert rows[1]['task_name'] == 'Jerry task (worked by Jerry)'

    async def test_fn2():
        result = await db.with_values('workers', {
            'task_id': 3,
            'name': 'Tom'
        }, {
            'task_id': 4,
            'name': 'Jerry'
        }).table('tasks').\
            update("name = workers.name || ' task'").\
            from_table('workers').\
            where('workers.task_id = tasks.id').\
            returning('workers.name AS worker_name', 'tasks.name AS task_name')
        return result
    rows = loop.run_until_complete(test_fn2())
    assert len(rows) == 2
    assert rows[0]['worker_name'] == 'Tom'
    assert rows[0]['task_name'] == 'Tom task'
    assert rows[1]['worker_name'] == 'Jerry'
    assert rows[1]['task_name'] == 'Jerry task'


def test_with_values_raw(db: DB):
    async def test_fn():
        result = await db.with_values('workers', {
            'task_id': 1,
            'name': 'Tom'
        }, {
            'task_id': 2,
            'name': 'Jerry'
        }).raw("""
                SELECT * FROM tasks
                WHERE EXISTS(
                    SELECT 1 FROM workers
                    JOIN task_results ON workers.task_id = task_results.task_id
                    where workers.task_id = tasks.id
                )
            """)
        return result
    rows = loop.run_until_complete(test_fn())
    assert len(rows) == 2
    assert rows[0]['name'] == 'tax return'
    assert rows[1]['name'] == 'pick up kids'


def test_with_values_rrule(db: DB):
    async def test_fn():
        rruleStr = "DTSTART:20210715T100000Z\nRRULE:FREQ=DAILY;COUNT=5"
        exruleStr = "DTSTART:20210715T100000Z\nRRULE:FREQ=DAILY;BYWEEKDAY=SA,SU"
        result = await db.rrule('my_rrules', {
            'rrule': rruleStr,
            'exrule': exruleStr,
            'rrule_after': ['20210716T100000Z'],
            'task_id': 1
        }).with_values('workers', {
            'task_id': 1,
            'name': 'Tom'
        }, {
            'task_id': 2,
            'name': 'Jerry'
        }).table('tasks').\
            join('workers', 'workers.task_id = tasks.id').\
            join('my_rrules', 'my_rrules.task_id = tasks.id').\
            select('workers.name AS worker_name',
                   'rrule AS worked_at', 'tasks.name AS task_name')
        return result
    rows = loop.run_until_complete(test_fn())
    assert len(rows) == 1
    assert rows[0]['worker_name'] == 'Tom'
    assert rows[0]['worked_at'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[0]['task_name'] == 'tax return'


def test_with_values_rrule_using_raw(db: DB):
    async def test_fn():
        rruleStr = "DTSTART:20210715T100000Z\nRRULE:FREQ=DAILY;COUNT=5"
        exruleStr = "DTSTART:20210715T100000Z\nRRULE:FREQ=DAILY;BYWEEKDAY=SA,SU"
        result = await db.rrule('my_rrules', {
            'rrule': rruleStr,
            'exrule': exruleStr,
            'rrule_after': ['20210716T100000Z'],
            'task_id': 1
        }).with_values('workers', {
            'task_id': 1,
            'name': 'Tom'
        }, {
            'task_id': 2,
            'name': 'Jerry'
        }).raw("""
                SELECT workers.name AS worker_name,
                    rrule AS worked_at,
                    tasks.name AS task_name
                FROM tasks
                JOIN workers ON workers.task_id = tasks.id
                JOIN my_rrules ON my_rrules.task_id = tasks.id
            """)
        return result
    rows = loop.run_until_complete(test_fn())
    assert len(rows) == 1
    assert rows[0]['worker_name'] == 'Tom'
    assert rows[0]['worked_at'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[0]['task_name'] == 'tax return'
