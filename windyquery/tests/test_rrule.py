import asyncio
import datetime
import pytest

from windyquery import DB
from windyquery.exceptions import RruleNoResults

loop = asyncio.get_event_loop()

rruleStr1 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;COUNT=5
"""

rruleStr2 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;INTERVAL=10;COUNT=5
RRULE:FREQ=DAILY;INTERVAL=5;COUNT=3
"""

exruleStr1 = """
DTSTART:20210303T100000Z
RRULE:FREQ=DAILY;BYWEEKDAY=SA,SU
"""


def test_rrule_select1(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': rruleStr1}).table('rrule1').select())
    assert len(rows) == 5
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 4, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 5, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[3]['rrule'] == datetime.datetime(
        2021, 3, 6, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[4]['rrule'] == datetime.datetime(
        2021, 3, 7, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_select2(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule2', {'rrule': rruleStr2}).table('rrule2').select())
    assert len(rows) == 6
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 8, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 13, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[3]['rrule'] == datetime.datetime(
        2021, 3, 23, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[4]['rrule'] == datetime.datetime(
        2021, 4, 2, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[5]['rrule'] == datetime.datetime(
        2021, 4, 12, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_where1(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': rruleStr1}).table('rrule1').where('rrule >= ?', datetime.datetime(2021, 3, 6, 10, 0, tzinfo=datetime.timezone.utc)).select())
    assert len(rows) == 2
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 6, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 7, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_where2(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule2', {'rrule': rruleStr2}).table('rrule2').where('rrule < ?', datetime.datetime(2021, 3, 13, 10, 0, tzinfo=datetime.timezone.utc)).select())
    assert len(rows) == 2
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 8, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_join(db: DB):
    rows = loop.run_until_complete(
        db.rrule('task_rrules', {
            'task_id': 1, 'rrule': rruleStr1
        }, {
            'task_id': 2, 'rrule': rruleStr2
        }).table('task_rrules').
        join('tasks', 'tasks.id', '=', 'task_rrules.task_id').
        where('rrule > ? AND rrule <= ?',
              datetime.datetime(2021, 3, 5, 10, 0,
                                tzinfo=datetime.timezone.utc),
              datetime.datetime(2021, 3, 8, 10, 0,
                                tzinfo=datetime.timezone.utc),
              ).
        select('task_rrules.rrule', 'tasks.name'))

    assert len(rows) == 3
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 6, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[0]['name'] == 'tax return'
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 7, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['name'] == 'tax return'
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 8, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['name'] == 'pick up kids'


def test_rrule_update(db: DB):
    loop.run_until_complete(
        db.rrule('task_rrules', {
            'task_id': 1, 'rrule': rruleStr1
        }, {
            'task_id': 2, 'rrule': rruleStr2
        }).table('task_results').update("result = 'finished'").
        from_table('task_rrules').
        where('task_rrules.task_id = task_results.task_id').
        where('rrule > ?',  datetime.datetime(2021, 3, 20, 10, 0,
                                              tzinfo=datetime.timezone.utc)))

    rows = loop.run_until_complete(
        db.table('task_results').where('result', 'finished').select())
    assert len(rows) == 1
    assert rows[0]['task_id'] == 2


def test_rrule_raw(db: DB):
    rows = loop.run_until_complete(db.rrule('task_rrules', {
        'task_id': 1, 'rrule': rruleStr1
    }, {
        'task_id': 3, 'rrule': rruleStr2
    }).raw("""
        INSERT INTO task_results
        (task_id, result)
        SELECT task_id, 'pending' from task_rrules
        WHERE rrule > $1
        RETURNING id, task_id
        """, datetime.datetime(2021, 3, 20, 10, 0,
                               tzinfo=datetime.timezone.utc)))
    assert len(rows) == 3
    assert rows[0]['task_id'] == 3
    assert rows[1]['task_id'] == 3
    assert rows[2]['task_id'] == 3

    rows = loop.run_until_complete(db.rrule('task_rrules', {
        'task_id': 1, 'rrule': rruleStr1
    }, {
        'task_id': 3, 'rrule': rruleStr2
    }).raw("""
        DELETE FROM task_results
        WHERE EXISTS(
            SELECT 1 FROM task_rrules
            WHERE 
                task_id = task_results.task_id AND
                rrule > $1
        )
        RETURNING id, task_id
        """, datetime.datetime(2021, 3, 20, 10, 0,
                               tzinfo=datetime.timezone.utc)))

    assert len(rows) == 3
    assert rows[0]['task_id'] == 3
    assert rows[1]['task_id'] == 3
    assert rows[2]['task_id'] == 3


def test_rrule_slice(db: DB):
    rruleStr3 = """
    DTSTART:20210203T100000Z
    RRULE:FREQ=DAILY
    """
    rows = loop.run_until_complete(db.rrule('task_rrules', {'rrule': rruleStr3, 'rrule_slice': slice(
        3)}).table('task_rrules').select('task_rrules.rrule'))
    assert len(rows) == 3
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 2, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 2, 4, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 2, 5, 10, 0, tzinfo=datetime.timezone.utc)

    rows = loop.run_until_complete(db.rrule('task_rrules', {'rrule': rruleStr3, 'rrule_slice': slice(
        10, 20, 2)}).table('task_rrules').select('task_rrules.rrule'))
    assert len(rows) == 5
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 2, 13, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 2, 15, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 2, 17, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[3]['rrule'] == datetime.datetime(
        2021, 2, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[4]['rrule'] == datetime.datetime(
        2021, 2, 21, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_no_results(db: DB):
    with pytest.raises(RruleNoResults) as excinfo:
        loop.run_until_complete(
            db.rrule('rrule1', {'rrule': rruleStr1, 'rrule_slice': slice(1000, 1001)}, ).table('rrule1').select())
    assert type(excinfo.value) is RruleNoResults
    # it should reset the build after an exception is raised
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': rruleStr1, 'rrule_slice': slice(0, 1)}, ).table('rrule1').select())
    assert len(rows) == 1


def test_rrule_multirrule(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': [rruleStr1, rruleStr2]}).order_by('rrule').table('rrule1').select())
    assert len(rows) == 10
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 4, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 5, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[3]['rrule'] == datetime.datetime(
        2021, 3, 6, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[4]['rrule'] == datetime.datetime(
        2021, 3, 7, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[5]['rrule'] == datetime.datetime(
        2021, 3, 8, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[6]['rrule'] == datetime.datetime(
        2021, 3, 13, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[7]['rrule'] == datetime.datetime(
        2021, 3, 23, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[8]['rrule'] == datetime.datetime(
        2021, 4, 2, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[9]['rrule'] == datetime.datetime(
        2021, 4, 12, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_rdate(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rdate': '20210503T100000Z'}).table('rrule1').select())
    assert len(rows) == 1
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 5, 3, 10, 0, tzinfo=datetime.timezone.utc)
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rdate': ['20210503T100000Z', '20210603T100000Z']}).table('rrule1').select())
    assert len(rows) == 2
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 5, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 6, 3, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_exdate(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': rruleStr1, 'exdate': '20210304T100000Z'}).table('rrule1').select())
    assert len(rows) == 4
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 5, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 6, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[3]['rrule'] == datetime.datetime(
        2021, 3, 7, 10, 0, tzinfo=datetime.timezone.utc)
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': rruleStr1, 'exdate': ['20210304T100000Z', '20210306T100000Z']}).table('rrule1').select())
    assert len(rows) == 3
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 5, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 7, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_exrule(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': rruleStr1, 'exrule': exruleStr1}).table('rrule1').select())
    assert len(rows) == 3
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 4, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 5, 10, 0, tzinfo=datetime.timezone.utc)


def test_rrule_coexist(db: DB):
    rows = loop.run_until_complete(
        db.rrule('rrule1', {'rrule': [rruleStr1, rruleStr2],
                            'exrule': exruleStr1,
                            'rdate': ['20210603T100000Z', '20210310T100000Z'],
                            'exdate': ['20210304T100000Z', '20210313T100000Z']}).order_by('rrule').table('rrule1').select())
    assert len(rows) == 8
    assert rows[0]['rrule'] == datetime.datetime(
        2021, 3, 3, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[1]['rrule'] == datetime.datetime(
        2021, 3, 5, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[2]['rrule'] == datetime.datetime(
        2021, 3, 8, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[3]['rrule'] == datetime.datetime(
        2021, 3, 10, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[4]['rrule'] == datetime.datetime(
        2021, 3, 23, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[5]['rrule'] == datetime.datetime(
        2021, 4, 2, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[6]['rrule'] == datetime.datetime(
        2021, 4, 12, 10, 0, tzinfo=datetime.timezone.utc)
    assert rows[7]['rrule'] == datetime.datetime(
        2021, 6, 3, 10, 0, tzinfo=datetime.timezone.utc)
