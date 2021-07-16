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


def test_rrule_functions(db: DB):
    from dateutil.parser import parse
    rruleStr = "DTSTART:20210715T100000Z\nRRULE:FREQ=DAILY;COUNT=5"
    exruleStr = "DTSTART:20210715T100000Z\nRRULE:FREQ=DAILY;BYWEEKDAY=SA,SU"

    async def test_fn():
        result1 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': [parse('20210716T100000Z')]}).table('my_rrules').select()
        result2 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': {'dt': parse('20210716T100000Z')}}).table('my_rrules').select()
        result3 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': ('20210716T100000Z',)}).table('my_rrules').select()
        result4 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': {'dt': '20210716T100000Z', 'inc': True}}).table('my_rrules').select()
        result5 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': ['20210716T100000Z', True]}).table('my_rrules').select()
        result6 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_before': [parse('20210719T100000Z')]}).table('my_rrules').select()
        result7 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_before': {'dt': parse('20210719T100000Z')}}).table('my_rrules').select()
        result8 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_before': ('20210719T100000Z',)}).table('my_rrules').select()
        result9 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_before': {'dt': '20210719T100000Z', 'inc': True}}).table('my_rrules').select()
        result10 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_before': ['20210719T100000Z', True]}).table('my_rrules').select()
        result11 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_between': [parse('20210716T100000Z'), parse('20210720T100000Z')]}).table('my_rrules').select()
        result12 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_between': {'after': parse('20210716T100000Z'), 'before': parse('20210720T100000Z')}}).table('my_rrules').select()
        result13 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_between': ('20210716T100000Z', '20210720T100000Z')}).table('my_rrules').select()
        result14 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_between': {'after': '20210716T100000Z', 'before': '20210719T100000Z', 'inc': True}}).table('my_rrules').select()
        result15 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_between': ['20210716T100000Z', '20210719T100000Z', True]}).table('my_rrules').select()
        result16 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': ['20210716T100000Z'], 'rrule_before': ['20210719T100000Z'], 'rrule_between': ['20210716T100000Z', '20210719T100000Z', True]}).table('my_rrules').select()
        result17 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': ['20210719T100000Z'], 'rrule_before': ['20210719T100000Z'], 'rrule_between': ['20210716T100000Z', '20210719T100000Z', True]}).table('my_rrules').select()
        result18 = await db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_slice': slice(1), 'rrule_between': ['20210716T100000Z', '20210719T100000Z', True]}).table('my_rrules').select()
        return result1, result2, result3, result4, result5, \
            result6, result7, result8, result9, result10, \
            result11, result12, result13, result14, result15, \
            result16, result17, result18

    result1, result2, result3, result4, result5, \
        result6, result7, result8, result9, result10, \
        result11, result12, result13, result14, result15, \
        result16, result17, result18 = loop.run_until_complete(
            test_fn())
    assert len(result1) == 1
    assert result1[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result2) == 1
    assert result2[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result3) == 1
    assert result3[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result4) == 1
    assert result4[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result5) == 1
    assert result5[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result6) == 1
    assert result6[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result7) == 1
    assert result7[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result8) == 1
    assert result8[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result9) == 1
    assert result9[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result10) == 1
    assert result10[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result11) == 1
    assert result11[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result12) == 1
    assert result12[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result13) == 1
    assert result13[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result14) == 2
    assert result14[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert result14[1]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result15) == 2
    assert result15[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert result15[1]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result16) == 1
    assert result16[0]['rrule'] == datetime.datetime(
        2021, 7, 19, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result17) == 1
    assert result17[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)
    assert len(result18) == 1
    assert result18[0]['rrule'] == datetime.datetime(
        2021, 7, 16, 10, 0, tzinfo=datetime.timezone.utc)

    with pytest.raises(RruleNoResults) as excinfo:
        loop.run_until_complete(db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_after': [
                                '20210719T100000Z']}).table('my_rrules').select())
    assert type(excinfo.value) is RruleNoResults

    with pytest.raises(RruleNoResults) as excinfo:
        loop.run_until_complete(db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_before': [
                                '20210715T100000Z']}).table('my_rrules').select())
    assert type(excinfo.value) is RruleNoResults

    with pytest.raises(RruleNoResults) as excinfo:
        loop.run_until_complete(db.rrule('my_rrules', {'rrule': rruleStr, 'exrule': exruleStr, 'rrule_between': {
                                'after': parse('20210716T100000Z'), 'before': parse('20210719T100000Z')}}).table('my_rrules').select())
    assert type(excinfo.value) is RruleNoResults
