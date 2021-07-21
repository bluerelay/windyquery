from datetime import datetime
from typing import Dict
from dateutil import rrule
from dateutil import parser
from windyquery.exceptions import RruleNoResults

from ._crud_base import CrudBase


class Rrule(CrudBase):

    def rrule(self, name: str, *items: Dict):
        if len(items) == 0:
            raise UserWarning('rrule cannot be empty')
        columns = list(items[0].keys())
        if len(columns) == 0:
            raise UserWarning('rrule cannot be empty dict')
        # keep only custom fields
        if 'rrule' in columns:
            columns.remove('rrule')
        if 'exrule' in columns:
            columns.remove('exrule')
        if 'rdate' in columns:
            columns.remove('rdate')
        if 'exdate' in columns:
            columns.remove('exdate')
        if 'rrule_slice' in columns:
            columns.remove('rrule_slice')
        if 'rrule_after' in columns:
            columns.remove('rrule_after')
        if 'rrule_before' in columns:
            columns.remove('rrule_before')
        if 'rrule_between' in columns:
            columns.remove('rrule_between')
        # faltten the Dict's into List's and extract rruleset
        values = []
        for item in items:
            val = []
            # get rruleset
            rrset = rrule.rruleset()
            rruleExist = False
            if item.get('rrule', False):
                rruleRawVal = item.get('rrule')
                rruleVals = []
                if isinstance(rruleRawVal, list) or isinstance(rruleRawVal, tuple):
                    rruleVals = list(rruleRawVal)
                elif isinstance(rruleRawVal, str):
                    rruleVals = [rruleRawVal]
                else:
                    raise UserWarning(f'invalid rrule input {rruleRawVal}')
                if len(rruleVals) > 0:
                    rruleExist = True
                for rruleVal in rruleVals:
                    try:
                        rrset.rrule(rrule.rrulestr(rruleVal))
                    except:
                        raise UserWarning(
                            f'invalid rrule: {rruleVal}') from None
            if item.get('exrule', False):
                exruleRawVal = item.get('exrule')
                exruleVals = []
                if isinstance(exruleRawVal, list) or isinstance(exruleRawVal, tuple):
                    exruleVals = list(exruleRawVal)
                elif isinstance(exruleRawVal, str):
                    exruleVals = [exruleRawVal]
                else:
                    raise UserWarning(f'invalid exrule input {exruleRawVal}')
                for exruleVal in exruleVals:
                    try:
                        rrset.exrule(rrule.rrulestr(exruleVal))
                    except:
                        raise UserWarning(
                            f'invalid exrule: {exruleVal}') from None
            if item.get('rdate', False):
                rdateRawVal = item.get('rdate')
                rdateVals = []
                if isinstance(rdateRawVal, list) or isinstance(rdateRawVal, tuple):
                    rdateVals = list(rdateRawVal)
                elif isinstance(rdateRawVal, str):
                    rdateVals = [rdateRawVal]
                else:
                    raise UserWarning(f'invalid rdate input {rdateRawVal}')
                if len(rdateVals) > 0:
                    rruleExist = True
                for rdateVal in rdateVals:
                    try:
                        rrset.rdate(parser.parse(rdateVal))
                    except:
                        raise UserWarning(
                            f'invalid rdate: {rdateVal}') from None
            if item.get('exdate', False):
                exdateRawVal = item.get('exdate')
                exdateVals = []
                if isinstance(exdateRawVal, list) or isinstance(exdateRawVal, tuple):
                    exdateVals = list(exdateRawVal)
                elif isinstance(exdateRawVal, str):
                    exdateVals = [exdateRawVal]
                else:
                    raise UserWarning(f'invalid exdate input {exdateRawVal}')
                for exdateVal in exdateVals:
                    try:
                        rrset.exdate(parser.parse(exdateVal))
                    except:
                        raise UserWarning(
                            f'invalid exdate: {exdateVal}') from None
            if not rruleExist:
                raise UserWarning(
                    f'the input dict {item} must contain a "rrule" or "rdate" field')
            val.append(rrset)
            # get rrule_slice
            sliceVal = item.get('rrule_slice', None)
            if sliceVal is not None and not isinstance(sliceVal, slice):
                raise UserWarning(f'invalid slice: {sliceVal}') from None
            val.append(sliceVal)
            # get rrule_after
            afterVal = item.get('rrule_after', None)
            if afterVal is not None:
                dt = None
                inc = False
                if (isinstance(afterVal, list) or isinstance(afterVal, tuple)) and len(afterVal) >= 1:
                    if isinstance(afterVal[0], datetime):
                        dt = afterVal[0]
                    elif isinstance(afterVal[0], str):
                        dt = parser.parse(afterVal[0])
                    if len(afterVal) == 2:
                        inc = afterVal[1]
                elif isinstance(afterVal, dict):
                    if 'dt' in afterVal:
                        if isinstance(afterVal['dt'], datetime):
                            dt = afterVal['dt']
                        elif isinstance(afterVal['dt'], str):
                            dt = parser.parse(afterVal['dt'])
                    if 'inc' in afterVal:
                        inc = afterVal['inc']
                else:
                    raise UserWarning(
                        f'invalid rrule_after: {afterVal}') from None
                if dt is None:
                    raise UserWarning(
                        f'a datetime.datetime parameter dt is required for rrule_after: {afterVal}') from None
                if not isinstance(inc, bool):
                    raise UserWarning(
                        f'the parameter inc needs to be boolean: {afterVal}') from None
                afterVal = (dt, inc)
            val.append(afterVal)
            # get rrule_before
            beforeVal = item.get('rrule_before', None)
            if beforeVal is not None:
                dt = None
                inc = False
                if (isinstance(beforeVal, list) or isinstance(beforeVal, tuple)) and len(beforeVal) >= 1:
                    if isinstance(beforeVal[0], datetime):
                        dt = beforeVal[0]
                    elif isinstance(beforeVal[0], str):
                        dt = parser.parse(beforeVal[0])
                    if len(beforeVal) == 2:
                        inc = beforeVal[1]
                elif isinstance(beforeVal, dict):
                    if 'dt' in beforeVal:
                        if isinstance(beforeVal['dt'], datetime):
                            dt = beforeVal['dt']
                        elif isinstance(beforeVal['dt'], str):
                            dt = parser.parse(beforeVal['dt'])
                    if 'inc' in beforeVal:
                        inc = beforeVal['inc']
                else:
                    raise UserWarning(
                        f'invalid rrule_before: {beforeVal}') from None
                if dt is None:
                    raise UserWarning(
                        f'a datetime.datetime parameter dt is required for rrule_before: {beforeVal}') from None
                if not isinstance(inc, bool):
                    raise UserWarning(
                        f'the parameter inc needs to be boolean: {beforeVal}') from None
                beforeVal = (dt, inc)
            val.append(beforeVal)
            # get rrule_between
            betweenVal = item.get('rrule_between', None)
            if betweenVal is not None:
                after = None
                before = None
                inc = False
                count = 1
                if (isinstance(betweenVal, list) or isinstance(betweenVal, tuple)) and len(betweenVal) >= 2:
                    if isinstance(betweenVal[0], datetime):
                        after = betweenVal[0]
                    elif isinstance(betweenVal[0], str):
                        after = parser.parse(betweenVal[0])
                    if isinstance(betweenVal[1], datetime):
                        before = betweenVal[1]
                    elif isinstance(betweenVal[1], str):
                        before = parser.parse(betweenVal[1])
                    if len(betweenVal) == 3:
                        inc = betweenVal[2]
                    if len(betweenVal) == 4:
                        count = betweenVal[3]
                elif isinstance(betweenVal, dict):
                    if 'after' in betweenVal:
                        if isinstance(betweenVal['after'], datetime):
                            after = betweenVal['after']
                        elif isinstance(betweenVal['after'], str):
                            after = parser.parse(betweenVal['after'])
                    if 'before' in betweenVal:
                        if isinstance(betweenVal['before'], datetime):
                            before = betweenVal['before']
                        elif isinstance(betweenVal['before'], str):
                            before = parser.parse(betweenVal['before'])
                    if 'inc' in betweenVal:
                        inc = betweenVal['inc']
                    if 'count' in betweenVal:
                        count = betweenVal['count']
                else:
                    raise UserWarning(
                        f'invalid rrule_between: {betweenVal}') from None
                if after is None:
                    raise UserWarning(
                        f'the parameter after needs to be datetime.datetime for rrule_between: {betweenVal}') from None
                if before is None:
                    raise UserWarning(
                        f'the parameter before needs to be datetime.datetime for rrule_between: {betweenVal}') from None
                if not isinstance(inc, bool):
                    raise UserWarning(
                        f'the parameter inc needs to be bool for rrule_between: {betweenVal}') from None
                if not isinstance(count, int):
                    raise UserWarning(
                        f'the parameter count needs to be int for rrule_between: {betweenVal}') from None
                betweenVal = (after, before, inc, count)
            val.append(betweenVal)
            # get the rest custom fields
            for col in columns:
                val.append(item.get(col, 'NULL'))
            values.append(val)
        self.collector.rrule(name, columns, values)
        return self

    def build_rrule(self, items) -> str:
        parsedItems = []
        for item in items:
            name = item['name']
            columns = item['columns']
            values = item['values']
            if not values:
                raise RruleNoResults(
                    f'the rrule for {name} returns no results')
            parsedItem = f'{name} {columns} AS (VALUES {values})'
            parsedItems.append(parsedItem)
        sql = ', '.join(parsedItems)
        return sql
