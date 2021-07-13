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
        sql = 'WITH ' + ', '.join(parsedItems)
        return sql
