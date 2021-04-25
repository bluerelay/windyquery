import json
import datetime
from typing import Any, Tuple


# add quotes to identifier
def quote_identifier(name: str) -> str:
    name = name.replace('"', '""')
    return '"' + name + '"'


# add quotes to literal
def quote_literal(name: str) -> str:
    name = name.replace("'", "''")
    isExtended = False
    if '\n' in name:
        name = name.replace('\n', '\\n')
        isExtended = True
    if '\t' in name:
        name = name.replace('\t', '\\t')
        isExtended = True
    if '\v' in name:
        name = name.replace('\v', '\\v')
        isExtended = True
    if isExtended:
        name = " E'" + name + "'"
    else:
        name = "'" + name + "'"
    return name


# remove quotes from literal
def unquote_literal(name: str) -> str:
    name = name.replace("''", "'")
    if name.startswith("'") and name.endswith("'"):
        name = name[1:-1]
    return name


def process_value(val: Any) -> Tuple[str, Any]:
    param = None
    if val is None or val == 'NULL':
        val = 'NULL'
    elif val == 'DEFAULT':
        val = 'DEFAULT'
    elif isinstance(val, dict) or isinstance(val, list):
        param = json.dumps(val)
        val = '?'
    elif isinstance(val, datetime.datetime) or isinstance(val, datetime.date):
        param = val
        val = '?'
    elif isinstance(val, bool):
        if val:
            val = 'TRUE'
        else:
            val = 'FALSE'
    elif isinstance(val, str):
        val = quote_literal(val)
    return val, param
