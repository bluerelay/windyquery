import json
from typing import Any, Tuple


# add quotes to identifier
def quote_identifier(name: str) -> str:
    name = name.replace('"', '""')
    return '"' + name + '"'


# add quotes to literal
def quote_literal(name: str) -> str:
    name = name.replace("'", "''")
    # if '\\' in name:
    #     name = name.replace('\\', '\\\\')
    #     name = " E'" + name + "'"
    # else:
    #    name = "'" + name + "'"
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
    elif isinstance(val, dict):
        param = json.dumps(val)
        val = '?'
    elif isinstance(val, bool):
        param = val
        val = '?'
    elif isinstance(val, str):
        val = quote_literal(val)
    return val, param
