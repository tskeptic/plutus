
from typing import Callable
import gzip
import json
import os


def get_file_operation(fpath: str) -> Callable:
    """Defines file operation based on the extention"""
    f_ext = fpath.split('.')[-1]
    if f_ext in ['gz', 'gzip']:
        return gzip.open
    elif f_ext in ['jl', 'jsonl', 'jsonlines']:
        return open
    else:
        raise Exception(f'unexpected extension: {f_ext}')

def read_jsonlines_file(fpath: str, ign_not_found: bool = True) -> list:
    """Reads JSON lines file into list object"""
    file_exists = os.path.isfile(fpath)
    if (not file_exists) and (not ign_not_found):
        raise FileNotFoundError(fpath)
    op = get_file_operation(fpath)
    if file_exists:
        return [json.loads(r) for r in op(fpath, 'r')]
    else:
        return []


def write_jsonlines_file(fpath: str, rows: list) -> int:
    """Writes jsons (dicts) to a file"""
    op = get_file_operation(fpath)
    total_rows = 0
    with op(fpath, 'wt') as f_out:
        for row in rows:
            f_out.write(''.join([json.dumps(row), '\n']))
            total_rows += 1
    return total_rows
