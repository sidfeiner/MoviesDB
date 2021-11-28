from typing import Dict, List

from memoization import cached

from SRC.API_DATA_RETRIEVE.common.mysql import MySQL

class Lookup:
    def __init__(self):
        pass

    def _lookup_key(self, _, table, value_col_name, value, id_col_name):
        return table, value_col_name, value, id_col_name

    def _lookup_keys(self, _, table, value_col_name, values, id_col_name):
        sorted_vals = sorted(values)
        return table, value_col_name, '##'.join(sorted_vals), id_col_name

    @cached(custom_key_maker=_lookup_key)
    def lookup_one(self, helper: MySQL, table: str, value_col_name: str, value: str, id_col_name: str = 'id'):
        res = helper.query_one(table, [id_col_name], {value_col_name: value})
        if res is None:
            return None
        return res[0]

    @cached(custom_key_maker=_lookup_keys)
    def lookup_many(self, helper: MySQL, table: str, value_col_name: str, values: List[str], id_col_name: str = 'id') -> Dict[str, int]:
        res = helper.query_all(table, [id_col_name, value_col_name], {value_col_name: values})
        return {row[1]: row[0] for row in res}
