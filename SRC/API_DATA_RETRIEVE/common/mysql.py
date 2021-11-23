import logging
from typing import Optional, Tuple, List, Dict, Any, Iterable
from mysql import connector
from mysql.connector import cursor

from SRC.API_DATA_RETRIEVE.common.contract import DEFAULT_DB
from SRC.API_DATA_RETRIEVE.common.formats import ToDB

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306


class MySQLAuth:
    def __init__(self, usr: str, pwd: str, host: Optional[str] = DEFAULT_HOST, port: Optional[int] = DEFAULT_PORT,
                 db: Optional[str] = DEFAULT_DB):
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self.usr = usr
        self.pwd = pwd
        self.db = db


class MySQL:
    def __init__(self, auth: MySQLAuth):
        self.auth = auth
        self._conn = None  # type: Optional[connector.MySQLConnection]

    def _execute(self, crsr: cursor.CursorBase, sql: str, params: Optional[Tuple] = None):
        return crsr.execute(sql, params) if params is not None else crsr.execute(sql)

    def get_cursor(self) -> cursor.CursorBase:
        return self._conn.cursor()

    def query_one(self, sql: str, params: Optional[Tuple] = None, crsr: Optional[cursor.CursorBase] = None):
        final_crsr = crsr or self._conn.cursor()
        self._execute(final_crsr, sql, params)
        res = crsr.fetchone()
        if final_crsr == crsr:
            crsr.close()
        return res

    def query_all(self, sql: str, params: Optional[Tuple] = None, crsr: Optional[cursor.CursorBase] = None):
        final_crsr = crsr or self._conn.cursor()
        self._execute(final_crsr, sql, params)
        res = crsr.fetchall()
        if final_crsr == crsr:
            crsr.close()
        return res

    def insert_many(self, table: str, items: List[ToDB], crsr: cursor.CursorBase, insert_ignore: bool = False):
        if len(items) == 0:
            return
        ordered = items[0].export_order()
        mapping = items[0].column_mapping()
        columns = [mapping.get(column, column) for column in ordered]
        joined_columns = ', '.join(columns)
        placeholders = ', '.join([f"%s" for _ in columns])
        sql = f"INSERT {'IGNORE' if insert_ignore else ''} INTO {table}({joined_columns}) VALUES({placeholders})"
        params = [[getattr(item, column) for column in ordered] for item in items]
        logging.info(f"inserting {len(params)} items to table {table}")
        return crsr.executemany(sql, params)

    def execute(self, sql, crsr: cursor.CursorBase, params=None):
        res = self._execute(crsr, sql, params)
        return res

    def commit(self):
        self._conn.commit()

    def __enter__(self):
        if self._conn is None:
            self._conn = connector.connect(
                host=self.auth.host,
                port=self.auth.port,
                user=self.auth.usr,
                password=self.auth.pwd,
                database=self.auth.db
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()
        self._conn = None


class BatchObjInserter:
    def __init__(self, mysql: MySQL, crsr: cursor.CursorBase, batch_size: int, table: str, cache_size: int = 0,
                 cache_key_func: Optional = lambda x: getattr(x, 'id')):
        self.mysql = mysql
        self.crsr = crsr
        self.batch_size = batch_size
        self.table = table
        self.cache_size = cache_size
        self.cache_key_func = cache_key_func
        self.cache = set()

        self.buffer = []

    def process_batch(self):
        self.mysql.insert_many(self.table, self.buffer, self.crsr, insert_ignore=True)
        self.buffer.clear()

    def append(self, item: ToDB):
        if item is None:
            return

        if self.cache_size > 0 and len(self.cache) < self.cache_size:
            if self.cache_key_func(item) not in self.cache:
                self.cache.add(self.cache_key_func(item))
                self.buffer.append(item)
        else:
            self.buffer.append(item)

        if len(self.buffer) == self.batch_size:
            self.process_batch()

    def extend(self, items: Iterable[ToDB]):
        if items is None:
            return
        for item in items:
            self.append(item)

    def flush(self):
        if len(self.buffer) > 0:
            self.process_batch()


class Item(ToDB):
    def __init__(self, value, column):
        self.value = value
        self.column = column

    @classmethod
    def export_order(cls) -> List[str]:
        return ['value']

    def override_target_names(self) -> Dict[str, str]:
        return {'value': self.column}


class BatchValueInserter(BatchObjInserter):
    def __init__(self, mysql: MySQL, crsr: cursor.CursorBase, batch_size: int, table: str, column: str,
                 cache_size: int = 5000):
        super().__init__(mysql, crsr, batch_size, table, cache_size, lambda x: getattr(x, 'value'))
        self.column = column
        self.cache_size = cache_size
        self.cache = set()

    def append(self, item: Any):
        if self.cache_size > 0 and len(self.cache) < self.cache_size:
            if item not in self.cache:
                self.cache.add(item)
                super().append(Item(item, self.column))
        else:
            if item not in self.cache:
                self.cache.add(item)
                super().append(Item(item, self.column))

    def extend(self, items: Iterable[Any]):
        for item in items:
            self.append(item)
