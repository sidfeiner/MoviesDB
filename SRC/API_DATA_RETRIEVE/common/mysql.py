from typing import Optional, Tuple, List
from mysql import connector
from mysql.connector import cursor

from SRC.API_DATA_RETRIEVE.common.formats import ToDB

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 3306
DEFAULT_DB = 'mysql'


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
        params = params or tuple()
        return crsr.execute(sql, params)

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
        columns = items[0].export_order()
        joined_columns = ', '.join(columns)
        placeholders = [f"%({column})s" for column in columns]
        sql = f"INSERT {'IGNORE' if insert_ignore else ''} INTO {table}({joined_columns}) VALUES({placeholders})"
        return crsr.executemany(sql, [[getattr(item, column) for column in columns] for item in items])

    def execute(self, sql, params, crsr: cursor.CursorBase):
        res = self._execute(crsr, sql, params)
        return res

    def __enter__(self):
        if self._conn is None:
            self._conn = connector.connect(
                host=self.auth.host,
                port=self.auth.port,
                user=self.auth.usr,
                password=self.auth.pwd,
                database=self.auth.db
            )
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()
        self._conn = None


class BatchInserter:
    def __init__(self, mysql: MySQL, batch_size: int, table: str):
        self.mysql = mysql
        self.batch_size = batch_size
        self.table = table

        self.buffer = []

    def process_batch(self):
        crsr = self.mysql.get_cursor()
        self.mysql.insert_many(self.table, self.buffer, crsr)
        crsr.close()
        self.buffer.clear()

    def append(self, item: ToDB):
        self.buffer.append(item)
        if len(self.buffer) == self.batch_size:
            self.process_batch()

    def flush(self):
        if len(self.buffer) > 0:
            self.process_batch()
