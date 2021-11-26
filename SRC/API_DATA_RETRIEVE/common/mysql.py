import logging
from typing import Optional, Tuple, List, Dict, Any, Iterable, Union
from mysql import connector
from mysql.connector import cursor

from SRC.API_DATA_RETRIEVE.common.formats import ToDB
from SRC.API_DATA_RETRIEVE.contract import DEFAULT_DB

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
    def __init__(self, auth: Optional[MySQLAuth] = None, conn=None):
        assert auth is not None or conn is not None
        self.auth = auth
        self._conn = conn  # type: Optional[connector.MySQLConnection]

    @classmethod
    def from_conn(cls, conn):
        return MySQL()

    def _execute(self, crsr: cursor.CursorBase, sql: str, params: Optional[Tuple] = None):
        return crsr.execute(sql, params) if params is not None else crsr.execute(sql)

    def get_cursor(self) -> cursor.CursorBase:
        return self._conn.cursor()

    @staticmethod
    def _results_as_dict(crsr: cursor.CursorBase, results: List[Tuple]) -> List[dict]:
        return [{k[0]: v for (k, v) in zip(crsr.description, res)} for res in results]

    def fetch_one(self, sql: str, params: Optional[Tuple] = None, crsr: Optional[cursor.CursorBase] = None,
                  as_dict: bool = False):
        """Low level query
        :param sql: raw SQL
        :param params: params for the query
        :param crsr: Optional cursor. If not given, one will be created and closed at the end of the query
        :param as_dict: If True, every record will be returned as a dictionary where the key is the column's name
        :return: None if no record was found, single record otherwise (tuple if as_dict = False, dict if as_dict = True)
        """
        final_crsr = crsr or self._conn.cursor()
        self._execute(final_crsr, sql, params)
        res = final_crsr.fetchone()
        if final_crsr == crsr:
            crsr.close()
        return self._results_as_dict(final_crsr, res) if as_dict else res

    def fetch_limit(self, sql: str, params: Optional[Union[Tuple, Dict]] = None,
                    crsr: Optional[cursor.CursorBase] = None,
                    as_dict: bool = False, limit: Optional[int] = None):
        """Low level query
        :param sql: raw SQL
        :param params: params for the query
        :param crsr: Optional cursor. If not given, one will be created and closed at the end of the query
        :param as_dict: If True, every record will be returned as a dictionary where the key is the column's name
        :param limit: Maximum amount of records to return. If None, no limit is enforced
        :return: List of records (tuples if as_dict = False, dict if as_dict = True)
        """
        sql = f"""
        {sql}
        {"" if limit is None else f"limit {limit}"}
        """
        final_crsr = crsr or self._conn.cursor()
        self._execute(final_crsr, sql, params)
        res = final_crsr.fetchall()
        if final_crsr == crsr:
            crsr.close()
        return self._results_as_dict(final_crsr, res) if as_dict else res

    def fetch_all(self, sql: str, params: Optional[Union[Tuple, Dict]] = None,
                  crsr: Optional[cursor.CursorBase] = None,
                  as_dict: bool = False):
        return self.fetch_limit(sql, params, crsr, as_dict, None)

    def _generate_query(self, table: str, projection: List[str] = None, filters: Dict[str, Any] = None):
        projection = projection or ['*']
        filters = filters or dict()
        where_clauses = []
        where_values = []
        for column, value in filters.items():
            where_clauses.append(f"{column} = %s")
            where_values.append(value)

        sql = f"""
                select {', '.join(projection)}
                from {table}
                {"where" if len(where_clauses) > 0 else ""} {' and '.join(where_clauses)}
                """
        return sql, where_values

    def query_one(self, table: str, projection: List[str] = None, filters: Dict[str, Any] = None,
                  crsr: Optional[cursor.CursorBase] = None, as_dict: bool = False):
        """Higher level query. Build the SQL based on the given params
        :param table: Table to query
        :param projection: Columns to return. If None, returns all columns
        :param filters: Filters for where clause. If None, return everything
        :param crsr: Optional cursor. If not given, one will be created and closed at the end of the query
        :param as_dict: If True, every record will be returned as a dictionary where the key is the column's name
        :return: None if no record was found, single record otherwise (tuple if as_dict = False, dict if as_dict = True)
        """
        sql, where_values = self._generate_query(table, projection, filters)
        return self.fetch_one(sql, tuple(where_values), crsr, as_dict)

    def query_limit(self, table: str, projection: List[str] = None, filters: Dict[str, Any] = None,
                    crsr: Optional[cursor.CursorBase] = None, as_dict: bool = False, limit: Optional[int] = None):
        """Higher level query. Build the SQL based on the given params
        :param table: Table to query
        :param projection: Columns to return. If None, returns all columns
        :param filters: Filters for where clause. If None, return everything
        :param crsr: Optional cursor. If not given, one will be created and closed at the end of the query
        :param as_dict: If True, every record will be returned as a dictionary where the key is the column's name
        :param limit: Maximum amount of records to return. If None, no limit is enforced
        :return: List of records (tuples if as_dict = False, dict if as_dict = True)
        """
        sql, where_values = self._generate_query(table, projection, filters)
        if limit is not None:
            sql += f" limit {limit}"
        return self.fetch_all(sql, tuple(where_values), crsr, as_dict)

    def query_all(self, table: str, projection: List[str] = None, filters: Dict[str, Any] = None,
                  crsr: Optional[cursor.CursorBase] = None, as_dict: bool = False):
        return self.query_limit(table, projection, filters, crsr, as_dict, None)

    def insert_many(self, table: str, items: List[ToDB], crsr: cursor.CursorBase, insert_ignore: bool = False):
        if len(items) == 0:
            return
        ordered = items[0].export_order()
        mapping = items[0].column_mapping()
        columns = [mapping.get(column, column) for column in ordered]
        joined_columns = ', '.join(columns)
        placeholders = ', '.join([f"%s" for _ in columns])
        ignore_clause = f'on duplicate key update {columns[0]}={columns[0]} ' if insert_ignore else ''
        sql = f"INSERT INTO {table}({joined_columns}) VALUES({placeholders}) {ignore_clause}"
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
        if item is None or (isinstance(item, str) and len(item.strip()) == 0):
            return
        super().append(Item(item, self.column))

    def extend(self, items: Iterable[Any]):
        for item in items:
            self.append(item)
