import re
import sqlite3
from sqlite3 import Connection, Cursor, Row
from typing import Any, Dict, List, Optional, Union

from easyconnect2.base import ConnectionPool
from easyconnect2.mappings import Column, Schema, Server, Table


class _Cursor(Cursor):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.commit()

    def fetchall(self, sql: str, *params: Any) -> List[Union[tuple, Dict[str, Any], Row]]:
        self.execute(sql, params)
        return super().fetchall()

    def fetchone(self, sql: str, *params: Any) -> Union[tuple, Dict[str, Any], Row]:
        self.execute(sql, params)
        return super().fetchone() or self.row_factory(self, [])


class _Connection(Connection):
    def cursor(self, cursorClass: Optional[type] = _Cursor) -> Cursor:
        return super().cursor(cursorClass)

    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     if not exc_tb:
    #         self.commit()
    #     super().__exit__(exc_type, exc_val, exc_tb)


class SQLite(ConnectionPool):
    def __init__(self, file: str, timeout: int = 5, check_same_thread: bool = False, use_dict: bool = True):
        super().__init__()
        self.file = file
        self.timeout = timeout
        self.check_same_thread = check_same_thread
        self.use_dict = use_dict

    @staticmethod
    def _row_factory(cursor: sqlite3.Cursor, row: tuple) -> Dict[str, Any]:
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)} if row else {}

    def _get_connection(self) -> _Connection:
        conn = sqlite3.connect(self.file, timeout=self.timeout, check_same_thread=self.check_same_thread, factory=_Connection)
        if self.use_dict:
            conn.row_factory = self._row_factory
        return conn

    @staticmethod
    def _map_column(table: Table, table_sql: Dict[str, str], cursor):
        for column_sql in cursor.fetchall(f'PRAGMA "{table.schema.name}".table_info("{table.name}")'):
            column = Column(column_sql['name'], table, column_sql['type'])
            column.primary_key = column_sql['pk'] == 1
            column.auto_inc = 'autoincrement' in re.search(rf'{column_sql["name"]} ([\w\s]+),', table_sql['sql'])[1].lower() if column_sql['type'].lower() == 'integer' and column_sql['pk'] else False
            column.nullable = column_sql['notnull'] == 0
            column.default = column_sql['dflt_value']
            column.max_length = int((re.search(r'(\d+)', column_sql['type']) or [None, -1])[1])

    def map_db(self) -> Server:
        server = Server(self.file, self)
        with self.connection() as conn:
            with conn.cursor() as cursor:
                for db in cursor.fetchall('PRAGMA database_list'):
                    schema = Schema(db['name'], server)
                    for table_sql in cursor.fetchall(f'SELECT name, sql FROM "{schema.name}".sqlite_master WHERE type=?', 'table'):
                        table = Table(table_sql['name'], schema)
                        self._map_column(table, table_sql, cursor)
        return server
