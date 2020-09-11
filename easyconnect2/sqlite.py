import sqlite3
from sqlite3 import Connection, Cursor, Row
from typing import Any, Dict, List, Optional, Union

from easyconnect2.base import ConnectionPool


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
