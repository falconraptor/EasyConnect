from collections import Iterable
from platform import system
from threading import Lock, Thread
from time import sleep
from typing import List, Dict, Any, Optional

try:
    import pymysql
except ImportError:
    class pymysql:
        def __getattribute__(self, item):
            pass

        def __setattr__(self, key, value):
            pass
try:
    import pyodbc as pypyodbc
except ImportError:
    try:
        import pypyodbc
    except ImportError:
        class pypyodbc:
            def __getattribute__(self, item):
                pass

            def __setattr__(self, key, value):
                pass

SERVERS = {}


class TmpConnection:
    def __init__(self, pool):
        self.pool = pool

    def __enter__(self):
        self.con = self.pool.get_connection()
        return self.con

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_type, (pymysql.OperationalError, pypyodbc.InterfaceError)):
            self.con.close()
            self.pool.running.remove(self.con)
            self.pool.connections.remove(self.con)
        elif isinstance(exc_type, pypyodbc.Error):
            def free():
                sleep(5)
                self.pool.free_connection(self.con)

            if 'Connection is busy' in exc_val:
                Thread(target=free).start()
        else:
            self.pool.free_connection(self.con)
            return False
        return True


class TmpCursor(TmpConnection):
    def __enter__(self):
        self.con = self.pool.get_connection()
        self.cursor = self.con.cursor()
        return self.cursor.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.__exit__(exc_type, exc_val, exc_tb)
        return super().__exit__(exc_type, exc_val, exc_tb)


class ConnectionPool:
    def __init__(self, connect):
        self.connect = connect
        self.connections = []
        self.running = []
        self.free = []
        self.__lock = Lock()
        self.num = 0

    def __del__(self):
        [c.close() for c in self.connections]

    def connection(self):
        return TmpConnection(self)

    def cursor(self):
        return TmpCursor(self)

    def free_connection(self, conn):
        self.running.remove(conn)
        self.free.append(conn)

    def get_connection(self):
        try:
            with self.__lock:
                conn = self.free.pop()
        except IndexError:
            self.num += 1
            if self.connect.keywords.get('program_name'):
                kw = dict(self.connect.keywords)
                kw['program_name'] += f' {self.num}'
                conn = self.connect.func(**kw)
            else:
                conn = self.connect.func(self.connect.args[0] + f' {self.num}')
            self.connections.append(conn)
        self.running.append(conn)
        return conn


class DBConnection:
    _pool: ConnectionPool

    @classmethod
    def connection(cls):
        return cls._pool.connection()

    @classmethod
    def cursor(cls):
        return cls._pool.cursor()

    @classmethod
    def execute(cls, sql: str, params: Optional[Iterable] = None) -> None:
        conn = cls._pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, *([params] if params else []))
            cls._pool.free_connection(conn)
        except (pymysql.OperationalError, pypyodbc.InterfaceError):
            conn.close()
            cls._pool.running.remove(conn)
            cls._pool.connections.remove(conn)
            cls.execute(sql, params)
        except pypyodbc.Error as e:
            if 'Connection is busy' not in repr(e) and 'Invalid cursor state' not in repr(e):
                raise e
            cls.execute(sql, params)
            cls._pool.free_connection(conn)

    @classmethod
    def fetch(cls, sql: str, params: Optional[Iterable] = None) -> Dict[str, Any]:
        conn = cls._pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, *([params] if params else []))
                results = cursor.fetchone()
            cls._pool.free_connection(conn)
            return results
        except (pymysql.OperationalError, pypyodbc.InterfaceError):
            conn.close()
            results = cls.fetch(sql, params)
            cls._pool.running.remove(conn)
            cls._pool.connections.remove(conn)
            return results
        except pypyodbc.Error as e:
            if 'Connection is busy' not in repr(e) and 'Invalid cursor state' not in repr(e):
                raise e
            results = cls.fetch(sql, params)
            cls._pool.free_connection(conn)
            return results

    @classmethod
    def fetchall(cls, sql: str, params: Optional[Iterable] = None) -> List[Dict[str, Any]]:
        conn = cls._pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, *([params] if params else []))
                results = cursor.fetchall()
            cls._pool.free_connection(conn)
            return results
        except (pymysql.OperationalError, pypyodbc.InterfaceError):
            conn.close()
            results = cls.fetchall(sql, params)
            cls._pool.running.remove(conn)
            cls._pool.connections.remove(conn)
            return results
        except pypyodbc.Error as e:
            if 'Connection is busy' not in repr(e) and 'Invalid cursor state' not in repr(e):
                raise e
            results = cls.fetchall(sql, params)
            cls._pool.free_connection(conn)
            return results

    @classmethod
    def get_databases(cls) -> Dict[str, dict]:
        raise NotImplementedError


class MYSQL(DBConnection):
    pass


class MSSQL(DBConnection):
    _driver = 'FreeTDS' if system() == 'Linux' else 'SQL Server'


pypyodbc.pooling = False
