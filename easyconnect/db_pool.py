import json
from collections.abc import Iterable
from functools import partial
from platform import system
from threading import Lock, Thread
from time import sleep
from typing import List, Dict, Any, Optional, Callable

from easyconnect.types import pymysql, pypyodbc

SERVERS = {}


class TmpConnection:
    def __init__(self, pool):
        self.pool = pool

    def __enter__(self):
        self.con = self.pool.get_connection()
        return self.con

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_type, (pymysql.OperationalError, pypyodbc.InterfaceError, pymysql.InternalError)):
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
        self.connections, self.running, self.free = [], [], []
        self.__lock = Lock()
        self.num = 0

    def __del__(self):
        [c.close() for c in self.connections]

    def connection(self) -> TmpConnection:
        return TmpConnection(self)

    def cursor(self) -> TmpCursor:
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
    success_hooks: List[Callable[[str, Optional[Iterable]], None]] = []
    failure_hooks: List[Callable[[str, Optional[Iterable]], None]] = []

    @classmethod
    def connection(cls) -> TmpConnection:
        return cls._pool.connection()

    @classmethod
    def cursor(cls) -> TmpCursor:
        return cls._pool.cursor()

    @classmethod
    def execute(cls, sql: str, params: Optional[Iterable] = None):
        if issubclass(cls, MYSQL):
            sql = sql.replace('?', '%s')
        conn = cls._pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, *([[json.dumps(p) if isinstance(p, dict) else p for p in params]] if params else []))
            cls._pool.free_connection(conn)
            [hook(sql, params) for hook in cls.success_hooks]
        except (pymysql.OperationalError, pypyodbc.InterfaceError, pymysql.InternalError) as e:
            if isinstance(e, pymysql.InternalError) and 'Packet sequence' not in e.__repr__():
                raise e
            conn.close()
            cls._pool.running.remove(conn)
            cls._pool.connections.remove(conn)
            cls.execute(sql, params)
            [hook(sql, params) for hook in cls.failure_hooks]
        except pypyodbc.Error as e:
            if 'Connection is busy' not in repr(e) and 'Invalid cursor state' not in repr(e):
                raise e
            cls.execute(sql, params)
            cls._pool.free_connection(conn)
            [hook(sql, params) for hook in cls.failure_hooks]

    @classmethod
    def fetch(cls, sql: str, params: Optional[Iterable] = None) -> Dict[str, Any]:
        if issubclass(cls, MYSQL):
            sql = sql.replace('?', '%s')
        conn = cls._pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, *([params] if params else []))
                results = cursor.fetchone() or {}
            cls._pool.free_connection(conn)
            return results or {}
        except (pymysql.OperationalError, pypyodbc.InterfaceError, pymysql.InternalError) as e:
            if isinstance(e, pymysql.InternalError) and 'Packet sequence' not in e.__repr__():
                raise e
            conn.close()
            results = cls.fetch(sql, params)
            cls._pool.running.remove(conn)
            cls._pool.connections.remove(conn)
            return results or {}
        except pypyodbc.Error as e:
            if 'Connection is busy' not in repr(e) and 'Invalid cursor state' not in repr(e):
                raise e
            results = cls.fetch(sql, params)
            cls._pool.free_connection(conn)
            return results or {}

    @classmethod
    def fetchall(cls, sql: str, params: Optional[Iterable] = None) -> List[Dict[str, Any]]:
        if issubclass(cls, MYSQL):
            sql = sql.replace('?', '%s')
        conn = cls._pool.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, *([params] if params else []))
                results = cursor.fetchall()
            cls._pool.free_connection(conn)
            return results
        except (pymysql.OperationalError, pypyodbc.InterfaceError, pymysql.InternalError) as e:
            if isinstance(e, pymysql.InternalError) and 'Packet sequence' not in e.__repr__():
                raise e
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
    def get_databases(cls) -> Dict[str, dict]:  # used for api mapping
        raise NotImplementedError


class MYSQL(DBConnection):
    def __init__(self, host: str, user: str, password: str, database: Optional[str] = None, port: Optional[int] = None, autocommit: Optional[bool] = None, program_name: Optional[str] = None):
        self._pool = ConnectionPool(partial(pymysql.connect, host=host, user=user, password=password, database=database, port=port, cursorclass=pymysql.DictCursor, autocommit=autocommit, program_name=program_name))

    @classmethod
    def get_databases(cls) -> Dict[str, dict]:
        databases = {}
        for row in cls.fetchall("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY, EXTRA FROM information_schema.columns WHERE TABLE_SCHEMA NOT IN (%s, %s, %s, %s, %s) AND TABLE_SCHEMA NOT LIKE %s ORDER BY TABLE_SCHEMA, TABLE_NAME", ('information_schema', 'phpmyadmin', 'mysql', 'performance_schema', 'sys', 'phabricator%')):
            if row['TABLE_SCHEMA'].lower() not in databases:
                databases[row['TABLE_SCHEMA'].lower()] = {'NAME': row['TABLE_SCHEMA']}
            database = databases[row['TABLE_SCHEMA'].lower()]
            if row['TABLE_NAME'].lower() not in database:
                database[row['TABLE_NAME'].lower()] = {'NAME': row['TABLE_NAME']}
            database[row['TABLE_NAME'].lower()][row['COLUMN_NAME'].lower()] = {'position': row['ORDINAL_POSITION'], 'default': row['COLUMN_DEFAULT'], 'nullable': row['IS_NULLABLE'] == 'YES', 'type': row['DATA_TYPE'], 'max_length': row['CHARACTER_MAXIMUM_LENGTH'] or -1, 'primary_key': row['COLUMN_KEY'] == 'PRI', 'auto_inc': row['EXTRA'] == 'auto_increment', 'name': row['COLUMN_NAME']}
        for database in databases.values():
            for table in database:
                if table == 'NAME':
                    continue
                database[table] = {k: {a: b for a, b in v.items() if a != 'position'} if k != 'NAME' else v for k, v in sorted(database[table].items(), key=lambda d: d[1]['position'] if d[0] != 'NAME' else 999)}
        return databases


class MSSQL(DBConnection):
    _driver = 'FreeTDS' if system() == 'Linux' else ([_ for _ in pypyodbc.drivers() if 'SQL Server' in _] or ['SQL Server'])[0]

    def __init__(self, host: str, user: str, password: str, database: str = '', port: int = 1433, program_name: str = ''):
        self._pool = ConnectionPool(partial(pypyodbc.connect, f'DRIVER={MSSQL._driver};SERVER={host},{port};UID={user};PWD={password};DATABASE={database};APP={program_name}'))

    @classmethod
    def get_databases(cls) -> Dict[str, dict]:
        databases = {}
        for db in cls.fetchall('SELECT "name" FROM master.dbo.sysdatabases WHERE "name" NOT IN (?, ?, ?, ?) AND "name" NOT LIKE ? ORDER BY "name"', ('master', 'tempdb', 'model', 'msdb', 'ReportServer%')):
            db = f'"{db["name"]}"'
            for row in cls.fetchall(f'SELECT c.TABLE_CATALOG, c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION, c.COLUMN_DEFAULT, c.IS_NULLABLE, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, tc.CONSTRAINT_TYPE, ic.is_identity FROM {db}.information_schema.COLUMNS c LEFT JOIN {db}.information_schema.KEY_COLUMN_USAGE kcu ON c.TABLE_CATALOG=kcu.TABLE_CATALOG AND c.TABLE_NAME=kcu.TABLE_NAME AND c.COLUMN_NAME=kcu.COLUMN_NAME LEFT JOIN {db}.information_schema.TABLE_CONSTRAINTS tc ON kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME LEFT JOIN {db}.sys.tables t ON t.name=c.TABLE_NAME LEFT JOIN {db}.sys.identity_columns ic ON t.object_id=ic.object_id AND ic.name=c.COLUMN_NAME ORDER BY c.TABLE_CATALOG, c.TABLE_NAME'):
                if row['TABLE_CATALOG'].lower() not in databases:
                    databases[row['TABLE_CATALOG'].lower()] = {'NAME': row['TABLE_CATALOG']}
                database = databases[row['TABLE_CATALOG'].lower()]
                if row['TABLE_NAME'].lower() not in database:
                    database[row['TABLE_NAME'].lower()] = {'NAME': row['TABLE_NAME']}
                database[row['TABLE_NAME'].lower()][row['COLUMN_NAME'].lower()] = {'position': row['ORDINAL_POSITION'], 'default': row['COLUMN_DEFAULT'], 'nullable': row['IS_NULLABLE'] == 'YES', 'type': row['DATA_TYPE'], 'max_length': row['CHARACTER_MAXIMUM_LENGTH'] or -1, 'name': row['COLUMN_NAME'], 'primary_key': row['CONSTRAINT_TYPE'] == 'PRIMARY KEY', 'auto_inc': row['is_identity'] or False}
        for database in databases.values():
            for table in database:
                if table == 'NAME':
                    continue
                database[table] = {k: {a: b for a, b in v.items() if a != 'position'} if k != 'NAME' else v for k, v in sorted(database[table].items(), key=lambda d: d[1]['position'] if d[0] != 'NAME' else 999)}
        return databases
