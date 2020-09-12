from platform import system
from threading import Lock

from easyconnect2.mappings import Server

try:
    import pyodbc as pypyodbc
except ImportError:
    from easyconnect2 import pypyodbc


class _Connection:
    def __init__(self, pool):
        self.pool = pool

    def __enter__(self):
        self.conn = self.pool.get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.free_connection(self.conn)


class ConnectionPool:
    def __init__(self):
        self.connections = []
        self.running = []
        self.free = []
        self.lock = Lock()

    def get_connection(self):
        if self.free:
            with self.lock:
                conn = self.free.pop()
        else:
            conn = self._get_connection()
            self.connections.append(conn)
        self.running.append(conn)
        return conn

    def free_connection(self, conn):
        self.running.remove(conn)
        self.free.append(conn)
        return self

    def _get_connection(self):
        raise NotImplemented

    def map_db(self) -> Server:
        raise NotImplemented  # TODO

    def generate_modals(self):
        raise NotImplemented  # TODO

    def __del__(self):
        self.close_all()

    def close_all(self):
        [conn.close() for conn in self.connections]

    def connection(self):
        return _Connection(self)


try:
    import pymysql
except ImportError:
    pymysql = None

if pymysql:
    class MYSQL(ConnectionPool):
        def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306, program_name: str = None):
            super().__init__()
            self.host = host
            self.user = user
            self.password = password
            self.database = database
            self.port = port
            self.program_name = program_name

        def _get_connection(self):
            return pymysql.connect(self.host, self.user, self.password, self.database, self.port, autocommit=None, cursorclass=pymysql.cursors.DictCursor, program_name=self.program_name)


class MSSQL(ConnectionPool):
    _driver = 'FreeTDS' if system() == 'Linux' else ([_ for _ in pypyodbc.drivers() if 'SQL Server' in _] or ['SQL Server'])[0]

    def __init__(self, host: str, user: str, password: str, database: str, port: int = 1433, program_name: str = None, driver: str = _driver):
        super().__init__()
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.program_name = program_name
        self.driver = driver

    def _get_connection(self):
        return pypyodbc.connect(f'DRIVER={self.driver};SERVER={self.host},{self.port};UID={self.user};PWD={self.password};DATABASE={self.database};APP={self.program_name}')
