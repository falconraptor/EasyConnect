from functools import partial

from db import MYSQL, ConnectionPool, MSSQL
try:
    import pyodbc as pypyodbc
except ImportError:
    import pypyodbc
import pymysql


class MSSQL_TEST(MSSQL):
    _pool: ConnectionPool = ConnectionPool(partial(pypyodbc.connect, f'DRIVER={MSSQL._driver};SERVER={server};UID={user};PWD={password};DATABASE={database};APP={program_name}'))


class MYSQL_TEST(MYSQL):
    _pool: ConnectionPool = ConnectionPool(partial(pymysql.connect, host=, user=, password=, database=, cursorclass=DictCursor, autocommit=None, program_name=))
