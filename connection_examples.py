import sqlite3
from functools import partial

from easyconnect import pypyodbc
from easyconnect.db_pool import ConnectionPool, MYSQL, MSSQL, SQLite
from easyconnect.types import pymysql


class MSSQL_TEST(MSSQL):
    _pool: ConnectionPool = ConnectionPool(partial(pypyodbc.connect, f'DRIVER={MSSQL._driver};SERVER={""};UID={""};PWD={""};DATABASE={""};APP={""}'))


class MYSQL_TEST(MYSQL):
    _pool: ConnectionPool = ConnectionPool(partial(pymysql.connect, host='', user='', password='', database='', cursorclass=pymysql.cursors.DictCursor, autocommit=None, program_name=""))


class SQLite_TEST(SQLite):
    _pool: ConnectionPool = ConnectionPool(partial(sqlite3.connect, database='', factory=SQLite.Connection))
