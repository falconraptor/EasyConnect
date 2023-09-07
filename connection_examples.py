from functools import partial

from easyconnect_old import pypyodbc
from easyconnect_old.db_pool import ConnectionPool, MYSQL, MSSQL
from easyconnect_old.types import pymysql


class MSSQL_TEST(MSSQL):
    _pool: ConnectionPool = ConnectionPool(partial(pypyodbc.connect, f'DRIVER={MSSQL._driver};SERVER={""};UID={""};PWD={""};DATABASE={""};APP={""}'))


class MYSQL_TEST(MYSQL):
    _pool: ConnectionPool = ConnectionPool(partial(pymysql.connect, host='', user='', password='', database='', cursorclass=pymysql.cursors.DictCursor, autocommit=None, program_name=""))
