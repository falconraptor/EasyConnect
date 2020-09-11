from datetime import datetime
from threading import Thread
from typing import Type, Union

from easyconnect.db_pool import ConnectionPool, MSSQL, MYSQL, SERVERS, SQLite
from easyconnect.types import pypyodbc

__all__ = ['MSSQL', 'MYSQL', 'SERVERS', 'map_dbs', 'SQLite', 'ConnectionPool']


def map_dbs(*classes: Type[Union[MYSQL, MSSQL, SQLite]], wait: bool = False, override: bool = False):
    def get(c):
        start = datetime.now()
        c.mapping = SERVERS[c.__name__.lower()] = (c.get_databases(), c)
        print(f'[SERVER] {c.__name__}:', datetime.now() - start)

    if not override and SERVERS:
        return
    threads = [Thread(target=get, args=(c,)) for c in classes if type(c) == type and issubclass(c, (MSSQL, MYSQL, SQLite))]
    [thread.start() for thread in threads]
    if wait:
        [thread.join() for thread in threads]


pypyodbc.pooling = False
