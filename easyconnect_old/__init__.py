from datetime import datetime
from threading import Thread
from typing import Iterable

from easyconnect_old.db_pool import MSSQL, MYSQL, SERVERS
from easyconnect_old.types import pypyodbc

__all__ = ['MSSQL', 'MYSQL', 'SERVERS', 'map_dbs']


def map_dbs(classes: Iterable, wait: bool = False, override: bool = False):
    def get(c):
        start = datetime.now()
        c.mapping = SERVERS[c.__name__.lower()] = (c.get_schemas(), c)
        print(f'[SERVER] {c.__name__}:', datetime.now() - start)

    if not override and SERVERS:
        return
    threads = [Thread(target=get, args=(c,)) for c in classes if type(c) == type and issubclass(c, (MSSQL, MYSQL))]
    [thread.start() for thread in threads]
    if wait:
        [thread.join() for thread in threads]


pypyodbc.pooling = False
