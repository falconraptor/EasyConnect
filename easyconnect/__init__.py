from datetime import datetime
from threading import Thread
from typing import Iterable

from easyconnect.db_pool import MSSQL, MYSQL, SERVERS
from easyconnect.types import pypyodbc

__all__ = ['MSSQL', 'MYSQL', 'SERVERS', 'map_dbs']


def map_dbs(classes: Iterable, wait: bool = False, override: bool = False):
    def get(c):
        start = datetime.now()
        c.mapping = SERVERS[getattr(c, "__name__", c._pool.connect.keywords['host']).lower()] = (c.get_databases(), c)
        print(f'[SERVER] {getattr(c, "__name__", c._pool.connect.keywords["host"])}', datetime.now() - start)

    if not override and SERVERS:
        return
    threads = [Thread(target=get, args=(c,)) for c in classes if isinstance(c, (MSSQL, MYSQL)) or (type(c) == type and issubclass(c, (MSSQL, MYSQL)))]
    [thread.start() for thread in threads]
    if wait:
        [thread.join() for thread in threads]


pypyodbc.pooling = False
