from os import remove
from random import randint

from easyconnect2.sqlite import SQLite


def test_sqlite():
    db = SQLite('test.db')
    with db.connection() as conn:
        with conn.cursor() as cursor:
            assert cursor.fetchone('SELECT * FROM sqlite_master') == {}
        conn.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT, i INT)')
        ran = randint(2, 100)
        with conn.cursor() as cursor:
            cursor.executemany('INSERT INTO test (i) VALUES (?)', [[ran]] * ran)
            assert cursor.fetchone('SELECT * FROM sqlite_sequence') == {'name': 'test', 'seq': ran}
    db.close_all()
    remove('test.db')


if __name__ == '__main__':
    test_sqlite()
