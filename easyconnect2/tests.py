from random import randint

from easyconnect2.sqlite import SQLite


def test_sqlite():
    db = SQLite('test.db')
    with db as conn:
        with conn.cursor() as cursor:
            assert cursor.fetchone('SELECT * FROM sqlite_master') == {}
        conn.execute('CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT)')
        ran = randint(2, 100)
        with conn.cursor() as cursor:
            cursor.executemany('INSERT INTO test (id)', [None] * ran)
            assert cursor.fetchone('SELECT * FROM sqlite_sequence') == {'name': 'test', 'seq': ran}
            cursor.execute('DROP TABLE test')


if __name__ == '__main__':
    test_sqlite()
