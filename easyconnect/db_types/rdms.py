from typing import Tuple

from easyconnect.db_types.base import DBConnection, Schema, Table, Column


class MYSQL(DBConnection):
    def get_schemas(self) -> Tuple[Schema]:
        schemas = {}
        for row in self.fetchall("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY, EXTRA FROM information_schema.columns WHERE TABLE_SCHEMA NOT IN (%s, %s, %s, %s, %s) AND TABLE_SCHEMA NOT LIKE %s ORDER BY TABLE_SCHEMA, TABLE_NAME", ('information_schema', 'phpmyadmin', 'mysql', 'performance_schema', 'sys', 'phabricator%')):
            try:
                schema = schemas[row['TABLE_SCHEMA'].lower()]
            except KeyError:
                schema = schemas[row['TABLE_SCHEMA'].lower()] = {'NAME': row['TABLE_SCHEMA']}
            try:
                table = schema[row['TABLE_NAME'].lower()]
            except KeyError:
                table = schema[row['TABLE_NAME'].lower()] = [row['TABLE_NAME']]
            table.append(Column(
                name=row['COLUMN_NAME'],
                position=row['ORDINAL_POSITION'],
                type=row['DATA_TYPE'],
                max_length=row['CHARACTER_MAXIMUM_LENGTH'] or None,
                default=row['COLUMN_DEFAULT'],
                nullable=row['IS_NULLABLE'] == 'YES',
                primary_key=row['COLUMN_KEY'] == 'PRI',
                auto_increment=row['EXTRA'] == 'auto_increment',
            ))
        db_list = []
        s_name = ''
        for schema in schemas.values():
            if isinstance(schema, str):
                s_name = schema
                continue
            table_list = tuple(Table(table[0], tuple(table[1:])) for table in schema)
            db_list.append(Schema(s_name, table_list))
        return tuple(db_list)


class MSSQL(DBConnection):
    def get_schemas(self) -> Tuple[Schema]:
        schemas = {}
        for db in self.fetchall('SELECT "name" FROM master.dbo.sysdatabases WHERE "name" NOT IN (?, ?, ?, ?) AND "name" NOT LIKE ? ORDER BY "name"', ('master', 'tempdb', 'model', 'msdb', 'ReportServer%')):
            db = f'"{db["name"]}"'
            for row in self.fetchall(f'SELECT c.TABLE_CATALOG, c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION, c.COLUMN_DEFAULT, c.IS_NULLABLE, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, tc.CONSTRAINT_TYPE, ic.is_identity FROM {db}.information_schema.COLUMNS c LEFT JOIN {db}.information_schema.KEY_COLUMN_USAGE kcu ON c.TABLE_CATALOG=kcu.TABLE_CATALOG AND c.TABLE_NAME=kcu.TABLE_NAME AND c.COLUMN_NAME=kcu.COLUMN_NAME LEFT JOIN {db}.information_schema.TABLE_CONSTRAINTS tc ON kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME LEFT JOIN {db}.sys.tables t ON t.name=c.TABLE_NAME LEFT JOIN {db}.sys.identity_columns ic ON t.object_id=ic.object_id AND ic.name=c.COLUMN_NAME ORDER BY c.TABLE_CATALOG, c.TABLE_NAME'):
                try:
                    schema = schemas[row['TABLE_CATALOG'].lower()]
                except KeyError:
                    schema = schemas[row['TABLE_CATALOG'].lower()] = {'NAME': row['TABLE_CATALOG']}
                try:
                    table = schema[row['TABLE_NAME'].lower()]
                except KeyError:
                    table = schema[row['TABLE_NAME'].lower()] = [row['TABLE_NAME']]
                table.append(Column(
                    name=row['COLUMN_NAME'],
                    position=row['ORDINAL_POSITION'],
                    type=row['DATA_TYPE'],
                    max_length=row['CHARACTER_MAXIMUM_LENGTH'] or None,
                    default=row['COLUMN_DEFAULT'],
                    nullable=row['IS_NULLABLE'] == 'YES',
                    primary_key=row['CONSTRAINT_TYPE'] == 'PRIMARY KEY',
                    auto_increment=row['is_identity'] or False,
                ))
        for schema in schemas.values():
            for table in schema:
                if table == 'NAME':
                    continue
                schema[table] = {k: {a: b for a, b in v.items() if a != 'position'} if k != 'NAME' else v for k, v in sorted(schema[table].items(), key=lambda d: d[1]['position'] if d[0] != 'NAME' else 999)}
        return schemas
