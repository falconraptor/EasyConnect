from typing import Any


class Server:
    def __init__(self, name: str, pool):
        self.name = name
        self.pool = pool
        self.schemas = {}

    def __getattr__(self, item):
        return self.schemas[item.lower()]


class Schema:
    def __init__(self, name: str, server: Server):
        self.name = name
        self.server = server
        self.tables = {}
        self.server.schemas[name.lower()] = self

    def __getattr__(self, item):
        return self.tables[item.lower()]


class Table:
    def __init__(self, name: str, schema: Schema):
        self.name = name
        self.schema = schema
        self.columns = {}
        self.schema.tables[name.lower()] = self

    def __getattr__(self, item):
        return self.columns[item.lower()]


class Column:
    def __init__(self, name: str, table: Table, type: str, primary_key: bool = False, auto_increment: bool = False, default: Any = None, nullable: bool = True):
        self.name = name
        self.table = table
        self.auto_inc = auto_increment
        self.primary_key = primary_key
        self.type = type
        self.default = default
        self.nullable = nullable
        self.max_length = -1
        self.table.columns[name.lower()] = self
        self.position = len(self.table.columns)
