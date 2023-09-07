from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict, Tuple


@dataclass
class DBInfo:
    host: str
    port: int
    database: Optional[str]
    username: str
    password: str


class Row(dict):
    __setattr__ = dict.__setitem__
    __getattribute__ = dict.__getitem__


@dataclass(frozen=True)
class Column:
    name: str
    position: int
    type: str
    max_length: Optional[int]
    default: Optional[Any]
    nullable: bool
    primary_key: bool
    auto_increment: bool


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column] = field(default_factory=tuple)


@dataclass(frozen=True)
class Schema:
    name: str
    tables: tuple[Table] = field(default_factory=tuple)


class DBConnection(ABC):
    def __init__(self, connection):
        self._connection = connection

    @abstractmethod
    def execute(self) -> None:
        pass

    @abstractmethod
    def fetch(self) -> Row[str, Any]:
        pass

    @abstractmethod
    def fetchall(self) -> Tuple[Row[str, Any]]:
        pass

    @abstractmethod
    def get_schemas(self) -> Tuple[Schema]:
        pass


class DBPool:
    def __init__(self, db_info: DBInfo, connect_method, connect_method_mapping: Dict[str, str]):
        self._in_use: List[DBConnection] = []
        self._free: List[DBConnection] = []
        self.db_info = db_info
        self.connect_method = connect_method
        self.connect_method_mapping = connect_method_mapping

    @contextmanager
    def connection(self) -> DBConnection:
        if self._free:
            connection = self._free.pop()
        else:
            connection = self.connect_method(**{self.connect_method_mapping.get(item, item): value for item, value in self.db_info.__dict__.items()})
        self._in_use.append(connection)
        yield connection
        self._in_use.remove(connection)
        self._free.append(connection)
