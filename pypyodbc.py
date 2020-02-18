from _datetime import datetime, time, date
from decimal import Decimal
from typing import Iterable, Optional
import ctypes
import os
import sys
import threading

POOLING = True
CONNECTION_TIMEOUT = 0
if not hasattr(ctypes, 'c_ssize_t'):
    if ctypes.sizeof(ctypes.c_uint) == ctypes.sizeof(ctypes.c_void_p):
        ctypes.c_ssize_t = ctypes.c_int
    elif ctypes.sizeof(ctypes.c_ulong) == ctypes.sizeof(ctypes.c_void_p):
        ctypes.c_ssize_t = ctypes.c_long
    elif ctypes.sizeof(ctypes.c_ulonglong) == ctypes.sizeof(ctypes.c_void_p):
        ctypes.c_ssize_t = ctypes.c_longlong
LOCK = threading.Lock()
SHARED_ENV_H = None
SQLWCHAR_SIZE = ctypes.sizeof(ctypes.c_wchar)
# determin the size of Py_UNICODE, sys.maxunicode > 65536 and 'UCS4' or 'UCS2'
UNICODE_SIZE = sys.maxunicode > 65536 and 4 or 2
# Define ODBC constants. They are widly used in ODBC documents and programs. They are defined in cpp header files: sql.h sqlext.h sqltypes.h sqlucode.h and you can get these files from the mingw32-runtime_3.13-1_all.deb package
SQL_DRIVER_NOPROMPT = 0
SQL_NULL_HANDLE, SQL_HANDLE_ENV, SQL_HANDLE_DBC, SQL_HANDLE_STMT = 0, 1, 2, 3
SQL_SUCCESS, SQL_SUCCESS_WITH_INFO = 0, 1
SQL_NO_DATA = 100
SQL_ATTR_ACCESS_MODE = 101
SQL_MODE_READ_ONLY = 1
SQL_IS_UINTEGER = -5
SQL_NULL_DATA = -1
SQL_RESET_PARAMS = 3
SQL_UNBIND = 2
SQL_CLOSE = 0
# Below defines The constants for sqlgetinfo method, and their coresponding return types
SQL_ACCESSIBLE_PROCEDURES = 20
SQL_ACCESSIBLE_TABLES = 19
SQL_ACTIVE_ENVIRONMENTS = 116
SQL_AGGREGATE_FUNCTIONS = 169
SQL_ALTER_DOMAIN = 117
SQL_ALTER_TABLE = 86
SQL_ASYNC_MODE = 10021
SQL_BATCH_ROW_COUNT = 120
SQL_BATCH_SUPPORT = 121
SQL_BOOKMARK_PERSISTENCE = 82
SQL_CATALOG_LOCATION = 114
SQL_CATALOG_NAME = 10003
SQL_CATALOG_NAME_SEPARATOR = 41
SQL_CATALOG_TERM = 42
SQL_CATALOG_USAGE = 92
SQL_COLLATION_SEQ = 10004
SQL_COLUMN_ALIAS = 87
SQL_CONCAT_NULL_BEHAVIOR = 22
SQL_CONVERT_FUNCTIONS = 48
SQL_CONVERT_VARCHAR = 70
SQL_CORRELATION_NAME = 74
SQL_CREATE_ASSERTION = 127
SQL_CREATE_CHARACTER_SET = 128
SQL_CREATE_COLLATION = 129
SQL_CREATE_DOMAIN = 130
SQL_CREATE_SCHEMA = 131
SQL_CREATE_TABLE = 132
SQL_CREATE_TRANSLATION = 133
SQL_CREATE_VIEW = 134
SQL_CURSOR_COMMIT_BEHAVIOR = 23
SQL_CURSOR_ROLLBACK_BEHAVIOR = 24
SQL_DATABASE_NAME = 16
SQL_DATA_SOURCE_NAME = 2
SQL_DATA_SOURCE_READ_ONLY = 25
SQL_DATETIME_LITERALS = 119
SQL_DBMS_NAME = 17
SQL_DBMS_VER = 18
SQL_DDL_INDEX = 170
SQL_DEFAULT_TXN_ISOLATION = 26
SQL_DESCRIBE_PARAMETER = 10002
SQL_DM_VER = 171
SQL_DRIVER_NAME = 6
SQL_DRIVER_ODBC_VER = 77
SQL_DRIVER_VER = 7
SQL_DROP_ASSERTION = 136
SQL_DROP_CHARACTER_SET = 137
SQL_DROP_COLLATION = 138
SQL_DROP_DOMAIN = 139
SQL_DROP_SCHEMA = 140
SQL_DROP_TABLE = 141
SQL_DROP_TRANSLATION = 142
SQL_DROP_VIEW = 143
SQL_DYNAMIC_CURSOR_ATTRIBUTES1 = 144
SQL_DYNAMIC_CURSOR_ATTRIBUTES2 = 145
SQL_EXPRESSIONS_IN_ORDERBY = 27
SQL_FILE_USAGE = 84
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1 = 146
SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2 = 147
SQL_GETDATA_EXTENSIONS = 81
SQL_GROUP_BY = 88
SQL_IDENTIFIER_CASE = 28
SQL_IDENTIFIER_QUOTE_CHAR = 29
SQL_INDEX_KEYWORDS = 148
SQL_INFO_SCHEMA_VIEWS = 149
SQL_INSERT_STATEMENT = 172
SQL_INTEGRITY = 73
SQL_KEYSET_CURSOR_ATTRIBUTES1 = 150
SQL_KEYSET_CURSOR_ATTRIBUTES2 = 151
SQL_KEYWORDS = 89
SQL_LIKE_ESCAPE_CLAUSE = 113
SQL_MAX_ASYNC_CONCURRENT_STATEMENTS = 10022
SQL_MAX_BINARY_LITERAL_LEN = 112
SQL_MAX_CATALOG_NAME_LEN = 34
SQL_MAX_CHAR_LITERAL_LEN = 108
SQL_MAX_COLUMNS_IN_GROUP_BY = 97
SQL_MAX_COLUMNS_IN_INDEX = 98
SQL_MAX_COLUMNS_IN_ORDER_BY = 99
SQL_MAX_COLUMNS_IN_SELECT = 100
SQL_MAX_COLUMNS_IN_TABLE = 101
SQL_MAX_COLUMN_NAME_LEN = 30
SQL_MAX_CONCURRENT_ACTIVITIES = 1
SQL_MAX_CURSOR_NAME_LEN = 31
SQL_MAX_DRIVER_CONNECTIONS = 0
SQL_MAX_IDENTIFIER_LEN = 10005
SQL_MAX_INDEX_SIZE = 102
SQL_MAX_PROCEDURE_NAME_LEN = 33
SQL_MAX_ROW_SIZE = 104
SQL_MAX_ROW_SIZE_INCLUDES_LONG = 103
SQL_MAX_SCHEMA_NAME_LEN = 32
SQL_MAX_STATEMENT_LEN = 105
SQL_MAX_TABLES_IN_SELECT = 106
SQL_MAX_TABLE_NAME_LEN = 35
SQL_MAX_USER_NAME_LEN = 107
SQL_MULTIPLE_ACTIVE_TXN = 37
SQL_MULT_RESULT_SETS = 36
SQL_NEED_LONG_DATA_LEN = 111
SQL_NON_NULLABLE_COLUMNS = 75
SQL_NULL_COLLATION = 85
SQL_NUMERIC_FUNCTIONS = 49
SQL_ODBC_INTERFACE_CONFORMANCE = 152
SQL_ODBC_VER = 10
SQL_OJ_CAPABILITIES = 65003
SQL_ORDER_BY_COLUMNS_IN_SELECT = 90
SQL_PARAM_ARRAY_ROW_COUNTS = 153
SQL_PARAM_ARRAY_SELECTS = 154
SQL_PROCEDURES = 21
SQL_PROCEDURE_TERM = 40
SQL_QUOTED_IDENTIFIER_CASE = 93
SQL_ROW_UPDATES = 11
SQL_SCHEMA_TERM = 39
SQL_SCHEMA_USAGE = 91
SQL_SCROLL_OPTIONS = 44
SQL_SEARCH_PATTERN_ESCAPE = 14
SQL_SERVER_NAME = 13
SQL_SPECIAL_CHARACTERS = 94
SQL_SQL92_DATETIME_FUNCTIONS = 155
SQL_SQL92_FOREIGN_KEY_DELETE_RULE = 156
SQL_SQL92_FOREIGN_KEY_UPDATE_RULE = 157
SQL_SQL92_GRANT = 158
SQL_SQL92_NUMERIC_VALUE_FUNCTIONS = 159
SQL_SQL92_PREDICATES = 160
SQL_SQL92_RELATIONAL_JOIN_OPERATORS = 161
SQL_SQL92_REVOKE = 162
SQL_SQL92_ROW_VALUE_CONSTRUCTOR = 163
SQL_SQL92_STRING_FUNCTIONS = 164
SQL_SQL92_VALUE_EXPRESSIONS = 165
SQL_SQL_CONFORMANCE = 118
SQL_STANDARD_CLI_CONFORMANCE = 166
SQL_STATIC_CURSOR_ATTRIBUTES1 = 167
SQL_STATIC_CURSOR_ATTRIBUTES2 = 168
SQL_STRING_FUNCTIONS = 50
SQL_SUBQUERIES = 95
SQL_SYSTEM_FUNCTIONS = 51
SQL_TABLE_TERM = 45
SQL_TIMEDATE_ADD_INTERVALS = 109
SQL_TIMEDATE_DIFF_INTERVALS = 110
SQL_TIMEDATE_FUNCTIONS = 52
SQL_TXN_CAPABLE = 46
SQL_TXN_ISOLATION_OPTION = 72
SQL_UNION = 96
SQL_USER_NAME = 47
SQL_XOPEN_CLI_YEAR = 10000
A_INFO_TYPES = {
    SQL_ACCESSIBLE_PROCEDURES: 'GI_YESNO', SQL_ACCESSIBLE_TABLES: 'GI_YESNO', SQL_ACTIVE_ENVIRONMENTS: 'GI_USMALLINT',
    SQL_AGGREGATE_FUNCTIONS: 'GI_UINTEGER', SQL_ALTER_DOMAIN: 'GI_UINTEGER',
    SQL_ALTER_TABLE: 'GI_UINTEGER', SQL_ASYNC_MODE: 'GI_UINTEGER', SQL_BATCH_ROW_COUNT: 'GI_UINTEGER',
    SQL_BATCH_SUPPORT: 'GI_UINTEGER', SQL_BOOKMARK_PERSISTENCE: 'GI_UINTEGER', SQL_CATALOG_LOCATION: 'GI_USMALLINT',
    SQL_CATALOG_NAME: 'GI_YESNO', SQL_CATALOG_NAME_SEPARATOR: 'GI_STRING', SQL_CATALOG_TERM: 'GI_STRING',
    SQL_CATALOG_USAGE: 'GI_UINTEGER', SQL_COLLATION_SEQ: 'GI_STRING', SQL_COLUMN_ALIAS: 'GI_YESNO',
    SQL_CONCAT_NULL_BEHAVIOR: 'GI_USMALLINT', SQL_CONVERT_FUNCTIONS: 'GI_UINTEGER', SQL_CONVERT_VARCHAR: 'GI_UINTEGER',
    SQL_CORRELATION_NAME: 'GI_USMALLINT', SQL_CREATE_ASSERTION: 'GI_UINTEGER', SQL_CREATE_CHARACTER_SET: 'GI_UINTEGER',
    SQL_CREATE_COLLATION: 'GI_UINTEGER', SQL_CREATE_DOMAIN: 'GI_UINTEGER', SQL_CREATE_SCHEMA: 'GI_UINTEGER',
    SQL_CREATE_TABLE: 'GI_UINTEGER', SQL_CREATE_TRANSLATION: 'GI_UINTEGER', SQL_CREATE_VIEW: 'GI_UINTEGER',
    SQL_CURSOR_COMMIT_BEHAVIOR: 'GI_USMALLINT', SQL_CURSOR_ROLLBACK_BEHAVIOR: 'GI_USMALLINT', SQL_DATABASE_NAME: 'GI_STRING',
    SQL_DATA_SOURCE_NAME: 'GI_STRING', SQL_DATA_SOURCE_READ_ONLY: 'GI_YESNO', SQL_DATETIME_LITERALS: 'GI_UINTEGER',
    SQL_DBMS_NAME: 'GI_STRING', SQL_DBMS_VER: 'GI_STRING', SQL_DDL_INDEX: 'GI_UINTEGER',
    SQL_DEFAULT_TXN_ISOLATION: 'GI_UINTEGER', SQL_DESCRIBE_PARAMETER: 'GI_YESNO', SQL_DM_VER: 'GI_STRING',
    SQL_DRIVER_NAME: 'GI_STRING', SQL_DRIVER_ODBC_VER: 'GI_STRING', SQL_DRIVER_VER: 'GI_STRING', SQL_DROP_ASSERTION: 'GI_UINTEGER',
    SQL_DROP_CHARACTER_SET: 'GI_UINTEGER', SQL_DROP_COLLATION: 'GI_UINTEGER', SQL_DROP_DOMAIN: 'GI_UINTEGER',
    SQL_DROP_SCHEMA: 'GI_UINTEGER', SQL_DROP_TABLE: 'GI_UINTEGER', SQL_DROP_TRANSLATION: 'GI_UINTEGER',
    SQL_DROP_VIEW: 'GI_UINTEGER', SQL_DYNAMIC_CURSOR_ATTRIBUTES1: 'GI_UINTEGER', SQL_DYNAMIC_CURSOR_ATTRIBUTES2: 'GI_UINTEGER',
    SQL_EXPRESSIONS_IN_ORDERBY: 'GI_YESNO', SQL_FILE_USAGE: 'GI_USMALLINT',
    SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: 'GI_UINTEGER', SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: 'GI_UINTEGER',
    SQL_GETDATA_EXTENSIONS: 'GI_UINTEGER', SQL_GROUP_BY: 'GI_USMALLINT', SQL_IDENTIFIER_CASE: 'GI_USMALLINT',
    SQL_IDENTIFIER_QUOTE_CHAR: 'GI_STRING', SQL_INDEX_KEYWORDS: 'GI_UINTEGER', SQL_INFO_SCHEMA_VIEWS: 'GI_UINTEGER',
    SQL_INSERT_STATEMENT: 'GI_UINTEGER', SQL_INTEGRITY: 'GI_YESNO', SQL_KEYSET_CURSOR_ATTRIBUTES1: 'GI_UINTEGER',
    SQL_KEYSET_CURSOR_ATTRIBUTES2: 'GI_UINTEGER', SQL_KEYWORDS: 'GI_STRING',
    SQL_LIKE_ESCAPE_CLAUSE: 'GI_YESNO', SQL_MAX_ASYNC_CONCURRENT_STATEMENTS: 'GI_UINTEGER',
    SQL_MAX_BINARY_LITERAL_LEN: 'GI_UINTEGER', SQL_MAX_CATALOG_NAME_LEN: 'GI_USMALLINT',
    SQL_MAX_CHAR_LITERAL_LEN: 'GI_UINTEGER', SQL_MAX_COLUMNS_IN_GROUP_BY: 'GI_USMALLINT',
    SQL_MAX_COLUMNS_IN_INDEX: 'GI_USMALLINT', SQL_MAX_COLUMNS_IN_ORDER_BY: 'GI_USMALLINT',
    SQL_MAX_COLUMNS_IN_SELECT: 'GI_USMALLINT', SQL_MAX_COLUMNS_IN_TABLE: 'GI_USMALLINT',
    SQL_MAX_COLUMN_NAME_LEN: 'GI_USMALLINT', SQL_MAX_CONCURRENT_ACTIVITIES: 'GI_USMALLINT',
    SQL_MAX_CURSOR_NAME_LEN: 'GI_USMALLINT', SQL_MAX_DRIVER_CONNECTIONS: 'GI_USMALLINT',
    SQL_MAX_IDENTIFIER_LEN: 'GI_USMALLINT', SQL_MAX_INDEX_SIZE: 'GI_UINTEGER',
    SQL_MAX_PROCEDURE_NAME_LEN: 'GI_USMALLINT', SQL_MAX_ROW_SIZE: 'GI_UINTEGER',
    SQL_MAX_ROW_SIZE_INCLUDES_LONG: 'GI_YESNO', SQL_MAX_SCHEMA_NAME_LEN: 'GI_USMALLINT',
    SQL_MAX_STATEMENT_LEN: 'GI_UINTEGER', SQL_MAX_TABLES_IN_SELECT: 'GI_USMALLINT',
    SQL_MAX_TABLE_NAME_LEN: 'GI_USMALLINT', SQL_MAX_USER_NAME_LEN: 'GI_USMALLINT',
    SQL_MULTIPLE_ACTIVE_TXN: 'GI_YESNO', SQL_MULT_RESULT_SETS: 'GI_YESNO',
    SQL_NEED_LONG_DATA_LEN: 'GI_YESNO', SQL_NON_NULLABLE_COLUMNS: 'GI_USMALLINT',
    SQL_NULL_COLLATION: 'GI_USMALLINT', SQL_NUMERIC_FUNCTIONS: 'GI_UINTEGER',
    SQL_ODBC_INTERFACE_CONFORMANCE: 'GI_UINTEGER', SQL_ODBC_VER: 'GI_STRING', SQL_OJ_CAPABILITIES: 'GI_UINTEGER',
    SQL_ORDER_BY_COLUMNS_IN_SELECT: 'GI_YESNO', SQL_PARAM_ARRAY_ROW_COUNTS: 'GI_UINTEGER',
    SQL_PARAM_ARRAY_SELECTS: 'GI_UINTEGER', SQL_PROCEDURES: 'GI_YESNO', SQL_PROCEDURE_TERM: 'GI_STRING',
    SQL_QUOTED_IDENTIFIER_CASE: 'GI_USMALLINT', SQL_ROW_UPDATES: 'GI_YESNO', SQL_SCHEMA_TERM: 'GI_STRING',
    SQL_SCHEMA_USAGE: 'GI_UINTEGER', SQL_SCROLL_OPTIONS: 'GI_UINTEGER', SQL_SEARCH_PATTERN_ESCAPE: 'GI_STRING',
    SQL_SERVER_NAME: 'GI_STRING', SQL_SPECIAL_CHARACTERS: 'GI_STRING', SQL_SQL92_DATETIME_FUNCTIONS: 'GI_UINTEGER',
    SQL_SQL92_FOREIGN_KEY_DELETE_RULE: 'GI_UINTEGER', SQL_SQL92_FOREIGN_KEY_UPDATE_RULE: 'GI_UINTEGER',
    SQL_SQL92_GRANT: 'GI_UINTEGER', SQL_SQL92_NUMERIC_VALUE_FUNCTIONS: 'GI_UINTEGER',
    SQL_SQL92_PREDICATES: 'GI_UINTEGER', SQL_SQL92_RELATIONAL_JOIN_OPERATORS: 'GI_UINTEGER',
    SQL_SQL92_REVOKE: 'GI_UINTEGER', SQL_SQL92_ROW_VALUE_CONSTRUCTOR: 'GI_UINTEGER',
    SQL_SQL92_STRING_FUNCTIONS: 'GI_UINTEGER', SQL_SQL92_VALUE_EXPRESSIONS: 'GI_UINTEGER',
    SQL_SQL_CONFORMANCE: 'GI_UINTEGER', SQL_STANDARD_CLI_CONFORMANCE: 'GI_UINTEGER',
    SQL_STATIC_CURSOR_ATTRIBUTES1: 'GI_UINTEGER', SQL_STATIC_CURSOR_ATTRIBUTES2: 'GI_UINTEGER',
    SQL_STRING_FUNCTIONS: 'GI_UINTEGER', SQL_SUBQUERIES: 'GI_UINTEGER',
    SQL_SYSTEM_FUNCTIONS: 'GI_UINTEGER', SQL_TABLE_TERM: 'GI_STRING', SQL_TIMEDATE_ADD_INTERVALS: 'GI_UINTEGER',
    SQL_TIMEDATE_DIFF_INTERVALS: 'GI_UINTEGER', SQL_TIMEDATE_FUNCTIONS: 'GI_UINTEGER',
    SQL_TXN_CAPABLE: 'GI_USMALLINT', SQL_TXN_ISOLATION_OPTION: 'GI_UINTEGER',
    SQL_UNION: 'GI_UINTEGER', SQL_USER_NAME: 'GI_STRING', SQL_XOPEN_CLI_YEAR: 'GI_STRING',
}


# Define exceptions
class PyPyOdbcException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class OdbcNoLibrary(PyPyOdbcException):
    pass


class OdbcLibraryError(PyPyOdbcException):
    pass


class Error(Exception):
    def __init__(self, error_code, error_desc):
        self.value = (error_code, error_desc)
        self.args = (error_code, error_desc)


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class ProgrammingError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class NotSupportedError(Error):
    pass


class OperationalError(DatabaseError):
    pass


# Find the ODBC library on the platform and connect to it using ctypes
# Get the References of the platform's ODBC functions via ctypes
ODBC_DECODING = 'utf_16'
ODBC_ENCODING = 'utf_16_le'
UCS_LENGTH = 2
if sys.platform in {'win32', 'cli'}:
    ODBC_API = ctypes.windll.odbc32
    # On Windows, the size of SQLWCHAR is hardcoded to 2-bytes.
    SQLWCHAR_SIZE = ctypes.sizeof(ctypes.c_ushort)
else:
    # Set load the library on linux
    try:
        # First try direct loading libodbc.so
        ODBC_API = ctypes.cdll.LoadLibrary('libodbc.so')
    except Exception as e:
        print(e.__repr__())
        # If direct loading libodbc.so failed
        # We try finding the libodbc.so by using find_library
        from ctypes.util import find_library

        library = find_library('odbc')
        if library is None:
            # If find_library still can not find the library we try finding it manually from where libodbc.so usually appears
            lib_paths = [path for path in ("/usr/lib/libodbc.so", "/usr/lib/i386-linux-gnu/libodbc.so", "/usr/lib/x86_64-linux-gnu/libodbc.so", "/usr/lib/libiodbc.dylib") if os.path.exists(path)]
            if len(lib_paths) == 0:
                raise OdbcNoLibrary('ODBC Library is not found. Is LD_LIBRARY_PATH set?')
            else:
                library = lib_paths[0]
        # Then we try loading the found libodbc.so again
        try:
            ODBC_API = ctypes.cdll.LoadLibrary(library)
        except Exception as e:
            print(e.__repr__())
            # If still fail loading, abort.
            raise OdbcLibraryError('Error while loading ' + library)
        # only iODBC uses utf-32 / UCS4 encoding data, others normally use utf-16 / UCS2
        # So we set those for handling.
        if 'libiodbc.dylib' in library:
            ODBC_DECODING = 'utf_32'
            ODBC_ENCODING = 'utf_32_le'
            UCS_LENGTH = 4
        del library
    # unixODBC defaults to 2-bytes SQLWCHAR, unless "-DSQL_WCHART_CONVERT" was added to CFLAGS, in which case it will be the size of wchar_t.
    # Note that using 4-bytes SQLWCHAR will break most ODBC drivers, as driver development mostly targets the Windows platform.
    from subprocess import getstatusoutput
    status, output = getstatusoutput('odbc_config --cflags')
    SQLWCHAR_SIZE = ctypes.sizeof(ctypes.c_wchar if not status and 'SQL_WCHART_CONVERT' in output else ctypes.c_ushort)
    del status, output
CREATE_BUFFER_U = ctypes.create_unicode_buffer
CREATE_BUFFER = ctypes.create_string_buffer
WCHAR_POINTER = ctypes.c_wchar_p
UCS_BUF = lambda s: s


def ucs_dec(buffer) -> str:
    return buffer.raw.decode(ODBC_ENCODING).split('\x00')[0]


FROM_BUFFER_U = lambda buffer: buffer.value
# This is the common case on Linux, which uses wide Python build together with the default unixODBC without the "-DSQL_WCHART_CONVERT" CFLAGS.
if sys.platform not in {'win32', 'cli', 'cygwin'}:
    if UNICODE_SIZE >= SQLWCHAR_SIZE:
        # We can only use unicode buffer if the size of wchar_t (UNICODE_SIZE) is the same as the size expected by the driver manager (SQLWCHAR_SIZE).
        CREATE_BUFFER_U = CREATE_BUFFER
        WCHAR_POINTER = ctypes.c_char_p
        UCS_BUF = lambda s: s.encode(ODBC_ENCODING)
        FROM_BUFFER_U = ucs_dec
    # Exoteric case, don't really care.
    elif UNICODE_SIZE < SQLWCHAR_SIZE:
        raise OdbcLibraryError('Using narrow Python build with ODBC library expecting wide unicode is not supported.')
# Database value to Python data type mappings
SQL_TYPE_NULL = 0
SQL_DECIMAL = 3
SQL_FLOAT = 6
SQL_DATE = 9
SQL_TIME = 10
SQL_TIMESTAMP = 11
SQL_VARCHAR = 12
SQL_LONGVARCHAR = -1
SQL_VARBINARY = -3
SQL_LONGVARBINARY = -4
SQL_BIGINT = -5
SQL_WVARCHAR = -9
SQL_WLONGVARCHAR = -10
SQL_SS_VARIANT = -150
SQL_SS_UDT = -151
SQL_SS_XML = -152
SQL_SS_TIME2 = -154
SQL_C_CHAR = SQL_CHAR = 1
SQL_NUMERIC = 2
SQL_INTEGER = 4
SQL_SMALLINT = 5
SQL_REAL = 7
SQL_DOUBLE = 8
SQL_TYPE_DATE = 91
SQL_TYPE_TIME = 92
SQL_C_BINARY = SQL_BINARY = -2
SQL_TINYINT = -6
SQL_BIT = -7
SQL_C_WCHAR = SQL_WCHAR = -8
SQL_GUID = -11
SQL_TYPE_TIMESTAMP = 93


def dttm_cvt(x) -> Optional[datetime]:
    x = x.decode('ascii')
    if x == '':
        return None
    return datetime(int(x[0:4]), int(x[5:7]), int(x[8:10]), int(x[10:13]), int(x[14:16]), int(x[17:19]), int(x[20:26].ljust(6, '0')))


def tm_cvt(x) -> Optional[time]:
    x = x.decode('ascii')
    if x == '':
        return None
    return time(int(x[0:2]), int(x[3:5]), int(x[6:8]), int(x[9:].ljust(6, '0')))


def dt_cvt(x) -> Optional[date]:
    x = x.decode('ascii')
    if x == '':
        return None
    return date(int(x[0:4]), int(x[5:7]), int(x[8:10]))


def decimal_cvt(x) -> Decimal:
    return Decimal(x.decode('ascii'))


BYTEARRAY_CVT = bytearray
if sys.platform == 'cli':
    BYTEARRAY_CVT = lambda x: bytearray(memoryview(x))
# Below Datatype mappings referenced the document at
# http://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.help.sdk_12.5.1.aseodbc/html/aseodbc/CACFDIGH.htm
SQL_DATA_TYPE_DICT = {
    # SQL Data TYPE        0.Python Data Type     1.Default Output Converter  2.Buffer Type     3.Buffer Allocator   4.Default Size  5.Variable Length
    SQL_TYPE_NULL: (None, lambda x: None, SQL_C_CHAR, CREATE_BUFFER, 2, False),
    SQL_CHAR: (str, lambda x: x, SQL_C_CHAR, CREATE_BUFFER, 2048, False),
    SQL_NUMERIC: (Decimal, decimal_cvt, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_DECIMAL: (Decimal, decimal_cvt, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_INTEGER: (int, int, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_SMALLINT: (int, int, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_FLOAT: (float, float, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_REAL: (float, float, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_DOUBLE: (float, float, SQL_C_CHAR, CREATE_BUFFER, 200, False),
    SQL_DATE: (date, dt_cvt, SQL_C_CHAR, CREATE_BUFFER, 30, False),
    SQL_TIME: (time, tm_cvt, SQL_C_CHAR, CREATE_BUFFER, 20, False),
    SQL_SS_TIME2: (time, tm_cvt, SQL_C_CHAR, CREATE_BUFFER, 20, False),
    SQL_TIMESTAMP: (datetime, dttm_cvt, SQL_C_CHAR, CREATE_BUFFER, 30, False),
    SQL_VARCHAR: (str, lambda x: x, SQL_C_CHAR, CREATE_BUFFER, 2048, True),
    SQL_LONGVARCHAR: (str, lambda x: x, SQL_C_CHAR, CREATE_BUFFER, 20500, True),
    SQL_BINARY: (bytearray, BYTEARRAY_CVT, SQL_C_BINARY, CREATE_BUFFER, 5120, True),
    SQL_VARBINARY: (bytearray, BYTEARRAY_CVT, SQL_C_BINARY, CREATE_BUFFER, 5120, True),
    SQL_LONGVARBINARY: (bytearray, BYTEARRAY_CVT, SQL_C_BINARY, CREATE_BUFFER, 20500, True),
    SQL_BIGINT: (int, int, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_TINYINT: (int, int, SQL_C_CHAR, CREATE_BUFFER, 150, False),
    SQL_BIT: (bool, lambda x: x == b'1', SQL_C_CHAR, CREATE_BUFFER, 2, False),
    SQL_WCHAR: (str, lambda x: x, SQL_C_WCHAR, CREATE_BUFFER_U, 2048, False),
    SQL_WVARCHAR: (str, lambda x: x, SQL_C_WCHAR, CREATE_BUFFER_U, 2048, True),
    SQL_GUID: (str, lambda x: x, SQL_C_CHAR, CREATE_BUFFER, 2048, False),
    SQL_WLONGVARCHAR: (str, lambda x: x, SQL_C_WCHAR, CREATE_BUFFER_U, 20500, True),
    SQL_TYPE_DATE: (date, dt_cvt, SQL_C_CHAR, CREATE_BUFFER, 30, False),
    SQL_TYPE_TIME: (time, tm_cvt, SQL_C_CHAR, CREATE_BUFFER, 20, False),
    SQL_TYPE_TIMESTAMP: (datetime, dttm_cvt, SQL_C_CHAR, CREATE_BUFFER, 30, False),
    SQL_SS_VARIANT: (str, lambda x: x, SQL_C_CHAR, CREATE_BUFFER, 2048, True),
    SQL_SS_XML: (str, lambda x: x, SQL_C_WCHAR, CREATE_BUFFER_U, 20500, True),
    SQL_SS_UDT: (bytearray, BYTEARRAY_CVT, SQL_C_BINARY, CREATE_BUFFER, 5120, True),
}
"""
Types mapping, applicable for 32-bit and 64-bit Linux / Windows / Mac OS X.
SQLPointer -> ctypes.c_void_p
SQLCHAR * -> ctypes.c_char_p
SQLWCHAR * -> ctypes.c_wchar_p on Windows, ctypes.c_char_p with unixODBC
SQLINT -> ctypes.c_int
SQLSMALLINT -> ctypes.c_short
SQMUSMALLINT -> ctypes.c_ushort
SQLLEN -> ctypes.c_ssize_t
SQLULEN -> ctypes.c_size_t
SQLRETURN -> ctypes.c_short
"""
# Define the python return type for ODBC functions with ret result.
for func_name in ("SQLAllocHandle", "SQLBindParameter", "SQLBindCol", "SQLCloseCursor", "SQLColAttribute", "SQLColumns", "SQLColumnsW", "SQLConnect", "SQLConnectW", "SQLDataSources", "SQLDataSourcesW", "SQLDescribeCol", "SQLDescribeColW", "SQLDescribeParam", "SQLDisconnect", "SQLDriverConnect", "SQLDriverConnectW", "SQLDrivers", "SQLDriversW", "SQLEndTran", "SQLExecDirect", "SQLExecDirectW", "SQLExecute", "SQLFetch", "SQLFetchScroll", "SQLForeignKeys", "SQLForeignKeysW", "SQLFreeHandle", "SQLFreeStmt", "SQLGetData", "SQLGetDiagRec", "SQLGetDiagRecW", "SQLGetInfo", "SQLGetInfoW", "SQLGetTypeInfo", "SQLMoreResults", "SQLNumParams", "SQLNumResultCols", "SQLPrepare", "SQLPrepareW", "SQLPrimaryKeys", "SQLPrimaryKeysW", "SQLProcedureColumns", "SQLProcedureColumnsW", "SQLProcedures", "SQLProceduresW", "SQLRowCount", "SQLSetConnectAttr", "SQLSetEnvAttr", "SQLStatistics", "SQLStatisticsW", "SQLTables", "SQLTablesW"):
    getattr(ODBC_API, func_name).restype = ctypes.c_short
if sys.platform not in 'cli':
    # Seems like the IronPython can not declare ctypes.POINTER type arguments
    ODBC_API.SQLAllocHandle.argtypes = [ctypes.c_short, ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    ODBC_API.SQLBindParameter.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.c_short, ctypes.c_short, ctypes.c_short, ctypes.c_size_t, ctypes.c_short, ctypes.c_void_p, ctypes.c_ssize_t, ctypes.POINTER(ctypes.c_ssize_t)]
    ODBC_API.SQLColAttribute.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.c_ushort, ctypes.c_void_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_ssize_t)]
    ODBC_API.SQLDataSources.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.c_char_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short), ctypes.c_char_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short)]
    ODBC_API.SQLDescribeCol.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.c_char_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_short)]
    ODBC_API.SQLDescribeParam.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_short), ctypes.POINTER(ctypes.c_short)]
    ODBC_API.SQLDriverConnect.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short), ctypes.c_ushort]
    ODBC_API.SQLDrivers.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.c_char_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short), ctypes.c_char_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short)]
    ODBC_API.SQLGetData.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.c_short, ctypes.c_void_p, ctypes.c_ssize_t, ctypes.POINTER(ctypes.c_ssize_t)]
    ODBC_API.SQLGetDiagRec.argtypes = [ctypes.c_short, ctypes.c_void_p, ctypes.c_short, ctypes.c_char_p, ctypes.POINTER(ctypes.c_int), ctypes.c_char_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short)]
    ODBC_API.SQLGetInfo.argtypes = [ctypes.c_void_p, ctypes.c_ushort, ctypes.c_void_p, ctypes.c_short, ctypes.POINTER(ctypes.c_short)]
    ODBC_API.SQLRowCount.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ssize_t)]
    ODBC_API.SQLNumParams.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_short)]
    ODBC_API.SQLNumResultCols.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_short)]
ODBC_API.SQLCloseCursor.argtypes = [ctypes.c_void_p]
ODBC_API.SQLColumns.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short]
ODBC_API.SQLConnect.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short]
ODBC_API.SQLDisconnect.argtypes = [ctypes.c_void_p]
ODBC_API.SQLEndTran.argtypes = [ctypes.c_short, ctypes.c_void_p, ctypes.c_short]
ODBC_API.SQLExecute.argtypes = [ctypes.c_void_p]
ODBC_API.SQLExecDirect.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]
ODBC_API.SQLFetch.argtypes = [ctypes.c_void_p]
ODBC_API.SQLFetchScroll.argtypes = [ctypes.c_void_p, ctypes.c_short, ctypes.c_ssize_t]
ODBC_API.SQLForeignKeys.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short]
ODBC_API.SQLFreeHandle.argtypes = [ctypes.c_short, ctypes.c_void_p]
ODBC_API.SQLFreeStmt.argtypes = [ctypes.c_void_p, ctypes.c_ushort]
ODBC_API.SQLGetTypeInfo.argtypes = [ctypes.c_void_p, ctypes.c_short]
ODBC_API.SQLMoreResults.argtypes = [ctypes.c_void_p]
ODBC_API.SQLPrepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int]
ODBC_API.SQLPrimaryKeys.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short]
ODBC_API.SQLProcedureColumns.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short]
ODBC_API.SQLProcedures.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short]
ODBC_API.SQLSetConnectAttr.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
ODBC_API.SQLSetEnvAttr.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
ODBC_API.SQLStatistics.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_ushort, ctypes.c_ushort]
ODBC_API.SQLTables.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short, ctypes.c_char_p, ctypes.c_short]


def to_wchar(argtypes):
    if argtypes:  # Under IronPython some argtypes are not declared
        return [WCHAR_POINTER if x == ctypes.c_char_p else x for x in argtypes]
    return argtypes


ODBC_API.SQLColumnsW.argtypes = to_wchar(ODBC_API.SQLColumns.argtypes)
ODBC_API.SQLConnectW.argtypes = to_wchar(ODBC_API.SQLConnect.argtypes)
ODBC_API.SQLDataSourcesW.argtypes = to_wchar(ODBC_API.SQLDataSources.argtypes)
ODBC_API.SQLDescribeColW.argtypes = to_wchar(ODBC_API.SQLDescribeCol.argtypes)
ODBC_API.SQLDriverConnectW.argtypes = to_wchar(ODBC_API.SQLDriverConnect.argtypes)
ODBC_API.SQLDriversW.argtypes = to_wchar(ODBC_API.SQLDrivers.argtypes)
ODBC_API.SQLExecDirectW.argtypes = to_wchar(ODBC_API.SQLExecDirect.argtypes)
ODBC_API.SQLForeignKeysW.argtypes = to_wchar(ODBC_API.SQLForeignKeys.argtypes)
ODBC_API.SQLPrepareW.argtypes = to_wchar(ODBC_API.SQLPrepare.argtypes)
ODBC_API.SQLPrimaryKeysW.argtypes = to_wchar(ODBC_API.SQLPrimaryKeys.argtypes)
ODBC_API.SQLProcedureColumnsW.argtypes = to_wchar(ODBC_API.SQLProcedureColumns.argtypes)
ODBC_API.SQLProceduresW.argtypes = to_wchar(ODBC_API.SQLProcedures.argtypes)
ODBC_API.SQLStatisticsW.argtypes = to_wchar(ODBC_API.SQLStatistics.argtypes)
ODBC_API.SQLTablesW.argtypes = to_wchar(ODBC_API.SQLTables.argtypes)
ODBC_API.SQLGetDiagRecW.argtypes = to_wchar(ODBC_API.SQLGetDiagRec.argtypes)
ODBC_API.SQLGetInfoW.argtypes = to_wchar(ODBC_API.SQLGetInfo.argtypes)
# Set the alias for the ctypes functions for beter code readbility or performance.
ADDR = ctypes.byref
C_SHORT = ctypes.c_short
C_SSIZE_T = ctypes.c_ssize_t
SQL_EXECUTE = ODBC_API.SQLExecute
SQL_END_TRAN = ODBC_API.SQLEndTran


def ctrl_err(ht, h, ansi: bool):
    """Classify type of ODBC error from (type of handle, handle, return value), and raise with a list"""
    if ansi:
        state = CREATE_BUFFER(22)
        message = CREATE_BUFFER(1024 * 4)
        odbc_func = ODBC_API.SQLGetDiagRec
        raw_s = lambda s: bytes(s, 'ascii')
    else:
        state = CREATE_BUFFER_U(24)
        message = CREATE_BUFFER_U(1024 * 4)
        odbc_func = ODBC_API.SQLGetDiagRecW
        raw_s = str
    native_error = ctypes.c_int()
    err_list = []
    number_errors = 1
    while True:
        ret = odbc_func(ht, h, number_errors, state, ADDR(native_error), message, 1024, ADDR(C_SHORT()))
        if ret == SQL_NO_DATA:
            # No more data, I can raise print(err_list[0][1])
            state = err_list[0][0] if err_list else ''
            err_text = raw_s('[') + state + raw_s('] ') + (err_list[0][1] if err_list else '')
            if state[:2] in {raw_s('24'), raw_s('25'), raw_s('42')}:
                raise ProgrammingError(state, err_text)
            elif state[:2] in {raw_s('22')}:
                raise DataError(state, err_text)
            elif state[:2] in {raw_s('23')} or state == raw_s('40002'):
                raise IntegrityError(state, err_text)
            elif state == raw_s('0A000'):
                raise NotSupportedError(state, err_text)
            elif state in {raw_s('HYT00'), raw_s('HYT01'), raw_s('01000')}:
                raise OperationalError(state, err_text)
            elif state[:2] in {raw_s('IM'), raw_s('HY')}:
                raise Error(state, err_text)
            # else:
            #     raise DatabaseError(state, err_text)
        elif ret == -2:
            # The handle passed is an invalid handle
            raise ProgrammingError('', 'SQL_INVALID_HANDLE')
        elif ret == SQL_SUCCESS:
            if ansi:
                err_list.append((state.value, message.value, native_error.value))
            else:
                err_list.append((FROM_BUFFER_U(state), FROM_BUFFER_U(message), native_error.value))
            # number_errors += 1
        elif ret == -1:
            raise ProgrammingError('', 'SQL_ERROR')


def check_success(odbc_obj, ret):
    """Validate return value, if not success, raise exceptions based on the handle"""
    if ret not in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_NO_DATA, SQL_NULL_DATA):
        if isinstance(odbc_obj, Cursor):
            ctrl_err(SQL_HANDLE_STMT, odbc_obj.stmt_h, odbc_obj.ansi)
        elif isinstance(odbc_obj, Connection):
            ctrl_err(SQL_HANDLE_DBC, odbc_obj.dbc_h, odbc_obj.ansi)
        else:
            ctrl_err(SQL_HANDLE_ENV, odbc_obj, False)


"""Here, we have a few callables that determine how a result row is returned.
A new one can be added by creating a callable that:
- accepts a cursor as its parameter.
- returns a callable that accepts an iterable containing the row values."""


def tuple_row(_):
    return lambda _: _


def dict_row(cursor):
    def row(values: Iterable) -> dict:
        return {c[0]: float(v) if isinstance(v, Decimal) else v for c, v in zip(cursor.description, values if isinstance(values, Iterable) else [values])} if values else {}

    return row


# When Null is used in a binary parameter, database usually would not accept the None for a binary field, so the work around is to use a
# Specical None that the pypyodbc moudle would know this NULL is for a binary field.
class BinaryNullType:
    pass


# The get_type function is used to determine if parameters need to be re-binded against the changed parameter types
# 'b' for bool, 'U' for long unicode string, 'u' for short unicode string
# 'S' for long 8 bit string, 's' for short 8 bit string, 'l' for big integer, 'i' for normal integer
# 'f' for float, 'D' for Decimal, 't' for datetime.time, 'd' for datetime.datetime, 'dt' for datetime.datetime
# 'bi' for binary


def get_type(v):
    if isinstance(v, bool):
        return 'b',
    elif isinstance(v, str):
        if len(v) >= 255:
            # use num of chars times 2 since utf-16-le encoding will double the number of bytes needed
            return 'U', (len(v) // 1000 + 1) * 1000 * 2
        else:
            return 'u',
    elif isinstance(v, (bytes, str)):
        if len(v) >= 255:
            return 'S', (len(v) // 1000 + 1) * 1000
        else:
            return 's',
    elif isinstance(v, int):
        # SQL_BIGINT defination: http://msdn.microsoft.com/en-us/library/ms187745.aspx
        if v > 2147483647 or v < -2147483648:
            return 'l',
        else:
            return 'i',
    elif isinstance(v, float):
        return 'f',
    elif isinstance(v, BinaryNullType):
        return 'BN',
    elif v is None:
        return 'N',
    elif isinstance(v, Decimal):
        t = v.as_tuple()  # 1.23 -> (1,2,3),-2 , 1.23*E7 -> (1,2,3),5
        return 'D', (len(t[1]), 0 - t[2])  # number of digits, and number of decimal digits
    elif isinstance(v, datetime):
        return 'dt',
    elif isinstance(v, date):
        return 'd',
    elif isinstance(v, time):
        return 't',
    elif isinstance(v, (bytearray, memoryview)):
        return 'bi', (len(v) // 1000 + 1) * 1000
    return type(v)


# The Cursor Class.
class Cursor:
    def __init__(self, conx, row_type_callable=None):
        """ Initialize self.stmt_h, which is the handle of a statement
        A statement is actually the basis of a python "cursor" object"""
        self.stmt_h = ctypes.c_void_p()
        self.connection = conx
        self.ansi = conx.ansi
        self.row_type_callable = row_type_callable or dict_row
        self.statement = None
        self._last_param_types = None
        self._param_buffer_list = []
        self._col_buffer_list = []
        self._row_type = None
        self.rowcount = -1
        self.description = None
        self._col_type_code_list = []
        self._outputsize = {}
        self._inputsizers = []
        check_success(self, ODBC_API.SQLAllocHandle(SQL_HANDLE_STMT, self.connection.dbc_h, ADDR(self.stmt_h)))
        self.timeout = conx.timeout
        if self.timeout != 0:
            self.set_timeout(self.timeout)
        self._param_sql_type_list = []
        self.closed = False

    def set_timeout(self, timeout):
        self.timeout = timeout
        check_success(self, ODBC_API.SQLSetStmtAttr(self.stmt_h, 0, self.timeout, 0))

    def prepare(self, query_string):
        """prepare a query"""
        # self._free_results(FREE_STATEMENT)
        if not self.connection:
            self.close()
        if isinstance(query_string, str):
            c_query_string = WCHAR_POINTER(UCS_BUF(query_string))
            ret = ODBC_API.SQLPrepareW(self.stmt_h, c_query_string, len(query_string))
        else:
            c_query_string = ctypes.c_char_p(query_string)
            ret = ODBC_API.SQLPrepare(self.stmt_h, c_query_string, len(query_string))
        if ret != SQL_SUCCESS:
            check_success(self, ret)
        self._param_sql_type_list = []
        if self.connection.support_SQLDescribeParam:
            num_params = C_SHORT()
            ret = ODBC_API.SQLNumParams(self.stmt_h, ADDR(num_params))
            if ret != SQL_SUCCESS:
                check_success(self, ret)
            for col_num in range(num_params.value):
                data_type = C_SHORT()
                decimal_digits = C_SHORT()
                ret = ODBC_API.SQLDescribeParam(self.stmt_h, ctypes.c_ushort(col_num + 1), ADDR(data_type), ADDR(ctypes.c_size_t()), ADDR(decimal_digits), ADDR(C_SHORT()))
                if ret != SQL_SUCCESS:
                    try:
                        check_success(self, ret)
                    except DatabaseError:
                        if sys.exc_info()[1].value[0] == '07009':
                            self._param_sql_type_list = []
                            break
                        raise sys.exc_info()[1]
                    except Exception as e:
                        print(e.__repr__())
                        raise sys.exc_info()[1]
                self._param_sql_type_list.append((data_type.value, decimal_digits.value))
        self.statement = query_string

    def _bind_params(self, param_types, pram_io_list=[]):
        """Create parameter buffers based on param types, and bind them to the statement"""
        # Clear the old Parameters
        if not self.connection:
            self.close()
        # self._free_results(NO_FREE_STATEMENT)
        # Get the number of query parameters judged by database.
        num_params = C_SHORT()
        ret = ODBC_API.SQLNumParams(self.stmt_h, ADDR(num_params))
        if ret != SQL_SUCCESS:
            check_success(self, ret)
        if len(param_types) != num_params.value:
            # In case number of parameters provided do not same as number required
            raise ProgrammingError('HY000', f'The SQL contains {num_params.value} parameter markers, but {len(param_types)} parameters were supplied')
        # Every parameter needs to be binded to a buffer
        param_buffer_list = []
        # Temporary holder since we can only call SQLDescribeParam before calling SQLBindParam.
        temp_holder = []
        for col_num in range(num_params.value):
            dec_num = 0
            sql_c_type = SQL_C_CHAR
            param_types_0 = param_types[col_num][0]
            if param_types_0 == 'u':
                sql_c_type = SQL_C_WCHAR
                sql_type = SQL_WVARCHAR
                # allocate two bytes for each char due to utf-16-le encoding
                buf_size = 255 * 2
                parameter_buffer = CREATE_BUFFER_U(buf_size)
            elif param_types_0 == 's':
                sql_type = SQL_VARCHAR
                buf_size = 255
            elif param_types_0 == 'U':
                sql_c_type = SQL_C_WCHAR
                sql_type = SQL_WLONGVARCHAR
                buf_size = param_types[col_num][1]  # len(self._inputsizers)>col_num and self._inputsizers[col_num] or 20500
                parameter_buffer = CREATE_BUFFER_U(buf_size)
            elif param_types_0 == 'S':
                sql_type = SQL_LONGVARCHAR
                buf_size = param_types[col_num][1]  # len(self._inputsizers)>col_num and self._inputsizers[col_num] or 20500
            # bool subclasses int, thus has to go first
            elif param_types_0 == 'b':
                sql_type = SQL_BIT
                buf_size = SQL_DATA_TYPE_DICT[sql_type][4]
            elif param_types_0 == 'i':
                sql_type = SQL_INTEGER
                buf_size = SQL_DATA_TYPE_DICT[sql_type][4]
            elif param_types_0 == 'l':
                sql_type = SQL_BIGINT
                buf_size = SQL_DATA_TYPE_DICT[sql_type][4]
            elif param_types_0 == 'D':  # Decimal
                sql_type = SQL_NUMERIC
                digit_num, dec_num = param_types[col_num][1]
                if dec_num > 0:
                    # has decimal
                    # 1.23 as_tuple -> (1,2,3),-2
                    # 1.23 digit_num = 3 dec_num = 2
                    # 0.11 digit_num = 2 dec_num = 2
                    # 0.01 digit_num = 1 dec_num = 2
                    buf_size = dec_num if dec_num > digit_num else digit_num
                else:
                    # no decimal
                    buf_size = digit_num - dec_num
                    dec_num = 0
                parameter_buffer = CREATE_BUFFER(buf_size + 4)  # add extra length for sign and dot
            elif param_types_0 == 'f':
                sql_type = SQL_DOUBLE
                buf_size = SQL_DATA_TYPE_DICT[sql_type][4]
            # datetime subclasses date, thus has to go first
            elif param_types_0 == 'dt':
                sql_type = SQL_TYPE_TIMESTAMP
                buf_size = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]
                dec_num = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][1]
            elif param_types_0 == 'd':
                if SQL_TYPE_DATE in self.connection.type_size_dic:
                    # if DEBUG:print('conx.type_size_dic.has_key(SQL_TYPE_DATE)')
                    sql_type = SQL_TYPE_DATE
                    buf_size = self.connection.type_size_dic[SQL_TYPE_DATE][0]
                    dec_num = self.connection.type_size_dic[SQL_TYPE_DATE][1]
                else:
                    # SQL Sever <2008 doesn't have a DATE type.
                    sql_type = SQL_TYPE_TIMESTAMP
                    buf_size = 10
            elif param_types_0 == 't':
                if SQL_TYPE_TIME in self.connection.type_size_dic:
                    sql_type = SQL_TYPE_TIME
                    buf_size = self.connection.type_size_dic[SQL_TYPE_TIME][0]
                    dec_num = self.connection.type_size_dic[SQL_TYPE_TIME][1]
                elif SQL_SS_TIME2 in self.connection.type_size_dic:
                    # TIME type added in SQL Server 2008
                    sql_type = SQL_SS_TIME2
                    buf_size = self.connection.type_size_dic[SQL_SS_TIME2][0]
                    dec_num = self.connection.type_size_dic[SQL_SS_TIME2][1]
                else:
                    # SQL Sever <2008 doesn't have a TIME type.
                    sql_type = SQL_TYPE_TIMESTAMP
                    buf_size = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]
                    dec_num = 3
            elif param_types_0 == 'BN':
                sql_c_type = SQL_C_BINARY
                sql_type = SQL_VARBINARY
                buf_size = 1
            elif param_types_0 == 'N':
                if len(self._param_sql_type_list) > 0:
                    sql_c_type = 99
                    sql_type = self._param_sql_type_list[col_num][0]
                    buf_size = 1
                else:
                    sql_type = SQL_CHAR
                    buf_size = 1
            elif param_types_0 == 'bi':
                sql_c_type = SQL_C_BINARY
                sql_type = SQL_LONGVARBINARY
                buf_size = param_types[col_num][1]  # len(self._inputsizers)>col_num and self._inputsizers[col_num] or 20500
            else:
                sql_type = SQL_LONGVARCHAR
                buf_size = len(self._inputsizers) > col_num and self._inputsizers[col_num] or 20500
            parameter_buffer = locals().get('parameter_buffer', CREATE_BUFFER(buf_size))
            temp_holder.append((sql_c_type, sql_type, buf_size, dec_num, parameter_buffer))
            del parameter_buffer
        for col_num, (sql_c_type, sql_type, buf_size, dec_num, parameter_buffer) in enumerate(temp_holder):
            len_or_ind_buf = C_SSIZE_T()
            input_output_type = 1
            if len(pram_io_list) > col_num:
                input_output_type = pram_io_list[col_num]
            ret = ODBC_API.SQLBindParameter(self.stmt_h, col_num + 1, input_output_type, sql_c_type, sql_type, buf_size, dec_num, ADDR(parameter_buffer), C_SSIZE_T(buf_size), ADDR(len_or_ind_buf))
            if ret != SQL_SUCCESS:
                check_success(self, ret)
            # Append the value buffer and the length buffer to the array
            param_buffer_list.append((parameter_buffer, len_or_ind_buf, sql_type))
        self._last_param_types = param_types
        self._param_buffer_list = param_buffer_list

    def execute(self, query_string, params=None, many_mode=False):
        """Execute the query string, with optional parameters.
        If parameters are provided, the query would first be prepared, then executed with parameters;
        If parameters are not provided, only th query sting, it would be executed directly"""
        if not self.connection:
            self.close()
        self._free_stmt(SQL_CLOSE)
        if params:
            # If parameters exist, first prepare the query then executed with parameters
            if not isinstance(params, (tuple, list)):
                raise TypeError("Params must be in a list, tuple")
            if query_string != self.statement:
                # if the query is not same as last query, then it is not prepared
                self.prepare(query_string)
            param_types = [get_type(_) for _ in params]
            if self._last_param_types is None or len(param_types) != len(self._last_param_types) or any(p_type[0] != 'N' and p_type != self._last_param_types[i] for i, p_type in enumerate(param_types)):
                self._free_stmt(SQL_RESET_PARAMS)
                self._bind_params(param_types)
            # With query prepared, now put parameters into buffers
            col_num = 0
            for param_buffer, param_buffer_len, sql_type in self._param_buffer_list:
                c_char_buf, c_buf_len = '', 0
                param_val = params[col_num]
                param_types_0 = param_types[col_num][0]
                if param_types_0 in {'N', 'BN'}:
                    param_buffer_len.value = SQL_NULL_DATA
                    col_num += 1
                    continue
                elif param_types_0 in {'i', 'l', 'f'}:
                    c_char_buf = bytes(str(param_val), 'ascii')
                    c_buf_len = len(c_char_buf)
                elif param_types_0 in {'s', 'S'}:
                    c_char_buf = param_val
                    c_buf_len = len(c_char_buf)
                elif param_types_0 in {'u', 'U'}:
                    c_char_buf = UCS_BUF(param_val)
                    c_buf_len = len(c_char_buf)
                elif param_types_0 == 'dt':
                    c_char_buf = bytes(param_val.strftime('%Y-%m-%d %H:%M:%S.%f')[:self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]], 'ascii')
                    c_buf_len = len(c_char_buf)
                    # print c_buf_len, c_char_buf
                elif param_types_0 == 'd':
                    c_char_buf = bytes(param_val.isoformat()[:self.connection.type_size_dic[SQL_TYPE_DATE][0] if SQL_TYPE_DATE in self.connection.type_size_dic else 10], 'ascii')
                    c_buf_len = len(c_char_buf)
                    # print c_char_buf
                elif param_types_0 == 't':
                    if SQL_TYPE_TIME in self.connection.type_size_dic:
                        c_char_buf = param_val.isoformat()[:self.connection.type_size_dic[SQL_TYPE_TIME][0]]
                        c_buf_len = len(c_char_buf)
                    elif SQL_SS_TIME2 in self.connection.type_size_dic:
                        c_char_buf = param_val.isoformat()[:self.connection.type_size_dic[SQL_SS_TIME2][0]]
                        c_buf_len = len(c_char_buf)
                    else:
                        c_buf_len = self.connection.type_size_dic[SQL_TYPE_TIMESTAMP][0]
                        time_str = param_val.isoformat()
                        if len(time_str) == 8:
                            time_str += '.000'
                        c_char_buf = f'1900-01-01 {time_str[:c_buf_len - 11]}'
                    c_char_buf = bytes(c_char_buf, 'ascii')
                    # print c_buf_len, c_char_buf
                elif param_types_0 == 'b':
                    c_char_buf = bytes('1' if param_val else '0', 'ascii')
                    c_buf_len = 1
                elif param_types_0 == 'D':  # Decimal
                    digit_string = ''.join(str(x) for x in param_val.as_tuple()[1])
                    digit_num, dec_num = param_types[col_num][1]
                    if dec_num > 0:
                        # has decimal
                        # 1.12 digit_num = 3 dec_num = 2
                        # 0.11 digit_num = 2 dec_num = 2
                        # 0.01 digit_num = 1 dec_num = 2
                        v = f'{param_val.as_tuple()[0] == 0 and "+" or "-"}{digit_string[:digit_num - dec_num]}.{digit_string[0 - dec_num:].zfill(dec_num)}'
                    else:
                        # no decimal
                        v = f'{digit_string}{"0" * (0 - dec_num)}'
                    c_char_buf = bytes(v, 'ascii')
                    c_buf_len = len(c_char_buf)
                elif param_types_0 == 'bi':
                    c_char_buf = bytes(param_val)
                    c_buf_len = len(c_char_buf)
                else:
                    c_char_buf = param_val
                if param_types_0 == 'bi':
                    param_buffer.raw = bytes(param_val)
                else:
                    # print (type(param_val), param_buffer, param_buffer.value)
                    param_buffer.value = c_char_buf
                if param_types_0 in {'U', 'u', 'S', 's'}:
                    # ODBC driver will find NUL in unicode and string to determine their length
                    param_buffer_len.value = -3
                else:
                    param_buffer_len.value = c_buf_len
                col_num += 1
            ret = SQL_EXECUTE(self.stmt_h)
            if ret != SQL_SUCCESS:
                # print param_valparam_buffer, param_buffer.value
                check_success(self, ret)
            if not many_mode:
                self._update_desc()
        else:
            self._free_stmt()
            self._last_param_types = None
            self.statement = None
            if isinstance(query_string, str):
                ret = ODBC_API.SQLExecDirectW(self.stmt_h, WCHAR_POINTER(UCS_BUF(query_string)), len(query_string))
            else:
                ret = ODBC_API.SQLExecDirect(self.stmt_h, ctypes.c_char_p(query_string), len(query_string))
            check_success(self, ret)
            self._update_desc()
        return self

    def executemany(self, query_string, params_list=None):
        if params_list is None:
            params_list = [None]
        if not self.connection:
            self.close()
        for params in params_list:
            self.execute(query_string, params, many_mode=True)
        self.rowcount = -1
        self._update_desc()

    def _create_col_buf(self):
        if not self.connection:
            self.close()
        self._free_stmt(SQL_UNBIND)
        self._col_buffer_list = []
        bind_data = True
        for col_num in range(self._num_of_cols()):
            col_sql_data_type = self._col_type_code_list[col_num]
            target_type = SQL_DATA_TYPE_DICT[col_sql_data_type][2]
            dynamic_length = SQL_DATA_TYPE_DICT[col_sql_data_type][5]
            # set default size base on the column's sql data type
            total_buf_len = SQL_DATA_TYPE_DICT[col_sql_data_type][4]
            # over-write if there's pre-set size value for "large columns"
            if total_buf_len > 20500:
                total_buf_len = self._outputsize.get(None, total_buf_len)
            # over-write if there's pre-set size value for the "col_num" column
            total_buf_len = self._outputsize.get(col_num, total_buf_len)
            # if the size of the buffer is very long, do not bind because a large buffer decrease performance, and sometimes you only get a NULL value. in that case use sqlgetdata instead.
            if self.description[col_num][2] >= 1024:
                dynamic_length = True
            alloc_buffer = SQL_DATA_TYPE_DICT[col_sql_data_type][3](total_buf_len)
            used_buf_len = C_SSIZE_T()
            if col_sql_data_type in {SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR}:
                target_type = SQL_C_WCHAR
                alloc_buffer = CREATE_BUFFER_U(total_buf_len)
            if bind_data and dynamic_length:
                bind_data = False
            self._col_buffer_list.append([self.description[col_num][0], target_type, used_buf_len, ADDR(used_buf_len), alloc_buffer, ADDR(alloc_buffer), total_buf_len, self.connection.output_converter[self._col_type_code_list[col_num]], bind_data])
            if bind_data:
                ret = ODBC_API.SQLBindCol(self.stmt_h, col_num + 1, target_type, ADDR(alloc_buffer), total_buf_len, ADDR(used_buf_len))
                if ret != SQL_SUCCESS:
                    check_success(self, ret)

    def _update_desc(self):
        """Get the information of (name, type_code, display_size, internal_size, col_precision, scale, null_ok)"""
        if not self.connection:
            self.close()
        cname = CREATE_BUFFER_U(1024)
        ctype_code = C_SHORT()
        csize = ctypes.c_size_t()
        cdisp_size = C_SSIZE_T(0)
        c_decimal_digits = C_SHORT()
        cnull_ok = C_SHORT()
        col_descr = []
        self._col_type_code_list = []
        for col in range(1, self._num_of_cols() + 1):
            ret = ODBC_API.SQLColAttribute(self.stmt_h, col, 6, ADDR(CREATE_BUFFER(10)), 10, ADDR(C_SHORT()), ADDR(cdisp_size))
            if ret != SQL_SUCCESS:
                check_success(self, ret)
            ret = ODBC_API.SQLDescribeColW(self.stmt_h, col, cname, len(cname), ADDR(C_SHORT()), ADDR(ctype_code), ADDR(csize), ADDR(c_decimal_digits), ADDR(cnull_ok))
            if ret != SQL_SUCCESS:
                check_success(self, ret)
            # (name, type_code, display_size,
            col_descr.append((FROM_BUFFER_U(cname), SQL_DATA_TYPE_DICT.get(ctype_code.value, (ctype_code.value,))[0], cdisp_size.value, csize.value, csize.value, c_decimal_digits.value, cnull_ok.value == 1 and True or False))
            self._col_type_code_list.append(ctype_code.value)
        if len(col_descr) > 0:
            self.description = col_descr
            # Create the row type before fetching.
            self._row_type = self.row_type_callable(self)
        else:
            self.description = None
        self._create_col_buf()

    # def _num_of_rows(self):
    #     """Get the number of rows"""
    #     if not self.connection:
    #         self.close()
    #     nor = C_SSIZE_T()
    #     ret = ODBC_API.SQLRowCount(self.stmt_h, ADDR(nor))
    #     if ret not in {SQL_SUCCESS, SQL_NO_DATA}:
    #         check_success(self, ret)
    #     self.rowcount = nor.value
    #     return self.rowcount

    def _num_of_cols(self):
        """Get the number of cols"""
        if not self.connection:
            self.close()
        noc = C_SHORT()
        ret = ODBC_API.SQLNumResultCols(self.stmt_h, ADDR(noc))
        if ret != SQL_SUCCESS:
            check_success(self, ret)
        return noc.value

    def fetchall(self):
        if not self.connection:
            self.close()
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchone(self):
        if not self.connection:
            self.close()
        ret = ODBC_API.SQLFetch(self.stmt_h)
        if ret in (SQL_SUCCESS, SQL_SUCCESS_WITH_INFO):
            '''Bind buffers for the record set columns'''
            value_list = []
            col_num = 1
            for col_name, target_type, used_buf_len, ADDR_used_buf_len, alloc_buffer, ADDR_alloc_buffer, total_buf_len, buf_cvt_func, bound_data in self._col_buffer_list:
                raw_data_parts = []
                while True:
                    if bound_data:
                        ret = SQL_SUCCESS
                    else:
                        ret = ODBC_API.SQLGetData(self.stmt_h, col_num, target_type, ADDR_alloc_buffer, total_buf_len, ADDR_used_buf_len)
                    if ret == SQL_SUCCESS:
                        if used_buf_len.value == SQL_NULL_DATA:
                            value_list.append(None)
                        else:
                            if not raw_data_parts:
                                # Means no previous data, no need to combine
                                if target_type == SQL_C_BINARY:
                                    value_list.append(buf_cvt_func(alloc_buffer.raw[:used_buf_len.value]))
                                elif target_type == SQL_C_WCHAR:
                                    if used_buf_len.value < total_buf_len:
                                        ctypes.memset(ctypes.addressof(alloc_buffer) + used_buf_len.value, 0, 1)
                                    value_list.append(buf_cvt_func(FROM_BUFFER_U(alloc_buffer)))
                                elif alloc_buffer.value == '':
                                    value_list.append('')
                                else:
                                    value_list.append(buf_cvt_func(alloc_buffer.value))
                            else:
                                # There are previous fetched raw data to combine
                                if target_type == SQL_C_BINARY:
                                    raw_data_parts.append(alloc_buffer.raw[:used_buf_len.value])
                                elif target_type == SQL_C_WCHAR:
                                    raw_data_parts.append(FROM_BUFFER_U(alloc_buffer))
                                else:
                                    raw_data_parts.append(alloc_buffer.value)
                        break
                    elif ret == SQL_SUCCESS_WITH_INFO:
                        # Means the data is only partial
                        if target_type == SQL_C_BINARY:
                            raw_data_parts.append(alloc_buffer.raw[:used_buf_len.value])
                        elif target_type == SQL_C_WCHAR:
                            raw_data_parts.append(FROM_BUFFER_U(alloc_buffer))
                        else:
                            raw_data_parts.append(alloc_buffer.value)
                    elif ret == SQL_NO_DATA:
                        # Means all data has been transmitted
                        break
                    else:
                        check_success(self, ret)
                if raw_data_parts:
                    if target_type != SQL_C_BINARY:
                        raw_value = ''.join(x.decode("utf-8") if type(x) is bytes else x for x in raw_data_parts)
                    else:
                        raw_value = b''.join(raw_data_parts)
                    value_list.append(buf_cvt_func(raw_value))
                col_num += 1
            return self._row_type(value_list)
        else:
            if ret == SQL_NO_DATA:
                return None
            check_success(self, ret)

    def _free_stmt(self, free_type=None):
        if not self.connection:
            self.close()
        if not self.connection.connected:
            raise ProgrammingError('HY000', 'Attempt to use a closed connection.')
        # self.description = None
        # self.rowcount = -1
        if free_type in {SQL_CLOSE, None}:
            ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_CLOSE)
            if ret != SQL_SUCCESS:
                check_success(self, ret)
        if free_type in {SQL_UNBIND, None}:
            ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_UNBIND)
            if ret != SQL_SUCCESS:
                check_success(self, ret)
        if free_type in {SQL_RESET_PARAMS, None}:
            ret = ODBC_API.SQLFreeStmt(self.stmt_h, SQL_RESET_PARAMS)
            if ret != SQL_SUCCESS:
                check_success(self, ret)

    def get_type_info(self, sql_type=None):
        if not self.connection:
            self.close()
        if ODBC_API.SQLGetTypeInfo(self.stmt_h, sql_type or 0) in {SQL_SUCCESS, SQL_SUCCESS_WITH_INFO}:
            self._update_desc()
            return self.fetchone()

    def commit(self):
        if not self.connection:
            self.close()
        self.connection.commit()

    def rollback(self):
        if not self.connection:
            self.close()
        self.connection.rollback()

    def close(self):
        """Call SQLCloseCursor API to free the statement handle"""
        #        ret = ODBC_API.SQLCloseCursor(self.stmt_h)
        #        check_success(self, ret)
        if self.connection.connected:
            for _ in (SQL_CLOSE, SQL_UNBIND, SQL_RESET_PARAMS):
                check_success(self, ODBC_API.SQLFreeStmt(self.stmt_h, _))
            check_success(self, ODBC_API.SQLFreeHandle(SQL_HANDLE_STMT, self.stmt_h))
        self.closed = True

    def __del__(self):
        if not self.closed:
            self.close()

    def __exit__(self, type, value, traceback):
        if not self.connection:
            self.close()
        if value:
            self.rollback()
        else:
            self.commit()
        self.close()

    def __enter__(self):
        return self


# This class implement a odbc connection.
class Connection:
    def __init__(self, connect_string: str = '', autocommit: bool = False, ansi: bool = False, timeout: int = 0, readonly: bool = False, **kargs):
        """Init variables and connect to the engine"""
        global SHARED_ENV_H
        self.connected = 0
        self.type_size_dic = {}
        self.ansi = False
        self.dbc_h = ctypes.c_void_p()
        self.autocommit = autocommit
        self.readonly = False
        # the query timeout value
        self.timeout = 0
        # self._cursors = []
        connect_string += ';'.join(f'{k}={v}' for k, v in kargs.items())
        self.connectString = connect_string
        self.clear_output_converters()
        try:
            LOCK.acquire()
            if SHARED_ENV_H is None:
                # Initialize an enviroment if it is not created.
                if POOLING:
                    check_success(SQL_NULL_HANDLE, ODBC_API.SQLSetEnvAttr(SQL_NULL_HANDLE, 201, 2, SQL_IS_UINTEGER))
                SHARED_ENV_H = ctypes.c_void_p()
                check_success(SHARED_ENV_H, ODBC_API.SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, ADDR(SHARED_ENV_H)))
                # Set the ODBC environment's compatibil leve to ODBC 3.0
                check_success(SHARED_ENV_H, ODBC_API.SQLSetEnvAttr(SHARED_ENV_H, 200, 3, 0))
        finally:
            LOCK.release()
        # Allocate an DBC handle self.dbc_h under the environment shared_env_h
        # This DBC handle is actually the basis of a "connection"
        # The handle of self.dbc_h will be used to connect to a certain source in the self.connect and self.ConnectByDSN method
        ret = ODBC_API.SQLAllocHandle(SQL_HANDLE_DBC, SHARED_ENV_H, ADDR(self.dbc_h))
        check_success(self, ret)
        self.connection_timeout = CONNECTION_TIMEOUT
        if self.connection_timeout != 0:
            self.set_connection_timeout(CONNECTION_TIMEOUT)
        self.connect(connect_string, autocommit, ansi, timeout, readonly)

    def set_connection_timeout(self, connection_timeout: int):
        self.connection_timeout = connection_timeout
        ret = ODBC_API.SQLSetConnectAttr(self.dbc_h, 113, connection_timeout, SQL_IS_UINTEGER)
        check_success(self, ret)

    def connect(self, connect_string: str = '', autocommit: bool = False, ansi: bool = False, timeout: int = 0, readonly: bool = False):
        """Connect to odbc, using connect strings and set the connection's attributes like autocommit and timeout
        by calling SQLSetConnectAttr"""
        # Before we establish the connection by the connection string
        # Set the connection's attribute of "timeout" (Actully LOGIN_TIMEOUT)
        if timeout != 0:
            check_success(self, ODBC_API.SQLSetConnectAttr(self.dbc_h, 103, timeout, SQL_IS_UINTEGER))
        # Create one connection with a connect string by calling SQLDriverConnect and make self.dbc_h the handle of this connection
        # Convert the connetsytring to encoded string so it can be converted to a ctypes c_char array object
        self.ansi = ansi
        if not ansi:
            c_connect_string = WCHAR_POINTER(UCS_BUF(self.connectString))
            odbc_func = ODBC_API.SQLDriverConnectW
        else:
            c_connect_string = ctypes.c_char_p(self.connectString)
            odbc_func = ODBC_API.SQLDriverConnect
        # With unixODBC, SQLDriverConnect will intermittently fail with error:
        #    [01000] [unixODBC][Driver Manager]Can't open lib '/path/to/so' : file not found"
        # or:
        #    [01000] [unixODBC][Driver Manager]Can't open lib '/path/to/so' : (null)"
        # when called concurrently by more than one threads. So, we have to use a lock to serialize the calls. By the way, the error is much less likely to happen if ODBC Tracing is enabled, likely due to the implicit serialization caused by writing to trace file.
        if ODBC_API._name != 'odbc32':
            try:
                LOCK.acquire()
                ret = odbc_func(self.dbc_h, 0, c_connect_string, len(self.connectString), None, 0, None, SQL_DRIVER_NOPROMPT)
            finally:
                LOCK.release()
        else:
            ret = odbc_func(self.dbc_h, 0, c_connect_string, len(self.connectString), None, 0, None, SQL_DRIVER_NOPROMPT)
        check_success(self, ret)
        # Set the connection's attribute of "autocommit"
        self.autocommit = autocommit
        check_success(self, ODBC_API.SQLSetConnectAttr(self.dbc_h, 102, int(self.autocommit), SQL_IS_UINTEGER))
        # Set the connection's attribute of "readonly"
        self.readonly = readonly
        if readonly:
            check_success(self, ODBC_API.SQLSetConnectAttr(self.dbc_h, SQL_ATTR_ACCESS_MODE, SQL_MODE_READ_ONLY, SQL_IS_UINTEGER))
        check_success(self, ODBC_API.SQLSetConnectAttr(self.dbc_h, SQL_ATTR_ACCESS_MODE, readonly and SQL_MODE_READ_ONLY or 0, SQL_IS_UINTEGER))
        self.connected = 1
        self.update_db_special_info()

    def clear_output_converters(self):
        self.output_converter = {sqltype: profile[1] for sqltype, profile in SQL_DATA_TYPE_DICT.items()}

    def add_output_converter(self, sqltype, func):
        self.output_converter[sqltype] = func

    def connect_by_dsn(self, dsn: str, user: str, passwd: str = ''):
        """Connect to odbc, we need dsn, user and optionally password"""
        self.dsn = dsn
        self.user = user
        self.passwd = passwd
        sn = CREATE_BUFFER(dsn)
        un = CREATE_BUFFER(user)
        pw = CREATE_BUFFER(passwd)
        check_success(self, ODBC_API.SQLConnect(self.dbc_h, sn, len(sn), un, len(un), pw, len(pw)))
        self.update_db_special_info()
        self.connected = 1

    def cursor(self, row_type_callable=None) -> Cursor:
        if not self.connected:
            raise ProgrammingError('HY000', 'Attempt to use a closed connection.')
        return Cursor(self, row_type_callable=row_type_callable)

    def update_db_special_info(self):
        try:
            if 'OdbcFb' in self.getinfo(SQL_DRIVER_NAME):
                return
        except Exception as e:
            print(e.__repr__())
        for sql_type in (SQL_TYPE_TIMESTAMP, SQL_TYPE_DATE, SQL_TYPE_TIME, SQL_SS_TIME2):
            with Cursor(self, row_type_callable=tuple_row) as cur:
                try:
                    info_tuple = cur.get_type_info(sql_type)
                    if info_tuple is not None:
                        self.type_size_dic[sql_type] = info_tuple[2], info_tuple[14]
                except Exception as e:
                    print(e.__repr__())
        self.support_SQLDescribeParam = False
        try:
            driver_name = self.getinfo(SQL_DRIVER_NAME)
            if any(x in driver_name for x in ('SQLSRV', 'ncli', 'libsqlncli')):
                self.support_SQLDescribeParam = True
        except Exception as e:
            print(e.__repr__())

    def commit(self):
        if not self.connected:
            raise ProgrammingError('HY000', 'Attempt to use a closed connection.')
        ret = SQL_END_TRAN(SQL_HANDLE_DBC, self.dbc_h, 0)
        if ret != SQL_SUCCESS:
            check_success(self, ret)

    def rollback(self):
        if not self.connected:
            raise ProgrammingError('HY000', 'Attempt to use a closed connection.')
        ret = SQL_END_TRAN(SQL_HANDLE_DBC, self.dbc_h, 1)
        if ret != SQL_SUCCESS:
            check_success(self, ret)

    def getinfo(self, infotype):
        if infotype not in A_INFO_TYPES.keys():
            raise ProgrammingError('HY000', 'Invalid getinfo value: ' + str(infotype))
        if A_INFO_TYPES[infotype] == 'GI_UINTEGER':
            alloc_buffer = ctypes.c_ulong()
            check_success(self, ODBC_API.SQLGetInfo(self.dbc_h, infotype, ADDR(alloc_buffer), 1000, ADDR(C_SHORT())))
            result = alloc_buffer.value
        elif A_INFO_TYPES[infotype] == 'GI_USMALLINT':
            alloc_buffer = ctypes.c_ushort()
            check_success(self, ODBC_API.SQLGetInfo(self.dbc_h, infotype, ADDR(alloc_buffer), 1000, ADDR(C_SHORT())))
            result = alloc_buffer.value
        else:
            alloc_buffer = CREATE_BUFFER(1000)
            api_f = ODBC_API.SQLGetInfo if self.ansi else ODBC_API.SQLGetInfoW
            check_success(self, api_f(self.dbc_h, infotype, ADDR(alloc_buffer), 1000, ADDR(C_SHORT())))
            result = alloc_buffer.value if self.ansi else ucs_dec(alloc_buffer)
            if A_INFO_TYPES[infotype] == 'GI_YESNO':
                result = str(result[0]) == str('Y')
        return result

    def __exit__(self, type, value, traceback):
        if value:
            self.rollback()
        else:
            self.commit()
        if self.connected:
            self.close()

    def __enter__(self):
        return self

    def __del__(self):
        if self.connected:
            self.close()

    def close(self):
        if not self.connected:
            raise ProgrammingError('HY000', 'Attempt to close a closed connection.')
        # for cur in self._cursors:
        # if cur is not None and not cur.closed:
        # cur.close()
        if self.connected:
            if not self.autocommit:
                self.rollback()
            check_success(self, ODBC_API.SQLDisconnect(self.dbc_h))
        check_success(self, ODBC_API.SQLFreeHandle(SQL_HANDLE_DBC, self.dbc_h))
        #        if shared_env_h.value:
        #            ret = ODBC_API.SQLFreeHandle(SQL_HANDLE_ENV, shared_env_h)
        #            check_success(shared_env_h, ret)
        self.connected = 0

    def execute(self, query_string: str, params=None, many_mode: bool = False):
        with self.cursor() as cur:
            cur.execute(query_string, params, many_mode)


connect = Connection
