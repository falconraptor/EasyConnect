try:
    import pymysql
except ImportError:
    class pymysql:
        OperationalError = InternalError = Exception
try:
    import pyodbc as pypyodbc
except ImportError:
    from easyconnect import pypyodbc
