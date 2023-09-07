try:
    import pymysql
except ImportError:
    class pymysql:
        def __getattribute__(self, item):
            pass

        def __setattr__(self, key, value):
            pass
try:
    import pyodbc as pypyodbc
except ImportError:
    try:
        from easyconnect_old import pypyodbc
    except ImportError:
        class pypyodbc:
            def __getattribute__(self, item):
                pass

            def __setattr__(self, key, value):
                pass
