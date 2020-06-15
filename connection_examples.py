from easyconnect.db_pool import MYSQL, MSSQL


class MSSQL_TEST(MSSQL):
    def __init__(self):
        super().__init__('321.159.456.258', '', '...', '')


class MYSQL_TEST(MYSQL):
    def __init__(self):
        super().__init__('321.159.456.258', '', '...', '')
