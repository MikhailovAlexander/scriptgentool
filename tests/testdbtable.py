import unittest
from unittest.mock import MagicMock
from datetime import datetime, date
from core.dbtable import DbTable
from core.sqlquerybuilder import SqlQueryBuilder
from core.sqlservertemplates import SqlServerTemplates
from dbconnector import DbConnector


LOGGER_DICT_STUB = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "default": {
            "level": "CRITICAL",
            "class": "logging.StreamHandler"
        }
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": "CRITICAL",
            "propagate": True
        }
    }
}
IS_CONNECTED = DbConnector().check_and_close_connection()
WORK_DB_NAME = "UnitTest_DbTable_Work"
CLEAR_DB_NAME = "UnitTest_DbTable_Clear"
TABLE_NAME = "dbo.test"
PRIMARY_KEY_COL = "test_id"
INT_COL = "test_int"
FLOAT_COL = "test_float"
STR_COL = "test_str"
UPDATE_DT_COL = "test_upddt"
COLUMNS = [PRIMARY_KEY_COL, INT_COL, FLOAT_COL, STR_COL, UPDATE_DT_COL]
STR_COLUMNS = ",".join(COLUMNS)
LINK_COLUMNS = (',\n' + ' ' * 12).join(["trg.{0} = src.{0}".format(col)
                                        for col in COLUMNS
                                        if col != PRIMARY_KEY_COL])
INS_COLUMNS = ",".join(["src.{0}".format(col) for col in COLUMNS])
DT = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
DT_STR = DT.isoformat(sep=' ', timespec='milliseconds')

CREATE_TABLE_SCRIPT = f"""
CREATE TABLE {TABLE_NAME}(
    [{PRIMARY_KEY_COL}] [bigint] IDENTITY(1,1) NOT NULL,
    [{INT_COL}] [bigint] NULL,
    [{FLOAT_COL}] [float] NULL,
    [{STR_COL}] [varchar](100) NULL,
    [{UPDATE_DT_COL}] [datetime] NULL,
 CONSTRAINT [PK_test] PRIMARY KEY CLUSTERED ([{PRIMARY_KEY_COL}] ASC)
) ON [PRIMARY];"""

CREATE_DB_SCRIPT = f"""
CREATE DATABASE [{WORK_DB_NAME}];
CREATE DATABASE [{CLEAR_DB_NAME}];"""

INIT_SCRIPT = f"""
USE [{WORK_DB_NAME}];
{CREATE_TABLE_SCRIPT}
USE [{CLEAR_DB_NAME}];
{CREATE_TABLE_SCRIPT}"""

TRUNCATE_SCRIPT = f"""
TRUNCATE TABLE {WORK_DB_NAME}.{TABLE_NAME};
TRUNCATE TABLE {CLEAR_DB_NAME}.{TABLE_NAME};
"""

DROP_SCRIPT = f"""
USE [master];
DROP DATABASE [{WORK_DB_NAME}];
DROP DATABASE [{CLEAR_DB_NAME}];"""


class TestDbTable(unittest.TestCase):
    templates = SqlServerTemplates()
    queries = SqlQueryBuilder(templates)
    connector = None
    cursor = None
    table = None
    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock(return_value=None)
    mock_cursor.fetchall = MagicMock(return_value=
                                     [[col,
                                       1 if col == UPDATE_DT_COL else 0,
                                       1 if col == PRIMARY_KEY_COL else 0]
                                      for col in COLUMNS])
    mock_table = DbTable(LOGGER_DICT_STUB, mock_cursor, queries, TABLE_NAME,
                         WORK_DB_NAME, CLEAR_DB_NAME)

    @classmethod
    def setUpClass(cls):
        if not IS_CONNECTED:
            return
        cls.connector = DbConnector()
        cls.cursor = cls.connector.get_cursor()
        cls.cursor.execute(CREATE_DB_SCRIPT)
        cls.cursor.execute(INIT_SCRIPT)
        cls.table = DbTable(LOGGER_DICT_STUB, cls.cursor, cls.queries,
                            TABLE_NAME, WORK_DB_NAME, CLEAR_DB_NAME)

    @classmethod
    def tearDownClass(cls):
        if cls.connector:
            cls.cursor.execute(DROP_SCRIPT)
            cls.connector.close()

    def setUp(self):
        if self.cursor:
            self.cursor.execute(TRUNCATE_SCRIPT)

    def __insert_data(self, database_name, values):
        ins_query = f"""
        SET IDENTITY_INSERT {database_name}.{TABLE_NAME} ON;
        INSERT INTO {database_name}.{TABLE_NAME}({STR_COLUMNS})
        VALUES {values};
        SET IDENTITY_INSERT {database_name}.{TABLE_NAME} OFF;
        """
        self.cursor.execute(ins_query)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test__init__(self):
        self.assertEqual(self.table.name, TABLE_NAME)

    def test__init__mock(self):
        table = DbTable(LOGGER_DICT_STUB, self.mock_cursor, self.queries,
                        TABLE_NAME, WORK_DB_NAME, CLEAR_DB_NAME)
        self.assertEqual(table.name, TABLE_NAME)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_subordinate_tables_empty(self):
        self.assertEqual(self.table.subordinate_tables, tuple())

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_subordinate_tables_single(self):
        self.assertEqual(self.table.subordinate_tables, False)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_subordinate_tables_multi(self):
        self.assertEqual(self.table.subordinate_tables, False)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_empty(self):
        self.assertEqual(self.table.get_delete_statement_list(), [])

    def test_get_delete_statement_list_empty_mock(self):
        self.mock_cursor.fetchall = MagicMock(return_value=[])
        self.assertEqual(self.mock_table.get_delete_statement_list(), [])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_single(self):
        values = f"(1,1,1.1,'test','{DT_STR}')"
        self.__insert_data(CLEAR_DB_NAME, values)
        str_id_list = "1"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.table.get_delete_statement_list(), [statement])

    def test_get_delete_statement_list_single_mock(self):
        self.mock_cursor.fetchall = MagicMock(return_value=[[123456789]])
        str_id_list = "123456789"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.mock_table.get_delete_statement_list(),
                         [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_multi(self):
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        self.__insert_data(CLEAR_DB_NAME, values)
        str_id_list = "1,2,3"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.table.get_delete_statement_list(), [statement])

    def test_get_delete_statement_list_multi_mock(self):
        self.mock_cursor.fetchall = MagicMock(return_value=[[123456789],
                                                            [987654321],
                                                            [111111111]])
        str_id_list = "123456789,987654321,111111111"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.mock_table.get_delete_statement_list(), [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_multi_row_limit(self):
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        self.__insert_data(CLEAR_DB_NAME, values)
        str_id_list = ["1", "2", "3"]
        statements = [
            self.templates.delete_statement.format(TABLE_NAME, PRIMARY_KEY_COL,
                                                   num)
            for num in str_id_list
        ]
        self.assertEqual(self.table.get_delete_statement_list(1), statements)

    def test_get_delete_statement_list_multi_row_limit_mock(self):
        self.mock_cursor.fetchall = MagicMock(return_value=[[123456789],
                                                            [987654321],
                                                            [111111111]])
        str_id_list = ["123456789", "987654321", "111111111"]
        statements = [
            self.templates.delete_statement.format(TABLE_NAME, PRIMARY_KEY_COL,
                                                   num)
            for num in str_id_list
        ]
        self.assertEqual(self.mock_table.get_delete_statement_list(1),
                         statements)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_empty(self):
        self.assertEqual(self.table.get_upsert_statement_list(), [])

    def test_get_upsert_statement_list_empty_mock(self):
        self.mock_cursor.fetchall = MagicMock(return_value=[])
        self.assertEqual(self.mock_table.get_upsert_statement_list(), [])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_single_row(self):
        values = "(123456789,123,1.23,'test','2022-03-30 23:19:14.777')"
        self.__insert_data(WORK_DB_NAME, values)
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(), [statement])

    def test_get_upsert_statement_list_single_row_mock(self):
        self.mock_cursor.fetchall = MagicMock(return_value=[[123456789, 123,
                                                             1.23, "test", DT]])
        values = f"(123456789,123,1.23,'test','{DT_STR}')"
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.mock_table.get_upsert_statement_list(),
                         [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_multi_rows(self):
        values = f"(123456787,123,1.23,'test',null)" + ',\n'+' ' * 8 +\
                 "(123456788,null,1.23,'''quoted''',null)" + ',\n'+' ' * 8 +\
                 f"(123456789,123,null,'test',null)"
        self.__insert_data(WORK_DB_NAME, values)
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(), [statement])

    def test_get_upsert_statement_list_multi_rows_mock(self):
        return_list = [[123456787, 123, 1.23, "test", DT],
                       [123456788, None, 1.23, "'quoted'", None],
                       [123456789, 123, None, "test", DT]]
        self.mock_cursor.fetchall = MagicMock(return_value=return_list)
        values = f"(123456787,123,1.23,'test','{DT_STR}')" + ',\n'+' ' * 8 +\
                 "(123456788,null,1.23,'''quoted''',null)" + ',\n'+' ' * 8 +\
                 f"(123456789,123,null,'test','{DT_STR}')"
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.mock_table.get_upsert_statement_list(),
                         [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_multi_row_limit(self):
        values = ["(123456787,123,1.23,'test',null)",
                  "(123456788,null,1.23,'''quoted''',null)"]
        self.__insert_data(WORK_DB_NAME, ','.join(values))
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values]
        self.assertEqual(self.table.get_upsert_statement_list(row_limit=1),
                         statements)

    def test_get_upsert_statement_list_multi_row_limit_mock(self):
        return_list = [[123456787, 123, 1.23, "test", DT],
                       [123456788, None, 1.23, "'quoted'", None]]
        self.mock_cursor.fetchall = MagicMock(return_value=return_list)
        values = [f"(123456787,123,1.23,'test','{DT_STR}')",
                  "(123456788,null,1.23,'''quoted''',null)"]
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values]
        self.assertEqual(self.mock_table.get_upsert_statement_list(row_limit=1),
                         statements)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_all_rows_empty(self):
        self.assertEqual(self.table.get_upsert_statement_list(all_rows=True),
                         [])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_all_rows(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  "(2,1,1.2,'b','1999-03-30 23:19:14.777')"]
        self.__insert_data(WORK_DB_NAME, ','.join(values))
        str_values = (',\n'+' ' * 8).join(values)
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           str_values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(all_rows=True),
                         [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_days_before_empty(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  "(2,1,1.2,'b','1999-03-30 23:19:14.777')"]
        self.__insert_data(WORK_DB_NAME, ','.join(values))
        self.assertEqual(self.table.get_upsert_statement_list(days_before=1),
                         [])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_days_before_single(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  f"(2,1,1.2,'b','{DT_STR}')"]
        self.__insert_data(WORK_DB_NAME, ','.join(values))
        str_values = values[1]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           str_values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(days_before=1),
                         [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_days_before_multi(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  f"(2,1,1.2,'b','{DT_STR}')", f"(3,1,1.2,'b','{DT_STR}')"]
        self.__insert_data(WORK_DB_NAME, ','.join(values))
        str_values = (',\n'+' ' * 8).join(values[1:])
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           str_values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(days_before=1),
                         [statement])


if __name__ == '__main__':
    unittest.main()
