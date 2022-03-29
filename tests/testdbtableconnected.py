import unittest
from datetime import datetime
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
CREATE_TABLE_SCRIPT = f"""CREATE TABLE {TABLE_NAME}(
    [{PRIMARY_KEY_COL}] [bigint] IDENTITY(1,1) NOT NULL,
    [{INT_COL}] [bigint] NULL,
    [{FLOAT_COL}] [float] NULL,
    [{STR_COL}] [varchar](100) NULL,
    [{UPDATE_DT_COL}] [datetime] NULL,
 CONSTRAINT [PK_test] PRIMARY KEY CLUSTERED ([{PRIMARY_KEY_COL}] ASC)
) ON [PRIMARY];"""
CREATE_DB_SCRIPT = f"""CREATE DATABASE [{WORK_DB_NAME}];
CREATE DATABASE [{CLEAR_DB_NAME}];"""
INIT_SCRIPT = f"""USE [{WORK_DB_NAME}];
{CREATE_TABLE_SCRIPT}
USE [{CLEAR_DB_NAME}];
{CREATE_TABLE_SCRIPT}"""
DROP_SCRIPT = f"""USE [master];
DROP DATABASE [{WORK_DB_NAME}];
DROP DATABASE [{CLEAR_DB_NAME}];"""


class TestDbTableConnected(unittest.TestCase):
    templates = SqlServerTemplates()
    queries = SqlQueryBuilder(templates)
    connector = None
    cursor = None
    table = None

    @classmethod
    def setUpClass(cls):
        if not IS_CONNECTED:
            return
        cls.connector = DbConnector()
        cls.cursor = cls.connector.get_cursor()
        cls.table = DbTable(LOGGER_DICT_STUB, cls.cursor, cls.queries,
                            TABLE_NAME, WORK_DB_NAME, CLEAR_DB_NAME)
        cls.cursor.execute(CREATE_DB_SCRIPT)
        cls.cursor.commit()
        cls.cursor.execute(INIT_SCRIPT)
        cls.cursor.commit()

    @classmethod
    def tearDownClass(cls):
        if cls.connector:
            cls.cursor.execute(DROP_SCRIPT)
            cls.cursor.commit()
            cls.connector.close()

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test__init__(self):
        self.assertEqual(self.table.name, TABLE_NAME)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_something(self):
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
