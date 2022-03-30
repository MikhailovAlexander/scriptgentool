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
COLUMNS = [PRIMARY_KEY_COL, INT_COL, FLOAT_COL, STR_COL, UPDATE_DT_COL]
STR_COLUMNS = ",".join(COLUMNS)
LINK_COLUMNS = (',\n' + ' ' * 12).join(["trg.{0} = src.{0}".format(col)
                                        for col in COLUMNS
                                        if col != PRIMARY_KEY_COL])
INS_COLUMNS = ",".join(["src.{0}".format(col) for col in COLUMNS])
DT = datetime.now()
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
        self.cursor.execute(TRUNCATE_SCRIPT)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test__init__(self):
        self.assertEqual(self.table.name, TABLE_NAME)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_empty(self):
        self.assertEqual(self.table.get_delete_statement_list(), [])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_single(self):
        ins_query = f"""
        SET IDENTITY_INSERT {CLEAR_DB_NAME}.{TABLE_NAME} ON;
        INSERT INTO {CLEAR_DB_NAME}.{TABLE_NAME}({STR_COLUMNS})
        VALUES(1,1,1.1,'test','{DT_STR}');
        SET IDENTITY_INSERT {CLEAR_DB_NAME}.{TABLE_NAME} OFF;
        """
        self.cursor.execute(ins_query)
        str_id_list = "1"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.table.get_delete_statement_list(), [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_multi(self):
        ins_query = f"""
        SET IDENTITY_INSERT {CLEAR_DB_NAME}.{TABLE_NAME} ON;
        INSERT INTO {CLEAR_DB_NAME}.{TABLE_NAME}({STR_COLUMNS})
        VALUES(1,1,1.1,'test','{DT_STR}'),
        (2,1,1.1,'test','{DT_STR}'),
        (3,1,1.1,'test','{DT_STR}');
        SET IDENTITY_INSERT {CLEAR_DB_NAME}.{TABLE_NAME} OFF;
        """
        self.cursor.execute(ins_query)
        str_id_list = "1,2,3"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.table.get_delete_statement_list(), [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_delete_statement_list_multi_row_limit(self):
        ins_query = f"""
        SET IDENTITY_INSERT {CLEAR_DB_NAME}.{TABLE_NAME} ON;
        INSERT INTO {CLEAR_DB_NAME}.{TABLE_NAME}({STR_COLUMNS})
        VALUES(1,1,1.1,'test','{DT_STR}'),
        (2,1,1.1,'test','{DT_STR}'),
        (3,1,1.1,'test','{DT_STR}');
        SET IDENTITY_INSERT {CLEAR_DB_NAME}.{TABLE_NAME} OFF;
        """
        self.cursor.execute(ins_query)
        str_id_list = ["1", "2", "3"]
        statements = [
            self.templates.delete_statement.format(TABLE_NAME, PRIMARY_KEY_COL,
                                                   num)
            for num in str_id_list
        ]
        self.assertEqual(self.table.get_delete_statement_list(1), statements)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_empty(self):
        self.assertEqual(self.table.get_upsert_statement_list(), [])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_single_row(self):
        values = "(123456789,123,1.23,'test','2022-03-30 23:19:14.777')"
        ins_query = f"""
        SET IDENTITY_INSERT {WORK_DB_NAME}.{TABLE_NAME} ON;
        INSERT INTO {WORK_DB_NAME}.{TABLE_NAME}({STR_COLUMNS})
        VALUES{values};
        SET IDENTITY_INSERT {WORK_DB_NAME}.{TABLE_NAME} OFF;
        """
        self.cursor.execute(ins_query)
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(), [statement])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_get_upsert_statement_list_multi_rows(self):
        values = f"(123456787,123,1.23,'test',null)" + ',\n'+' ' * 8 +\
                 "(123456788,null,1.23,'''quoted''',null)" + ',\n'+' ' * 8 +\
                 f"(123456789,123,null,'test',null)"
        ins_query = f"""
        SET IDENTITY_INSERT {WORK_DB_NAME}.{TABLE_NAME} ON;
        INSERT INTO {WORK_DB_NAME}.{TABLE_NAME}({STR_COLUMNS})
        VALUES{values};
        SET IDENTITY_INSERT {WORK_DB_NAME}.{TABLE_NAME} OFF;
        """
        self.cursor.execute(ins_query)
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(), [statement])

    def test_get_upsert_statement_list_multi_row_limit(self):
        values = [f"(123456787,123,1.23,'test',null)",
                  "(123456788,null,1.23,'''quoted''',null)"]
        ins_query = f"""
        SET IDENTITY_INSERT {WORK_DB_NAME}.{TABLE_NAME} ON;
        INSERT INTO {WORK_DB_NAME}.{TABLE_NAME}({STR_COLUMNS})
        VALUES{','.join(values)};
        SET IDENTITY_INSERT {WORK_DB_NAME}.{TABLE_NAME} OFF;
        """
        self.cursor.execute(ins_query)
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values]
        self.assertEqual(self.table.get_upsert_statement_list(row_limit=1),
                         statements)


if __name__ == '__main__':
    unittest.main()
