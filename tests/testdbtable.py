import unittest
from unittest.mock import MagicMock
from datetime import datetime
from core.dbtable import DbTable
from core.sqlquerybuilder import SqlQueryBuilder
from core.sqlservertemplates import SqlServerTemplates

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
TABLE_NAME = "dbo.test"
PRIMARY_KEY_COL = "test_id"
UPDATE_DT_COL = "test_upddt"
INT_COL = "test_int"
FLOAT_COL = "test_float"
STR_COL = "test_str"
COLUMNS = [PRIMARY_KEY_COL, INT_COL, FLOAT_COL, STR_COL, UPDATE_DT_COL]
STR_COLUMNS = ",".join(COLUMNS)
LINK_COLUMNS = (',\n' + ' ' * 12).join(["trg.{0} = src.{0}".format(col)
                                        for col in COLUMNS
                                        if col != PRIMARY_KEY_COL])
INS_COLUMNS = ",".join(["src.{0}".format(col) for col in COLUMNS])
DT = datetime.now()
DT_STR = DT.isoformat(sep=' ', timespec='milliseconds')
WORK_DB_NAME = "work"
CLEAR_DB_NAME = "clear"


class TestDbTable(unittest.TestCase):
    templates = SqlServerTemplates()
    cursor = MagicMock()
    cursor.execute = MagicMock(return_value=None)
    cursor.fetchall = MagicMock(return_value=
                                [[col,
                                  1 if col == UPDATE_DT_COL else 0,
                                  1 if col == PRIMARY_KEY_COL else 0]
                                 for col in COLUMNS])
    queries = SqlQueryBuilder(templates)
    table = DbTable(LOGGER_DICT_STUB, cursor, queries, TABLE_NAME, WORK_DB_NAME,
                    CLEAR_DB_NAME)

    def test__init__(self):
        table = DbTable(LOGGER_DICT_STUB, self.cursor, self.queries, TABLE_NAME,
                        WORK_DB_NAME, CLEAR_DB_NAME)
        self.assertEqual(table.name, TABLE_NAME)

    def test_get_delete_statement_list_empty(self):
        self.cursor.fetchall = MagicMock(return_value=[])
        self.assertEqual(self.table.get_delete_statement_list(), [])

    def test_get_delete_statement_list_single(self):
        self.cursor.fetchall = MagicMock(return_value=[[123456789]])
        str_id_list = "123456789"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.table.get_delete_statement_list(), [statement])

    def test_get_delete_statement_list_multi(self):
        self.cursor.fetchall = MagicMock(return_value=[[123456789], [987654321],
                                                       [111111111]])
        str_id_list = "123456789,987654321,111111111"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        self.assertEqual(self.table.get_delete_statement_list(), [statement])

    def test_get_delete_statement_list_multi_row_limit(self):
        self.cursor.fetchall = MagicMock(return_value=[[123456789], [987654321],
                                                       [111111111]])
        str_id_list = ["123456789", "987654321", "111111111"]
        statements = [
            self.templates.delete_statement.format(TABLE_NAME, PRIMARY_KEY_COL,
                                                   num)
            for num in str_id_list
        ]
        self.assertEqual(self.table.get_delete_statement_list(1), statements)

    def test_get_upsert_statement_list_empty(self):
        self.cursor.fetchall = MagicMock(return_value=[])
        self.assertEqual(self.table.get_upsert_statement_list(), [])

    def test_get_upsert_statement_list_single_row(self):
        self.cursor.fetchall = MagicMock(return_value=[[123456789, 123, 1.23,
                                                        "test", DT]])
        values = f"(123456789,123,1.23,'test','{DT_STR}')"
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(), [statement])

    def test_get_upsert_statement_list_multi_rows(self):
        return_list = [[123456787, 123, 1.23, "test", DT],
                       [123456788, None, 1.23, "'quoted'", None],
                       [123456789, 123, None, "test", DT]]
        self.cursor.fetchall = MagicMock(return_value=return_list)
        values = f"(123456787,123,1.23,'test','{DT_STR}')" + ',\n'+' ' * 8 +\
                 "(123456788,null,1.23,'''quoted''',null)" + ',\n'+' ' * 8 +\
                 f"(123456789,123,null,'test','{DT_STR}')"

        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        self.assertEqual(self.table.get_upsert_statement_list(), [statement])

    def test_get_upsert_statement_list_multi_row_limit(self):
        return_list = [[123456787, 123, 1.23, "test", DT],
                       [123456788, None, 1.23, "'quoted'", None]]
        self.cursor.fetchall = MagicMock(return_value=return_list)
        values = [f"(123456787,123,1.23,'test','{DT_STR}')",
                  "(123456788,null,1.23,'''quoted''',null)"]
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
