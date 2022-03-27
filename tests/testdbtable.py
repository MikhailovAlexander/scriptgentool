import unittest
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
        "" : {
            "handlers": ["default"],
            "level": "CRITICAL",
            "propagate": True
        }
    }
}


class CursorStub(object):
    def __init__(self, fetchall_function):
        self.__fetchall_function = fetchall_function

    def execute(self, query, *args):
        pass

    def fetchall(self):
        return self.__fetchall_function()


class TestDbTable(unittest.TestCase):

    def test__init__(self):
        def stub():
            return [["test_id", 0, 1],
                    ["test_val", 0, 0],
                    ["test_upddt", 1, 0]]
        self.cursor = CursorStub(stub)
        self.queries = SqlQueryBuilder(SqlServerTemplates())
        self.table_name = "dbo.test"
        self.work_db_name = "work"
        self.clear_db_name = "clear"
        table = DbTable(LOGGER_DICT_STUB, self.cursor, self.queries,
                        self.table_name, self.work_db_name, self.clear_db_name)
        self.assertEqual(table.name, self.table_name)


if __name__ == '__main__':
    unittest.main()
