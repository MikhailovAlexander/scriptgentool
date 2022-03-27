import unittest
from datetime import datetime
from core.sqlquerybuilder import SqlQueryBuilder
from core.sqlservertemplates import SqlServerTemplates

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
WORK_DB_NAME = "work"
CLEAR_DB_NAME = "clear"


class TestSqlQueryBuilder(unittest.TestCase):
    templates = SqlServerTemplates()
    builder = SqlQueryBuilder(templates)

    def test_get_column_query(self):
        query = self.templates.column_query.format(TABLE_NAME)
        self.assertEqual(self.builder.get_column_query(TABLE_NAME), query)

    def test_get_search_del_query(self):
        query = self.templates.search_del_query.format(PRIMARY_KEY_COL,
                                                       TABLE_NAME,
                                                       WORK_DB_NAME,
                                                       CLEAR_DB_NAME)
        self.assertEqual(self.builder.get_search_del_query(PRIMARY_KEY_COL,
                                                           TABLE_NAME,
                                                           WORK_DB_NAME,
                                                           CLEAR_DB_NAME),
                         query)

    def test_get_search_upsert_query_single_column(self):
        column_list = "single_column"
        update_dt_field = "update_column"
        beg_date = None
        query = self.templates.search_upsert_query.format("src." + column_list,
                                                          WORK_DB_NAME,
                                                          TABLE_NAME,
                                                          PRIMARY_KEY_COL,
                                                          update_dt_field,
                                                          CLEAR_DB_NAME)
        self.assertEqual(self.builder.get_search_upsert_query([column_list],
                                                              WORK_DB_NAME,
                                                              TABLE_NAME,
                                                              PRIMARY_KEY_COL,
                                                              update_dt_field,
                                                              CLEAR_DB_NAME,
                                                              beg_date),
                         query)

    def test_get_search_upsert_query_multi_columns(self):
        columns_str = ",".join(["src.{0}".format(col) for col in COLUMNS])
        beg_date = None
        query = self.templates.search_upsert_query.format(columns_str,
                                                          WORK_DB_NAME,
                                                          TABLE_NAME,
                                                          PRIMARY_KEY_COL,
                                                          UPDATE_DT_COL,
                                                          CLEAR_DB_NAME)
        self.assertEqual(self.builder.get_search_upsert_query(COLUMNS,
                                                              WORK_DB_NAME,
                                                              TABLE_NAME,
                                                              PRIMARY_KEY_COL,
                                                              UPDATE_DT_COL,
                                                              CLEAR_DB_NAME,
                                                              beg_date),
                         query)

    def test_get_search_upsert_query_multi_columns_with_date(self):
        columns_str = ",".join(["src.{0}".format(col) for col in COLUMNS])
        beg_date = "01.01.2022"
        template = self.templates.search_upsert_query + "\n\tand src.{4} >= {6}"
        query = template.format(columns_str, WORK_DB_NAME, TABLE_NAME,
                                PRIMARY_KEY_COL, UPDATE_DT_COL, CLEAR_DB_NAME,
                                beg_date)
        self.assertEqual(self.builder.get_search_upsert_query(COLUMNS,
                                                              WORK_DB_NAME,
                                                              TABLE_NAME,
                                                              PRIMARY_KEY_COL,
                                                              UPDATE_DT_COL,
                                                              CLEAR_DB_NAME,
                                                              beg_date),
                         query)

    def test_get_all_rows_query_single_column(self):
        column_list = "single_column"
        query = self.templates.all_rows_query.format("src." + column_list,
                                                     WORK_DB_NAME, TABLE_NAME)
        self.assertEqual(self.builder.get_all_rows_query([column_list],
                                                         WORK_DB_NAME,
                                                         TABLE_NAME),
                         query)

    def test_get_all_rows_query_multi_columns(self):
        columns_str = ",".join(["src.{0}".format(col) for col in COLUMNS])
        query = self.templates.all_rows_query.format(columns_str, WORK_DB_NAME,
                                                     TABLE_NAME)
        self.assertEqual(self.builder.get_all_rows_query(COLUMNS,
                                                         WORK_DB_NAME,
                                                         TABLE_NAME),
                         query)

    def test_get_delete_statement_single_value(self):
        id_list = "123"
        query = self.templates.delete_statement.format(TABLE_NAME,
                                                       PRIMARY_KEY_COL,
                                                       id_list)
        self.assertEqual(self.builder.get_delete_statement(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           [id_list]),
                         query)

    def test_get_delete_statement_multi_values(self):
        id_list = ["123", "4", "56"]
        str_id_list = ",".join(id_list)
        query = self.templates.delete_statement.format(TABLE_NAME,
                                                       PRIMARY_KEY_COL,
                                                       str_id_list)
        self.assertEqual(self.builder.get_delete_statement(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           id_list),
                         query)

    def test_get_upsert_statement_single_value(self):
        data = [[None]]
        values = "(null)"
        query = self.templates.upsert_statement.format(TABLE_NAME, STR_COLUMNS,
                                                       values, PRIMARY_KEY_COL,
                                                       LINK_COLUMNS,
                                                       INS_COLUMNS)
        self.assertEqual(self.builder.get_upsert_statement(TABLE_NAME,
                                                           COLUMNS, data,
                                                           PRIMARY_KEY_COL),
                         query)

    def test_get_upsert_statement_multi_value(self):
        dt = datetime.now()
        dt_str = dt.isoformat(sep=' ', timespec='milliseconds')
        data = [[None, 123, 1.23, "test", "'quoted'", dt]]
        values = f"(null,123,1.23,'test','''quoted''','{dt_str}')"
        query = self.templates.upsert_statement.format(TABLE_NAME, STR_COLUMNS,
                                                       values, PRIMARY_KEY_COL,
                                                       LINK_COLUMNS,
                                                       INS_COLUMNS)
        self.assertEqual(self.builder.get_upsert_statement(TABLE_NAME,
                                                           COLUMNS, data,
                                                           PRIMARY_KEY_COL),
                         query)

    def test_get_upsert_statement_multi_rows(self):
        dt = datetime.now()
        dt_str = dt.isoformat(sep=' ', timespec='milliseconds')
        data = [[None, 123, 1.23, "test", "'quoted'", dt],
                [1, 123, None, "TEST", "123", dt]]
        values = f"(null,123,1.23,'test','''quoted''','{dt_str}')" +\
                 ',\n'+' ' * 8 +\
                 f"(1,123,null,'TEST','123','{dt_str}')"
        query = self.templates.upsert_statement.format(TABLE_NAME, STR_COLUMNS,
                                                       values, PRIMARY_KEY_COL,
                                                       LINK_COLUMNS,
                                                       INS_COLUMNS)
        self.assertEqual(self.builder.get_upsert_statement(TABLE_NAME,
                                                           COLUMNS, data,
                                                           PRIMARY_KEY_COL),
                         query)

    def test_get_upsert_statement_type_error(self):
        data = [[tuple()]]
        self.assertRaises(TypeError, self.builder.get_upsert_statement,
                          TABLE_NAME, COLUMNS, data, PRIMARY_KEY_COL)


if __name__ == '__main__':
    unittest.main()
