import unittest
from datetime import datetime
from core.sqlquerybuilder import SqlQueryBuilder
from core.sqlservertemplates import SqlServerTemplates


class TestSqlQueryBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.builder = SqlQueryBuilder(SqlServerTemplates())

    def test_get_column_query(self):
        table_name = "dbo.test"
        query = (
            "select \n"
            "   c.name as ColumnName,\n"
            "   case when c.name like '%_updDT' then 1 else 0 end as IsUpdDT,\n"
            "   sign(c.status & 128) as IsIdentity\n"
            "from syscolumns as c\n"
            "   inner join systypes as t on c.xtype = t.xtype\n"
            "       and c.usertype = t.usertype\n"
            "where c.id = OBJECT_ID('{0}')\n"
            "and t.name != 'timestamp'\n"
            "order by c.colid\n").format(table_name)
        self.assertEqual(self.builder.get_column_query(table_name), query)

    def test_get_search_del_query(self):
        primary_key = "primary_key"
        table_name = "dbo.test"
        work_db_name = "work"
        clear_db_name = "clear"
        query = (
            "select clr.{0}\n"
            "from {3}.{1} as clr\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {2}.{1} as src\n"
            "    where clr.{0} = src.{0});\n").format(primary_key, table_name,
                                                      work_db_name,
                                                      clear_db_name)
        self.assertEqual(self.builder.get_search_del_query(primary_key,
                                                           table_name,
                                                           work_db_name,
                                                           clear_db_name),
                         query)

    def test_get_search_upsert_query_single_column(self):
        column_list = "single_column"
        work_db_name = "work"
        table_name = "dbo.test"
        primary_key = "primary_key"
        update_dt_field = "update_column"
        clear_db_name = "clear"
        beg_date = None
        query = (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {5}.{2} as clr\n"
            "    where clr.{3} = src.{3}\n"
            "        and clr.{4} = src.{4})").format("src." + column_list,
                                                     work_db_name,
                                                     table_name, primary_key,
                                                     update_dt_field,
                                                     clear_db_name)
        self.assertEqual(self.builder.get_search_upsert_query([column_list],
                                                              work_db_name,
                                                              table_name,
                                                              primary_key,
                                                              update_dt_field,
                                                              clear_db_name,
                                                              beg_date),
                         query)

    def test_get_search_upsert_query_multi_columns(self):
        column_list = ["first_column", "second_column", "third_column"]
        columns_str = "src.first_column,src.second_column,src.third_column"
        work_db_name = "work"
        table_name = "dbo.test"
        primary_key = "primary_key"
        update_dt_field = "update_column"
        clear_db_name = "clear"
        beg_date = None
        query = (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {5}.{2} as clr\n"
            "    where clr.{3} = src.{3}\n"
            "        and clr.{4} = src.{4})").format(columns_str,
                                                     work_db_name,
                                                     table_name, primary_key,
                                                     update_dt_field,
                                                     clear_db_name)
        self.assertEqual(self.builder.get_search_upsert_query(column_list,
                                                              work_db_name,
                                                              table_name,
                                                              primary_key,
                                                              update_dt_field,
                                                              clear_db_name,
                                                              beg_date),
                         query)

    def test_get_search_upsert_query_multi_columns_with_date(self):
        column_list = ["first_column", "second_column", "third_column"]
        columns_str = "src.first_column,src.second_column,src.third_column"
        work_db_name = "work"
        table_name = "dbo.test"
        primary_key = "primary_key"
        update_dt_field = "update_column"
        clear_db_name = "clear"
        beg_date = "01.01.2022"
        query = (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {5}.{2} as clr\n"
            "    where clr.{3} = src.{3}\n"
            "        and clr.{4} = src.{4})"
            "\n\tand src.{4} >= {6}").format(columns_str, work_db_name,
                                             table_name, primary_key,
                                             update_dt_field, clear_db_name,
                                             beg_date)
        self.assertEqual(self.builder.get_search_upsert_query(column_list,
                                                              work_db_name,
                                                              table_name,
                                                              primary_key,
                                                              update_dt_field,
                                                              clear_db_name,
                                                              beg_date),
                         query)

    def test_get_all_rows_query_single_column(self):
        column_list = "single_column"
        work_db_name = "work"
        table_name = "dbo.test"
        query = (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n").format("src." + column_list, work_db_name,
                                            table_name)
        self.assertEqual(self.builder.get_all_rows_query([column_list],
                                                         work_db_name,
                                                         table_name),
                         query)

    def test_get_all_rows_query_multi_columns(self):
        column_list = ["first_column", "second_column", "third_column"]
        columns_str = "src.first_column,src.second_column,src.third_column"
        work_db_name = "work"
        table_name = "dbo.test"
        query = (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n").format(columns_str, work_db_name,
                                            table_name)
        self.assertEqual(self.builder.get_all_rows_query(column_list,
                                                         work_db_name,
                                                         table_name),
                         query)

    def test_get_delete_statement_single_value(self):
        table_name = "dbo.test"
        primary_key = "primary_key"
        id_list = "123"
        query = (
            "delete from {0} where {1} in ({2});\n"
            "GO\n").format(table_name, primary_key, id_list)
        self.assertEqual(self.builder.get_delete_statement(table_name,
                                                           primary_key,
                                                           [id_list]),
                         query)

    def test_get_delete_statement_multi_values(self):
        table_name = "dbo.test"
        primary_key = "primary_key"
        id_list = ["123", "4", "56"]
        str_id_list = "123,4,56"
        query = (
            "delete from {0} where {1} in ({2});\n"
            "GO\n").format(table_name, primary_key, str_id_list)
        self.assertEqual(self.builder.get_delete_statement(table_name,
                                                           primary_key,
                                                           id_list),
                         query)

    def test_get_upsert_statement_single_value(self):
        table_name = "dbo.test"
        column_list = ["first_column", "second_column", "primary_key"]
        columns = "first_column,second_column,primary_key"
        links = "trg.first_column = src.first_column" + ',\n'+' ' * 12 +\
                "trg.second_column = src.second_column"
        ins_columns = "src.first_column,src.second_column,src.primary_key"
        data = [[None]]
        values = "(null)"
        primary_key = "primary_key"
        query = (
            "set identity_insert {0} on;\n"
            "with src as(\n"
            "    select\n"
            "        {1}\n"
            "    from(values\n"
            "        {2}\n"
            "    ) as t_table(\n"
            "        {1}))\n"
            "merge {0} as trg\n"
            "    using src on trg.{3} = src.{3}\n"
            "    when matched then\n"
            "        update set\n"
            "            {4}\n"
            "    when not matched by target then\n"
            "        insert(\n"
            "        {1})\n"
            "        values(\n"
            "            {5});\n"
            "set identity_insert {0} off;\n"
            "GO\n").format(table_name, columns, values, primary_key, links,
                           ins_columns)
        self.assertEqual(self.builder.get_upsert_statement(table_name,
                                                           column_list, data,
                                                           primary_key),
                         query)

    def test_get_upsert_statement_multi_value(self):
        table_name = "dbo.test"
        column_list = ["first_column", "primary_key"]
        columns = "first_column,primary_key"
        links = "trg.first_column = src.first_column"
        ins_columns = "src.first_column,src.primary_key"
        dt = datetime.now()
        dt_str = dt.isoformat(sep=' ', timespec='milliseconds')
        data = [[None, 123, 1.23, "test", "'quoted'", dt]]
        values = f"(null,123,1.23,'test','''quoted''','{dt_str}')"
        primary_key = "primary_key"
        query = (
            "set identity_insert {0} on;\n"
            "with src as(\n"
            "    select\n"
            "        {1}\n"
            "    from(values\n"
            "        {2}\n"
            "    ) as t_table(\n"
            "        {1}))\n"
            "merge {0} as trg\n"
            "    using src on trg.{3} = src.{3}\n"
            "    when matched then\n"
            "        update set\n"
            "            {4}\n"
            "    when not matched by target then\n"
            "        insert(\n"
            "        {1})\n"
            "        values(\n"
            "            {5});\n"
            "set identity_insert {0} off;\n"
            "GO\n").format(table_name, columns, values, primary_key, links,
                           ins_columns)
        self.assertEqual(self.builder.get_upsert_statement(table_name,
                                                           column_list, data,
                                                           primary_key),
                         query)

    def test_get_upsert_statement_multi_rows(self):
        table_name = "dbo.test"
        column_list = ["first_column", "second_column", "primary_key"]
        columns = "first_column,second_column,primary_key"
        links = "trg.first_column = src.first_column" + ',\n'+' ' * 12 +\
                "trg.second_column = src.second_column"
        ins_columns = "src.first_column,src.second_column,src.primary_key"
        dt = datetime.now()
        dt_str = dt.isoformat(sep=' ', timespec='milliseconds')
        data = [[None, 123, 1.23, "test", "'quoted'", dt],
                [1, 123, None, "TEST", "123", dt]]
        values = f"(null,123,1.23,'test','''quoted''','{dt_str}')" +\
                 ',\n'+' ' * 8 +\
                 f"(1,123,null,'TEST','123','{dt_str}')"
        primary_key = "primary_key"
        query = (
            "set identity_insert {0} on;\n"
            "with src as(\n"
            "    select\n"
            "        {1}\n"
            "    from(values\n"
            "        {2}\n"
            "    ) as t_table(\n"
            "        {1}))\n"
            "merge {0} as trg\n"
            "    using src on trg.{3} = src.{3}\n"
            "    when matched then\n"
            "        update set\n"
            "            {4}\n"
            "    when not matched by target then\n"
            "        insert(\n"
            "        {1})\n"
            "        values(\n"
            "            {5});\n"
            "set identity_insert {0} off;\n"
            "GO\n").format(table_name, columns, values, primary_key, links,
                           ins_columns)
        self.assertEqual(self.builder.get_upsert_statement(table_name,
                                                           column_list, data,
                                                           primary_key),
                         query)

    def test_get_upsert_statement_type_error(self):
        table_name = "dbo.test"
        column_list = ["first_column", "second_column", "primary_key"]
        data = [[tuple()]]
        primary_key = "primary_key"
        self.assertRaises(TypeError, self.builder.get_upsert_statement,
                          table_name, column_list, data, primary_key)


if __name__ == '__main__':
    unittest.main()
