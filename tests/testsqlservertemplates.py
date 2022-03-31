import unittest
from core.sqltemplates import SqlTemplates
from core.sqlservertemplates import SqlServerTemplates


class TestSqlServerTemplates(unittest.TestCase):
    def setUp(self) -> None:
        self.templates = SqlServerTemplates()

    def test_derive(self):
        self.assertIsInstance(self.templates, SqlTemplates)

    def test_column_query(self):
        column_query = (
            "select \n"
            "   c.name as ColumnName,\n"
            "   case when c.name like '%_updDT' then 1 else 0 end as IsUpdDT,\n"
            "   sign(c.status & 128) as IsIdentity\n"
            "from syscolumns as c\n"
            "   inner join systypes as t on c.xtype = t.xtype\n"
            "       and c.usertype = t.usertype\n"
            "where c.id = OBJECT_ID('{0}')\n"
            "and t.name != 'timestamp'\n"
            "order by c.colid\n")
        self.assertEqual(self.templates.column_query, column_query)

    def test_sub_tables_query(self):
        sub_tables_query = (
            "select object_schema_name(parent_object_id) + '.'\n"
            "    + object_name(parent_object_id) as key_name\n"
            "from sys.foreign_keys\n"
            "where referenced_object_id = object_id('{0}')\n"
            "    and type_desc = 'foreign_key_constraint';")
        self.assertEqual(self.templates.sub_tables_query, sub_tables_query)

    def test_search_del_query(self):
        search_del_query = (
            "select clr.{0}\n"
            "from {3}.{1} as clr\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {2}.{1} as src\n"
            "    where clr.{0} = src.{0});\n")
        self.assertEqual(self.templates.search_del_query, search_del_query)

    def test_search_upsert_query(self):
        search_upsert_query = (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {5}.{2} as clr\n"
            "    where clr.{3} = src.{3}\n"
            "        and clr.{4} = src.{4})")
        self.assertEqual(self.templates.search_upsert_query,
                         search_upsert_query)

    def test_all_rows_query(self):
        all_rows_query = (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n")
        self.assertEqual(self.templates.all_rows_query, all_rows_query)

    def test_delete_statement(self):
        delete_statement = (
            "delete from {0} where {1} in ({2});\n"
            "GO\n")
        self.assertEqual(self.templates.delete_statement, delete_statement)

    def test_upsert_statement(self):
        upsert_statement = (
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
            "GO\n")
        self.assertEqual(self.templates.upsert_statement, upsert_statement)

    def tearDown(self) -> None:
        self.templates = None


if __name__ == '__main__':
    unittest.main()
