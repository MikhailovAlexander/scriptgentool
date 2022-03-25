from core.sqlquerybuilder import SqlTemplates


class SqlServerTemplates(SqlTemplates):
    """The class contains templates for SQL queries and statements used with
    SQL Server databases.

    Properties
    -----------------
    column_query: str
        SQL query template for getting database table columns by table name
    search_del_query: str
        SQL query template for searching deleted rows in the database table.
    search_upsert_query: str
        SQL query template for searching updated rows in the database table.
    all_rows_query: str
        SQL query template for getting all rows from the database table.
    delete_statement: str
        SQL statement for deleting rows from the database table.
    upsert_statement: str
        SQL statement for updating and inserting rows to the database table.
    """

    @property
    def column_query(self) -> str:
        """SQL query template for getting database table columns by table name.
        Uses the name of the database table as a placeholder 0.
        """

        return (
            "select \n"
            "   c.colid ColumnID,\n"
            "   c.name as ColumnName,\n"
            "   t.name as TypeName,\n"
            "   sign(c.status & 128) as IsIdentity\n"
            "from syscolumns as c\n"
            "   inner join systypes as t on c.xtype = t.xtype\n"
            "       and c.usertype = t.usertype\n"
            "where c.id = OBJECT_ID('{0}')\n"
            "and t.name != 'timestamp'\n"
            "order by c.colid\n")

    @property
    def search_del_query(self) -> str:
        """SQL query template for searching deleted rows in the database table.
        Uses the name of the primary key column as a placeholder 0.
        Uses the name of the database table as a placeholder 1.
        Uses the name of the work database as a placeholder 2.
        Uses the name of the clear database as a placeholder 3.
        """

        return (
            "select clr.{0}\n"
            "from {3}.{1} as clr\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {2}.{1} as src\n"
            "    where clr.{0} = src.{0});\n")

    @property
    def search_upsert_query(self) -> str:
        """SQL query template for searching updated rows in the database table.
        Uses the column names list as a placeholder 0.
        Uses the name of the work database as a placeholder 1.
        Uses the name of the database table as a placeholder 2.
        Uses the name of the primary key column as a placeholder 3.
        Uses the name of the update date column as a placeholder 4.
        Uses the name of the clear database as a placeholder 5.

        Warning: please, don't add a semicolon at the end of query.
        Day count condition can be added at the end of this query.
        """

        return (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n"
            "where not exists(\n"
            "    select 1\n"
            "    from {5}.{2} as clr\n"
            "    where clr.{3} = src.{3}\n"
            "        and clr.{4} = src.{4})")

    @property
    def all_rows_query(self) -> str:
        """SQL query template for getting all rows from the database table.
        Uses the column names list as a placeholder 0.
        Uses the name of the work database as a placeholder 1.
        Uses the name of the clear database as a placeholder 3.
        """

        return (
            "select\n"
            "    {0}\n"
            "from {1}.{2} as src\n")

    @property
    def delete_statement(self) -> str:
        """SQL statement for deleting rows from the database table.
        Uses the name of the database table as a placeholder 0.
        Uses the name of the primary key column as a placeholder 1.
        Uses the row identifiers list as a placeholder 2.
        """

        return (
            "delete from {0} where {1} in ({2});\n"
            "GO\n")

    @property
    def upsert_statement(self) -> str:
        """SQL statement for updating and inserting rows to the database table.
        Uses the name of the database table as a placeholder 0.
        Uses the column names list as a placeholder 1.
        Uses the upsert values list as a placeholder 2.
        Uses the name of the primary key column as a placeholder 3.
        Uses the fields links list as a placeholder 4.
        Example: trg.column = src.column. The list must not contain a primary key.
        Uses the src.column list as a placeholder 5.
        """
        return (
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
