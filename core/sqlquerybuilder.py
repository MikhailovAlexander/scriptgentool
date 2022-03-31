from datetime import datetime
from typing import Union

from core.sqltemplates import SqlTemplates


class SqlQueryBuilder(object):
    """A class for create SQL queries and statements.

    Methods
    -------
    get_column_query(self, table_name: str) -> str:
        Builds an SQL query for getting table columns by table name.
    get_sub_tables_query(self, table_name: str, clear_db_name: str) -> str:
        Builds an SQL query for getting database table names containing
        foreign keys to this table.
    get_search_del_query(self, primary_key: str, table_name: str,
                        work_db_name: str, clear_db_name: str) -> str:
        Builds an SQL query for searching deleted rows in the target database
        table.
    get_search_upsert_query(self, column_list: list[str], work_db_name: str,
                            table_name: str, primary_key: str,
                            update_dt_field: str, clear_db_name: str,
                            beg_date: str = None) -> str:
        Builds an SQL query for searching updated or inserted rows in the
        target database table.
    get_all_rows_query(self, column_list: list[str], work_db_name: str,
                       table_name: str) -> str:
        Builds an SQL query for getting all rows from the target database table.
    get_delete_statement(self, table_name: str, primary_key: str,
                         id_list: list[str]) -> str:
        Builds an SQL statement for deleting rows from the database table.
    get_upsert_statement(self, table_name: str, column_list: list[str],
                         data: list[list[str]], primary_key: str) -> str:
        Builds an SQL statement for updating and inserting rows to the
        database table.
    """

    def __init__(self, templates: SqlTemplates):
        """
        :param templates: a SqlTemplates subclass implemented template
        properties.
        """
        self.__templates: SqlTemplates = templates

    def get_column_query(self, table_name: str) -> str:
        """Builds an SQL query for getting table columns by table name.

        :param table_name: the name of the target database table.
        :return: the text of the SQL query.
        """

        return self.__templates.column_query.format(table_name)

    def get_sub_tables_query(self, table_name: str) -> str:
        """Builds an SQL query for getting database table names containing
        foreign keys to this table.

        :param table_name: the name of the target database table.
        :return: the text of the SQL query.
        """
        return self.__templates.sub_tables_query.format(table_name)

    def get_search_del_query(self, primary_key: str, table_name: str,
                             work_db_name: str, clear_db_name: str) -> str:
        """Builds an SQL query for searching deleted rows in the target database
        table.

        :param primary_key: the name of the primary key column.
        :param table_name: the name of the target database table.
        :param work_db_name: the name of the work database.
        :param clear_db_name: the name of the clear database.
        :return: the text of the SQL query.
        """

        return self.__templates.search_del_query.format(primary_key, table_name,
                                                        work_db_name,
                                                        clear_db_name)

    def get_search_upsert_query(self, column_list: list[str], work_db_name: str,
                                table_name: str, primary_key: str,
                                update_dt_field: str, clear_db_name: str,
                                beg_date: str = None) -> str:
        """Builds an SQL query for searching updated or inserted rows in the
        target database table.

        :param column_list: the list of the column names for the table.
        :param work_db_name: the name of the work database.
        :param table_name: the name of the target database table.
        :param primary_key: the name of the primary key column.
        :param update_dt_field: the name of the column with update date.
        :param clear_db_name: the name of the clear database.
        :param beg_date: the start date to search updated or inserted rows.
        :return: the text of the SQL query.
        """

        query = self.__templates.search_upsert_query
        fields = SqlQueryBuilder.__get_columns_str(column_list, 'src.{0}')
        if beg_date:
            query += "\n\tand src.{4} >= {6}"
            return query.format(fields, work_db_name, table_name, primary_key,
                                update_dt_field, clear_db_name, beg_date)
        return query.format(fields, work_db_name, table_name, primary_key,
                            update_dt_field, clear_db_name)

    def get_all_rows_query(self, column_list: list[str], work_db_name: str,
                           table_name: str) -> str:
        """Builds an SQL query for getting all rows from the target database
        table.

        :param column_list: the list of the column names for the table.
        :param work_db_name: the name of the work database.
        :param table_name: the name of the target database table.
        :return: the text of the SQL query.
        """

        fields = SqlQueryBuilder.__get_columns_str(column_list, 'src.{0}')
        return self.__templates.all_rows_query.format(fields, work_db_name,
                                                      table_name)

    def get_delete_statement(self, table_name: str, primary_key: str,
                             id_list: list[str]) -> str:
        """Builds an SQL statement for deleting rows from the database table.

        :param table_name: the name of the target database table.
        :param primary_key: the name of the primary key column.
        :param id_list: the row identifiers list to delete.
        :return: the text of the SQL statement.
        """

        return self.__templates.delete_statement.format(table_name, primary_key,
                                                        ','.join(id_list))

    def get_upsert_statement(self, table_name: str, column_list: list[str],
                             data: list[list[Union[None, int, float, str,
                                                   datetime]]],
                             primary_key: str) -> str:
        """Builds an SQL statement for updating and inserting rows to the
        database table.

        :param table_name: the name of the target database table.
        :param column_list: the list of the column names for the table.
        :param data: the list of rows. Each row is a list of values.
        :raise TypeError: if the value type from the data not in
        Union[None, int, float, str, datetime].
        :param primary_key: the name of the primary key column.
        :return: the text of the SQL statement.
        """

        fields = SqlQueryBuilder.__get_columns_str(column_list)
        values = [SqlQueryBuilder.__get_str_value_row(row) for row in data]
        str_values = (',\n'+' ' * 8).join(values)
        upd_pattern = 'trg.{0} = src.{0}'
        upd_sep = ',\n'+' ' * 12
        upd_columns = [col for col in column_list if col != primary_key]
        upd_fields = SqlQueryBuilder.__get_columns_str(upd_columns, upd_pattern,
                                                       upd_sep)
        src_fields = SqlQueryBuilder.__get_columns_str(column_list, 'src.{0}')
        return self.__templates.upsert_statement.format(table_name, fields,
                                                        str_values, primary_key,
                                                        upd_fields, src_fields)

    @staticmethod
    def __get_columns_str(column_list: list[str], pattern: str = '{0}',
                          sep: str = ',') -> str:
        """Formats the list of the table columns with the separator.

        :param column_list: the list of the column names for the table.
        :param pattern: pattern to format column name.
        :param sep: string to separate column names.
        :return: formatted string presentation of the table columns.
        """

        return sep.join([pattern.format(col) for col in column_list])

    @staticmethod
    def __get_str_value_row(row: list[Union[None, int, float, str,
                                            datetime]]) -> str:
        """Formats the row to include in the SQL statement.

        :param row: the list of the values to format.
        :raise TypeError: if the value type from the row not in
        Union[None, int, float, str, datetime].
        :return: formatted string presentation of the row with the values.
        """
        str_values = [SqlQueryBuilder.__get_str_value(value) for value in row]
        return '(' + ','.join(str_values) + ')'

    @staticmethod
    def __get_str_value(value: Union[None, int, float, str, datetime]) -> str:
        """Formats the value to include in the SQL statement.

        :param value: value to format.
        :raise TypeError: if the value type not in Union[None, int, float, str,
        datetime].
        :return: formatted string presentation of the value.
        """
        if value is None:
            return 'null'
        elif type(value) == int or type(value) == float:
            return str(value)
        elif type(value) == str:
            return "'" + value.replace("'", "''") + "'"
        elif type(value) == datetime:
            return ("'" + value.isoformat(sep=' ', timespec='milliseconds')
                    + "'")
        raise TypeError(f'indefinite type to formatting: {type(value)}')
