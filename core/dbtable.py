from logging import Logger
import logging.config
from pyodbc import Error as DbError, Cursor
from datetime import datetime, timedelta
from math import ceil
from typing import Union

from core.sqlquerybuilder import SqlQueryBuilder


class DbTable(object):
    """A class for generate SQL statements to migrate work database to clear.

    Properties
    ---------
    name(self) -> str:
        Returns the name of database table.

    Methods
    -------
    get_delete_statement_list(self, row_limit: int = None) -> list[str]:
        Compares the data of two database(work and clear), searches id rows
        to delete and generate the necessary SQL statements to migrate work
        database to clear. Statements are packaged into scripts by constraint
        row_limit.
    get_upsert_statement_list(self, days_before: int = None,
                                  row_limit: int = None,
                                  all_rows: bool = False):
        Compares the data of two database(work and clear), searches rows
        to update or insert and generate the necessary SQL statements to migrate
        work database to clear. Statements are packaged into scripts by
        constraint row_limit. Searches database diffs by the number of days
        (before the current date) received in the days_before parameter.
        If the all_rows parameter is True, uploads all rows from table in the
        work database.
    """
    
    def __init__(self, config_dict: dict[str: str], cursor: Cursor,
                 queries: SqlQueryBuilder, table_name: str, work_db_name: str,
                 clear_db_name: str):
        """
        :param config_dict: a dictionary with the logger configuration.
        :param cursor: a database cursor for executing SQL queries.
        :param queries: an SqlQueryBuilder class instance to build SQL queries
        and statements.
        :param table_name: the name of the target database table.
        :param work_db_name: the name of the work database.
        :param clear_db_name: the name of the clear database.
        :raise RuntimeError: if database query (search table columns) execution
        failed.
        """

        logging.config.dictConfig(config_dict)
        self.__logger: Logger = logging.getLogger(__name__)
        self.__logger.info(f"table: {table_name}")
        self.__cursor: Cursor = cursor
        self.__queries: SqlQueryBuilder = queries
        self.__name: str = table_name
        self.__primary_key: str = ""
        self.__update_dt_field: str = ""
        self.__columns: list[str] = []
        self.__work_db_name: str = work_db_name
        self.__clear_db_name: str = clear_db_name
        self.__set_columns()

    @property
    def name(self) -> str:
        """
        :return: the name of database table.
        """
        return self.__name

    def get_delete_statement_list(self, row_limit: int = None) -> list[str]:
        """Compares the data of two database(work and clear), searches id rows
        to delete and generate the necessary SQL statements to migrate work
        database to clear. Statements are packaged into scripts by constraint
        row_limit.

        :param row_limit: the maximum number of ids in one script. If the
        row_limit parameter is not filled in, all statements will be packed
        into one script.
        :raise RuntimeError: if database query execution failed.
        :return: the list of scripts with delete statements.
        """

        self.__logger.info(f'table: {self.__name}, row limit: {row_limit}')
        ids = self.__get_list_to_delete()
        script_list = []
        if ids:
            if not row_limit:
                row_limit = len(ids)
            for i in range(ceil(len(ids)/row_limit)):
                ids_part = ids[i * row_limit: (i + 1) * row_limit]
                script_list.append(
                    self.__queries.get_delete_statement(self.__name,
                                                        self.__primary_key,
                                                        ids_part)
                )
        return script_list

    def get_upsert_statement_list(self, days_before: int = None,
                                  row_limit: int = None,
                                  all_rows: bool = False):
        """Compares the data of two database(work and clear), searches rows
        to update or insert and generate the necessary SQL statements to migrate
        work database to clear. Statements are packaged into scripts by
        constraint row_limit. Searches database diffs by the number of days
        (before the current date) received in the days_before parameter.
        If the all_rows parameter is True, uploads all rows from table in the
        work database.

        :param days_before: the number of days (before the current date) to
        search database diffs.
        :param row_limit: the maximum number of ids in one script. If the
        row_limit parameter is not filled in, all statements will be packed
        into one script.
        :param all_rows: if True uploads all rows from table in the work
        database, otherwise uploads diffs between work and clear databases.
        :raise RuntimeError: if database query execution failed.
        :return: the list of scripts with insert/update statements.
        """

        self.__logger.info(f'table: {self.__name}, days before: {days_before}, '
                           f'row limit: {row_limit}, all rows: {all_rows}')
        if all_rows:
            data = self.__get_all_rows()
        else:
            data = self.__get_list_to_upsert(days_before)
        script_list = []
        if data:
            if not row_limit:
                row_limit = len(data)
            for i in range(ceil(len(data)/row_limit)):
                data_part = data[i * row_limit: (i + 1) * row_limit]
                script_list.append(
                    self.__queries.get_upsert_statement(self.__name,
                                                        self.__columns,
                                                        data_part,
                                                        self.__primary_key)
                )
        return script_list

    def __set_columns(self) -> None:
        """Gets table columns info from the database."""
        query = self.__queries.get_column_query(self.__name)
        self.__logger.debug(f'get_column_query: {query}')
        result = self.__get_query_result(query)
        for item in result:
            column_name = item[0]
            is_update_dt = bool(item[0])
            is_primary_key = bool(item[2])
            if is_primary_key:
                self.__primary_key = column_name
            elif is_update_dt:
                self.__update_dt_field = column_name
            self.__columns.append(column_name)

    def __get_list_to_delete(self) -> list[str]:
        """Gets id rows to delete."""
        query = self.__queries.get_search_del_query(self.__primary_key,
                                                    self.__name,
                                                    self.__work_db_name,
                                                    self.__clear_db_name)
        result = self.__get_query_result(query)
        return [str(row[0]) for row in result]

    def __get_list_to_upsert(self, days_before: int) \
            -> list[list[Union[None, int, float, str, datetime]]]:
        """Gets rows data to update or insert."""
        beg_date = None
        if days_before:
            beg_date = datetime.now() - timedelta(days=days_before)
            beg_date = beg_date.strftime("'%Y-%m-%d'")
        query = self.__queries.get_search_upsert_query(self.__columns,
                                                       self.__work_db_name,
                                                       self.__name,
                                                       self.__primary_key,
                                                       self.__update_dt_field,
                                                       self.__clear_db_name,
                                                       beg_date)
        result = self.__get_query_result(query)
        return [list(row) for row in result]

    def __get_all_rows(self) -> list[list[Union[None, int, float, str,
                                                datetime]]]:
        """Gets all rows data to insert."""
        query = self.__queries.get_all_rows_query(self.__columns,
                                                  self.__work_db_name,
                                                  self.__name)
        result = self.__get_query_result(query)
        return [list(row) for row in result]

    def __get_query_result(self, query: str) -> list[list[Union[None, int,
                                                                float, str,
                                                                datetime]]]:
        """Executes SQL query and gets the query result."""
        result = []
        try:
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
        except DbError as ex:
            self.__logger.exception(ex)
            self.__logger.error(f'query: {query}')
            raise RuntimeError('query execution failed')
        return result
