import logging
import logging.config
from pyodbc import Error as DbError, Cursor
from datetime import datetime, timedelta
from math import ceil

from core.sqlquerybuilder import SqlQueryBuilder


class DbColumn(object):
    """Вспомогательный класс для поля таблицы в БД"""
    
    def __init__(self, column_name, column_type, is_primary_key=False):
        self.name = column_name
        self.type = column_type
        self.is_primary_key = is_primary_key
    
    def __str__(self):
        """Возвращает нстроковое представление объекта"""
        return f'{self.name} ({self.type})'


class DbTable(object):
    """Класс для выгрузки скриптов модификации данных таблицы в БД"""
    
    def __init__(self, config_dict, cursor, table_name, work_db_name,
                 clear_db_name):
        logging.config.dictConfig(config_dict)
        self.__logger = logging.getLogger(__name__)
        self.__logger.info(f'table: {table_name}')
        self.__name = table_name
        self.__cursor = cursor
        self.__primary_key = None
        self.__update_dt_field = None
        self.__columns = []
        self.__work_db_name = work_db_name
        self.__clear_db_name = clear_db_name
        self.__get_columns()

    def __get_columns(self):
        """Получает данныые из БД о полях таблицы"""
        query = SqlQueryBuilder.get_column_query(self.__name)
        self.__logger.debug(f'get_column_query: {query}')
        try:
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
        except DbError as ex:
            self.__logger.exception(ex)
            self.__logger.error(f'query: {query}')
            raise RuntimeError('get_column_query run failed')
        for item in result:
            if item[3] == 1:  # is identity
                self.__primary_key = item[1]
                self.__columns.append(DbColumn(item[1], item[2], True))
            elif '_updDT' in item[1]:
                self.__update_dt_field = item[1]
                self.__columns.append(DbColumn(item[1], item[2]))
            else:
                self.__columns.append(DbColumn(item[1], item[2]))

    def _get_list_to_delete(self):
        """Возвращает список id в строковом представлении
        для удаления
        """
        query = SqlQueryBuilder.get_search_del_query(self.__primary_key, self.__name,
                                                     self.__work_db_name,
                                                     self.__clear_db_name)
        try:
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
        except DbError as ex:
            self.__logger.exception(ex)
            self.__logger.error(f'query: {query}')
            raise RuntimeError('get_search_del_query run failed')
        return [str(row[0]) for row in result]

    def _get_list_to_upsert(self, days_before):
        """Возвращает список строк данных для обновления/вставки

        days_before - период в днях для проверки обновлений

        """
        beg_date = None
        if days_before:
            beg_date = datetime.now() - timedelta(days=days_before)
            beg_date = beg_date.strftime("'%Y-%m-%d'")
        query = SqlQueryBuilder.get_search_upsert_query(self.__columns,
                                                        self.__work_db_name, self.__name,
                                                        self.__primary_key,
                                                        self.__update_dt_field,
                                                        self.__clear_db_name, beg_date)
        try:
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
        except DbError as ex:
            self.__logger.exception(ex)
            self.__logger.error(f'query: {query}')
            raise RuntimeError('get_search_upsert_query run failed')
        return [list(row) for row in result]

    def _get_all_rows(self):
        """Возвращает список всех строк таблицы"""
        query = SqlQueryBuilder.get_all_rows_query(self.__columns, self.__work_db_name,
                                                   self.__name)
        try:
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
        except DbError as ex:
            self.__logger.exception(ex)
            self.__logger.error(f'query: {query}')
            raise RuntimeError('get_all_rows_query run failed')
        return [list(row) for row in result]

    def get_delete_statement_list(self, row_limit=None):
        """возвращает список скриптов для удаления неактуальных строк
        в таблице
        
        row_limit - ограничение на количество id в одном скрипте
        
        """
        self.__logger.info(f'table: {self.__name}, row limit: {row_limit}')
        ids = self._get_list_to_delete()
        script_list = []
        if ids:
            if not row_limit:
                row_limit = len(ids)  # Все записи попадут в один скрипт
            for i in range(ceil(len(ids)/row_limit)):
                part = ids[i * row_limit: (i + 1) * row_limit]
                script_list.append(
                    SqlQueryBuilder.get_delete_statement(self.__name, self.__primary_key,
                                                         part)
                )
        return script_list

    def get_upsert_statement_list(self, days_before=None, row_limit=None,
                                  all_rows=False):
        """возвращает список скриптов для обновления/вставки
        строк в таблице
        
        days_before - период для проверки обновлений
        row_limit - ограничение на количество строк в одном скрипте
        all_rows - выгрузка всех строк таблиц
        
        """
        self.__logger.info(f'table: {self.__name}, days before: {days_before}, '
                          f'row limit: {row_limit}, all rows: {all_rows}')
        if all_rows:
            data = self._get_all_rows()
        else:
            data = self._get_list_to_upsert(days_before)
        script_list = []
        if data:
            if not row_limit:
                row_limit = len(data)  # Все записи попадут в один скрипт
            for i in range(ceil(len(data)/row_limit)):
                data_part = data[i * row_limit: (i + 1) * row_limit]
                script_list.append(
                    SqlQueryBuilder.get_upsert_statement(self.__name,
                                                         self.__columns,
                                                         data_part,
                                                         self.__primary_key)
                )
        return script_list
