import pyodbc

from testconfigreader import TestConfigReader


class DbConnector:
    def __init__(self):
        self.__config = TestConfigReader().get_config("connection")
        self.__connection = None
        self.__cursor = None

    def __del__(self):
        self.close()

    def get_cursor(self):
        if self.__cursor:
            return self.__cursor
        if self.__connection:
            self.__cursor = self.__connection.cursor()
            return self.__cursor
        self.__connection = pyodbc.connect(self.__config["conn_string"],
                                           autocommit=True)
        self.__cursor = self.__connection.cursor()
        return self.__cursor

    def check_and_close_connection(self):
        result = self.check_connection()
        self.close()
        return result

    def close(self):
        if self.__cursor:
            self.__cursor.close()
            self.__cursor = None
        if self.__connection:
            self.__connection.close()
            self.__connection = None

    def check_connection(self):
        if not self.__config:
            return False
        if self.__cursor:
            return True
        if self.__connection:
            try:
                self.__cursor = self.__connection.cursor()
                return True
            except Exception:
                return False
        try:
            self.__connection = pyodbc.connect(self.__config["conn_string"])
            self.__cursor = self.__connection.cursor()
            return True
        except Exception:
            return False
