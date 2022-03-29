import pyodbc
import os
import json


CONNECTION_CONF_FILE_PATH = 'dbconnect.json'


class DbConnector:
    def __init__(self):
        self.__config = self.__read_config()
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
        self.__connection = pyodbc.connect(self.__get_con_str(),
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
        con_str = self.__get_con_str()
        if not con_str:
            return False
        try:
            self.__connection = pyodbc.connect(con_str)
            self.__cursor = self.__connection.cursor()
            return True
        except Exception:
            return False

    @staticmethod
    def __read_config():
        if not os.path.exists(CONNECTION_CONF_FILE_PATH):
            return None
        connect_config = None
        with open(CONNECTION_CONF_FILE_PATH, 'r') as conf_file:
            connect_config = json.load(conf_file)
        return connect_config

    def __get_con_str(self):
        if not self.__config:
            return None
        db_serv = self.__config["server"]
        db_port = self.__config["port"]
        init_db_name = self.__config["init_db_name"]
        db_user = self.__config["user"]
        db_pass = self.__config["password"]
        return 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + db_serv +\
               ';PORT=' + db_port + ';DATABASE=' + init_db_name + ';UID=' + \
               db_user + ';PWD=' + db_pass + ';TrustServerCertificate=Yes'
