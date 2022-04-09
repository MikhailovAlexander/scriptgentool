import os
import json


CONF_FILE_PATH = 'testconfig.json'


class TestConfigReader:
    def __init__(self):
        self.__config_dict = self.__read_config()

    def get_config(self, config_key=None):
        if not self.__config_dict:
            return None
        if not config_key:
            return self.__config_dict
        if config_key in self.__config_dict:
            return self.__config_dict[config_key]
        return None

    @staticmethod
    def __read_config():
        if not os.path.exists(CONF_FILE_PATH):
            return None
        connect_config = None
        with open(CONF_FILE_PATH, 'r') as conf_file:
            connect_config = json.load(conf_file)
        if connect_config:
            return connect_config
        return {}
