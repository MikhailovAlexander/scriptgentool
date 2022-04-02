from logging import Logger
import logging.config
import os
from datetime import datetime
from typing import Union


class FileWriter:
    """A class for writing scripts to created files.

    Properties
    ---------
    files(self) -> list[str]:
        returns a copy of the list of created files paths
    Methods
    -------
    save_scripts(self, scripts: list[str], prefix: str,
                 into_new_file: bool = False) -> None:
        Write scripts to created files.
    """
    
    def __init__(self, config_dict: dict[str: str], file_size_limit: int,
                 folder_path: str, liquibase_string: str):
        """
        :param config_dict: a dictionary with the logger configuration.
        :param file_size_limit: the maximum size of file with scripts.
        :param folder_path: a folder to create files.
        :param liquibase_string: the line to start the script file.
        """
        logging.config.dictConfig(config_dict)
        self.__logger: Logger = logging.getLogger(__name__)
        self.__logger.info(f'file_size_limit: {file_size_limit}, '
                           f'folder_path: {folder_path}')
        self.__files: list[str] = []
        self.__cur_name: Union[str, None] = None
        self.__cur_size: int = file_size_limit + 1
        self.__limit: int = file_size_limit
        self.__folder_path: str = folder_path
        self.__liquibase_string: str = liquibase_string

    @property
    def files(self) -> list[str]:
        """
        :return: a copy of the list of created files paths
        """

        return self.__files.copy()

    def save_scripts(self, scripts: list[str], prefix: str,
                     into_new_file: bool = False) -> None:
        """Write scripts to created files.

        :param scripts: the list of script to saving into the files.
        :param prefix: a string to start the file name.
        :param into_new_file: if True start writing from a new file, otherwise
        from the current file.
        :raise RuntimeError: when the number of generated files with the same
        datetime exceeds 99.
        :return: None
        """

        self.__logger.info(f'{len(scripts)} scripts, '
                           f'prefix: {prefix}, into_new_file: {into_new_file}')
        if into_new_file:
            self.__cur_size = self.__limit + 1
        for script in scripts:
            if self.__cur_size > self.__limit:
                self.__cur_name = self.__generate_file_name(prefix)
                self.__files.append(os.path.abspath(self.__cur_path))
                self.__cur_size = 0
            self.__add_script_to_file(script)
            self.__cur_size = os.path.getsize(self.__cur_path)
            self.__logger.debug(f'file path: {self.__cur_path}, '
                                f'size: {self.__cur_size}')

    @property
    def __cur_path(self) -> str:
        """
        :return: path to the last created file.
        """

        if self.__cur_name:
            return self.__folder_path + '/' + self.__cur_name

    def __generate_file_name(self, prefix: str) -> str:
        """Generates a name for a new file starting with the prefix and
        the current datetime and ending with the order number.

        :param prefix: a string to start the file name.
        :raise RuntimeError: when the number of generated files with the same
        datetime exceeds 99.
        :return: the file name.
        """

        name = datetime.now().strftime(f'{prefix}%Y%m%d%H%M')
        path = self.__folder_path + '/' + name
        file_num = 1
        while os.path.exists(path + '{:02}.sql'.format(file_num)):
            if file_num > 99:
                raise RuntimeError(f'Over then 99 with name: {name}')
            file_num += 1
        return f'{name}{file_num:02}.sql'

    def __add_script_to_file(self, script: str) -> None:
        """Writes the script into the current file.

        :param script: a script text.
        :return: None.
        """

        write_mode = 'w'
        if os.path.exists(self.__cur_path):
            write_mode = 'a'
        self.__logger.debug(f'file path: {self.__cur_path}, mode: {write_mode}')
        with open(self.__cur_path, write_mode, encoding="utf-8") as file:
            if write_mode == 'w':
                file.write(self.__liquibase_string)
            file.write(script)
