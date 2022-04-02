from logging import Logger
import logging.config
import os
import subprocess
from git import Repo, Remote,  GitError
from datetime import datetime
from pyodbc import Cursor
from typing import Union

from core.dbtable import DbTable
from core.filewriter import FileWriter
from core.sqlquerybuilder import SqlQueryBuilder
from core.sqlservertemplates import SqlServerTemplates


class ScriptGenerator:
    """A class for create script files with the SQL statements to migrate work
    database to clear.

    Methods
    -------
    upsert_tables(self, file_size_limit: int, message: str,
                  days_before: int = None, row_limit: int = None) -> None:
        Creates script files with the SQL statements to migrate
        work database to clear. Statements are packaged into scripts by
        constraint row_limit. Searches database diffs by the number of days
        (before the current date) received in the days_before parameter.
        Generated scripts applied the clear database with the liquibase.
        Created files committed into the git repository with tho commit message
        from the message parameter.
    upload_tables(self, file_size_limit: int, message: str,
                  row_limit: int = None) -> None:
        Creates script files with the SQL statements to migrate all tables
        rows from the work database to clear. Statements are packaged into
        scripts by constraint row_limit.
        Generated scripts applied the clear database with the liquibase.
        Created files committed into the git repository with tho commit message
        from the message parameter.
    """

    def __init__(self, config_dict: dict[str: str], cursor: Cursor,
                 work_db_name: str, clear_db_name: str, git_folder_path: str,
                 target_folder: str, table_settings: dict[str: str],
                 liquibase_settings: dict[str: str]):
        """
        :param config_dict: a dictionary with the logger configuration.
        :param cursor: a database cursor for executing SQL queries.
        :param work_db_name: the name of the work database.
        :param clear_db_name: the name of the clear database.
        :param git_folder_path:
        :param target_folder:
        :param table_settings:
        :param liquibase_settings:
        """
        self.__config_dict: dict[str: str] = config_dict
        logging.config.dictConfig(config_dict)
        self.__logger: Logger = logging.getLogger(__name__)
        self.__logger.info(f'work_db: {work_db_name}, '
                           f'git_folder: {git_folder_path}, '
                           f'tree folder: {target_folder}')

        self.__git_folder_path: str = git_folder_path
        self.__target_folder: str = target_folder
        self.__repo: Union[Repo, None] = None
        self.__origin: Union[Remote, None] = None
        self.__changelog_filepath = None
        self.__init_git_objects()
            
        self.__cursor: Cursor = cursor
        self.__work_db_name: str = work_db_name
        self.__clear_db_name: str = clear_db_name
        self.__liquibase_settings: dict[str: str] = liquibase_settings
        self.__db_table_list:  list[DbTable] = self.__get_db_table_list(
            table_settings["table_list"])
        self.__upsert_only_list: list[str] = table_settings["upsert_only_list"]
        self.__delete_only_list: list[str] = table_settings["delete_only_list"]

    @property
    def __target_folder_path(self) -> str:
        """
        :return: a path to target folder.
        """

        return self.__git_folder_path + "/" + self.__target_folder

    def upsert_tables(self, file_size_limit: int, message: str,
                      days_before: int = None, row_limit: int = None) -> None:
        """Creates script files with the SQL statements to migrate
        work database to clear. Statements are packaged into scripts by
        constraint row_limit. Searches database diffs by the number of days
        (before the current date) received in the days_before parameter.
        Generated scripts applied the clear database with the liquibase.
        Created files committed into the git repository with tho commit message
        from the message parameter.

        :param file_size_limit: the maximum size of file with scripts.
        :param message: the commit message for the git repository.
        :param days_before: the number of days (before the current date) to
        search database diffs.
        :param row_limit: the maximum number of rows in one script.
        :raise RuntimeError: if the clear database update with the liquibase
        failed.
        :raise RuntimeError: if git pull/push repeating fails over then 3 times.
        :return: None
        """

        self.__logger.info(f"file_size_limit: {file_size_limit},  days_before:"
                           f"{days_before}, row_limit: {row_limit}")
        saver = FileWriter(self.__config_dict, file_size_limit,
                           self.__target_folder_path,
                           self.__liquibase_settings["liquibase_string"])
        for db_table in [tb for tb in self.__db_table_list
                         if tb.name not in self.__delete_only_list]:
            scripts = db_table.get_upsert_statement_list(days_before, row_limit)
            saver.save_scripts(scripts, "Rep")
        for db_table in [tb for tb in self.__db_table_list[::-1]
                         if tb.name not in self.__upsert_only_list]:
            scripts = db_table.get_delete_statement_list(row_limit)
            saver.save_scripts(scripts, "Rep")
        self.__logger.info(f"{len(saver.files)} was generated")
        if saver.files:
            self.__update_changelog([os.path.basename(file)
                                     for file in saver.files])
            self.__update_clear_db()
            self.__commit_files(saver.files, message)

    def upload_tables(self, file_size_limit: int, message: str,
                      row_limit: int = None) -> None:
        """Creates script files with the SQL statements to migrate all tables
        rows from the work database to clear. Statements are packaged into
        scripts by constraint row_limit.
        Generated scripts applied the clear database with the liquibase.
        Created files committed into the git repository with tho commit message
        from the message parameter.

        :param file_size_limit: the maximum size of file with scripts.
        :param message: the commit message for the git repository.
        :param row_limit: the maximum number of rows in one script.
        :raise RuntimeError: if the clear database update with the liquibase
        failed.
        :raise RuntimeError: if git pull/push repeating fails over then 3 times.
        :return: None
        """
        self.__logger.info(f'file_size_limit: {file_size_limit}, '
                           f'row_limit: {row_limit}')
        saver = FileWriter(self.__config_dict, file_size_limit,
                           self.__target_folder_path,
                           self.__liquibase_settings['liquibase_string'])
        for db_table in self.__db_table_list:
            scripts = db_table.get_upsert_statement_list(row_limit=row_limit,
                                                         all_rows=True)
            saver.save_scripts(scripts, db_table.name.replace('.', '_'),
                               into_new_file=True)
        self.__update_changelog([os.path.basename(file)
                                 for file in saver.files])
        self.__update_clear_db()
        self.__commit_files(saver.files, message)

    def __init_git_objects(self) -> None:
        """Sets private attributes to work with Git objects.

        :raise NotADirectoryError: if git repository folder does not exist.
        :return: None
        """

        self.__logger.info("run")
        if not os.path.exists(self.__git_folder_path):
            raise NotADirectoryError(
                f"git_folder_path {self.__git_folder_path} does not exist")
        self.__repo = Repo(self.__git_folder_path)
        self.__origin = self.__repo.remote(name="origin")
        try:
            self.__origin.pull()
            self.__origin.push()
        except GitError as ex:
            self.__logger.exception(ex)
            self.__git_pull_push_repeat(pull_repeat=True, push_repeat=True)
        changelog_name = datetime.now().strftime("changelog_tree%Y%m.yml")
        changelog_name = self.__target_folder_path + "/" + changelog_name
        if not os.path.exists(changelog_name):
            self.__logger.info("New yml creating run")
            with open(changelog_name, "tw", encoding="utf-8") as file:
                file.write("databaseChangeLog:")
            self.__commit_files([os.path.abspath(changelog_name)],
                                "new month changelog add",
                                is_new_changelog=True)
        self.__changelog_filepath = changelog_name

    def __get_db_table_list(self, table_list: list[str]) -> list[DbTable]:
        """Creates the list of the DbTable objects.

        :param table_list: the list of the table names.
        :return: the list of the DbTable objects.
        """
        self.__logger.info("run")
        db_table_list = []
        for table_name in table_list:
            db_table = DbTable(self.__config_dict, self.__cursor,
                               SqlQueryBuilder(SqlServerTemplates()),
                               table_name, self.__work_db_name,
                               self.__clear_db_name)
            db_table_list.append(db_table)
        return db_table_list

    def __git_pull_push_repeat(self, pull_repeat: bool = False,
                               push_repeat: bool = False) -> None:
        """Repeats pull/push operations with the git repository.

        :param pull_repeat: need to repeat a pull operation.
        :param push_repeat: need to repeat a pull operation.
        :raise RuntimeError: if repeating fails over then 3 times.
        :return: None
        """

        attempt_num = 1
        pull_successful = not pull_repeat
        push_successful = not push_repeat
        while attempt_num < 3:
            self.__logger.error(f"attempt_num: {attempt_num}")
            attempt_num += 1
            try:
                if not pull_successful:
                    self.__origin.pull()
                    self.__logger.error("git pull finished")
                    pull_successful = True
                if not push_successful:
                    self.__origin.push()
                    self.__logger.error("git push finished")
                    push_successful = True
            except GitError as ex:
                self.__logger.exception(ex)
        if not pull_successful and push_successful:
            self.__logger.error("git pull/push failed")
            raise RuntimeError("git pull/push failed")

    def __update_changelog(self, file_list: list[str]) -> None:
        """Includes the files with changesets into the changelog.

        :param file_list: the list of file path to include into the changelog.
        :return: None
        """

        self.__logger.info(f"{len(file_list)} file references adding")
        with open(self.__changelog_filepath, 'a', encoding="utf-8") as file:
            for file_name in file_list:
                file.write(self.__get_include_str(file_name))
        self.__logger.info("file references added in yml")

    @staticmethod
    def __get_include_str(file_name: str) -> str:
        """Builds string to including the file name into the changelog.

        :param file_name: the file name to include into the changelog.
        :return: include string for the changelog file.
        """

        include_str = ("\n - include: "
                       '{{ file: "{0}", relativeToChangelogFile: "true" }}')
        return include_str.format(file_name)

    def __update_clear_db(self) -> None:
        """Applies the changests from the changelog to the clear database with
        the liquibase.
        :raise RuntimeError: if update with the liquibase failed.
        """
        if self.__liquibase_settings["skip_update"]:
            self.__logger.warning("Clear db updating is skipped")
            return
        self.__logger.info("Clear db updating run")
        cmd = "call {0} --defaultsFile={1} --logFile={2} " \
              "--changeLogFile={3} update"
        log = self.__liquibase_settings["liquibase_log_path"]
        cmd = cmd.format(self.__liquibase_settings["liquibase_path"],
                         self.__liquibase_settings["liquibase_properties_path"],
                         log,
                         self.__target_folder + "/"
                         + os.path.basename(self.__changelog_filepath))
        try:
            self.__logger.info(f"cmd: {cmd}\n cwd: "
                               f"{os.path.abspath(self.__git_folder_path)} ")
            subprocess.run(cmd, shell=True, check=True,
                           cwd=os.path.abspath(self.__git_folder_path))
            self.__logger.info(f"Clear db is updated")
        except Exception as ex:
            self.__logger.error(ex)
            raise RuntimeError("Update clear db failed! see the information "
                               f"in the liquibase log: {log}")

    def __commit_files(self, files: list[str], message: str,
                       is_new_changelog: bool = False) -> None:
        """Commit new files tho the git repository.

        :param files: the list of the file paths.
        :param message: the commit message.
        :param is_new_changelog: is need to commit the changelog file.
        :raise RuntimeError: if pull/push repeating fails over then 3 times.
        :return: None
        """

        self.__logger.info(f"commit {len(files)} scripts")
        if not files:
            return
        if not is_new_changelog:
            files.append(os.path.abspath(self.__changelog_filepath))
        self.__logger.info("changelog file added in list to commit")
        try:
            self.__origin.pull()
            self.__logger.info("git pull finished")
        except GitError as ex:
            self.__logger.exception(ex)
            self.__git_pull_push_repeat(pull_repeat=True)
        self.__repo.index.add(files)
        self.__logger.info("git add finished")
        self.__repo.index.commit(message)
        self.__logger.info("git commit finished")
        try:
            self.__origin.push()
            self.__logger.info("git push finished")
        except GitError as ex:
            self.__logger.exception(ex)
            self.__git_pull_push_repeat(pull_repeat=True, push_repeat=True)
