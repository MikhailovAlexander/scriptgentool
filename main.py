import os
from typing import Any, Sequence
from pyodbc import Connection, Cursor, connect
import logging
import logging.config
import json
import argparse

from core.scriptgenerator import ScriptGenerator

LOG_CONF_FILE_PATH = "config/logger_conf.json"
APP_CONF_FILE_PATH = "config/app_conf.json"


def load_json(json_file_path: str) -> dict[str: Any]:
    """Reads a data from the json file.

    :param json_file_path: the file path for data loading.
    :return: a json data.
    """

    with open(json_file_path, 'r') as conf_file:
        return json.load(conf_file)


def get_connection(conn_config: dict[str: str]) -> Connection:
    """Opens the database connection with parameters from the config dictionary.

    :param conn_config: a dictionary with connection parameters.
    :return: a Connection object.
    """

    db_serv = conn_config["server"]
    clear_db_name = conn_config["clear_db_name"]
    db_user = conn_config["user"]
    db_pass = conn_config["password"]
    con_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={db_serv};" +\
              f";DATABASE={clear_db_name};UID={db_user};PWD={db_pass};" \
              f"TrustServerCertificate=Yes"
    return connect(con_str)


def parse_args(script_config: dict[str: Any]) -> argparse.Namespace:
    """Parses command line arguments.

    :param script_config: a dictionary with default values for the arguments.
    :return: a Namespace object with arguments values.
    """

    days_before = script_config["days_before"]
    row_limit = script_config["row_limit"]
    file_size_limit = script_config["file_size_limit"]
    parser = argparse.ArgumentParser(description="A tool for generate scripts "
                                                 "with the database changes")
    parser.add_argument("-a", "--all", action="store_true",
                        help="All data upload")
    parser.add_argument("-d", "--days", type=int, default=days_before,
                        help=f"Days to search changes, default {days_before}")
    parser.add_argument("-r", "--rows", type=int, default=row_limit,
                        help=f"Row limit in single script, default {row_limit}")
    parser.add_argument("-s", "--size", type=int, default=file_size_limit,
                        help=f"File size limit in bytes, "
                             f"default {file_size_limit}")
    return parser.parse_args()


def check_table_settings(table_settings: dict[str: Any]) -> None:
    """Checks the settings from the dictionary and raises error if the check
    failed.

    :param table_settings: a dictionary with database settings.
    :return: None
    """
    table_list = [table.lower() for table in table_settings["table_list"]]
    upsert_only_list = [table.lower() for table
                        in table_settings["upsert_only_list"]]
    delete_only_list = [table.lower() for table
                        in table_settings["delete_only_list"]]
    if not table_list:
        raise Exception("table_list is empty")
    for table in upsert_only_list:
        if table not in table_list:
            raise Exception(f"upsert_only_list has table({table}), "
                            "which is not include in the table_list")
    for table in delete_only_list:
        if table not in table_list:
            raise Exception(f"delete_only_list has table({table}), "
                            "which is not include in the table_list")
        if table in upsert_only_list:
            raise Exception(f"delete_only_list has table({table}), "
                            "which is include in the upsert_only_list")


def main():
    """Основная функция для выгрузки изменений по списку таблиц"""
    log_config = load_json(LOG_CONF_FILE_PATH)
    file_path = None
    try:
        file_path = log_config["handlers"]["file_handler"]["filename"]
    except Exception as ex:
        pass
    if file_path:
        folder = os.path.split(file_path)[0]
        if not os.path.isdir(folder):
            os.mkdir(folder)
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("Start app")
    connection = None
    cursor = None
    try:
        app_config = load_json(APP_CONF_FILE_PATH)
        args = parse_args(app_config["script_settings"])
        logger.info(f"args: all: {args.all}, days: {args.days}, "
                    f"rows: {args.rows}, size: {args.size}")
        clear_db_name = app_config["connection"]["clear_db_name"]
        work_db_name = app_config["connection"]["work_db_name"]
        git_folder_path = app_config["repository"]["git_folder_path"]
        target_folder = app_config["repository"]["target_folder"]
        table_settings = app_config["table_settings"]
        check_table_settings(table_settings)
        connection = get_connection(app_config["connection"])
        cursor = connection.cursor()
        generator = ScriptGenerator(log_config, cursor, work_db_name,
                                    clear_db_name, git_folder_path,
                                    target_folder, table_settings,
                                    app_config["liquibase_settings"])
        if args.all:
            message = app_config["script_settings"]["upload_message"]
            generator.upload_tables(args.size, message, args.rows)
        else:
            message = app_config["script_settings"]["upsert_message"]
            generator.upsert_tables(args.size, message, args.days, args.rows)
    except Exception as ex:
        logger.exception(ex)
        exit(1)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logger.info('Connection close')
