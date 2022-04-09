import copy
import re
import unittest
from git import Repo
from datetime import datetime
import os

from main import main as tested_main
from core.sqlservertemplates import SqlServerTemplates
from testconfigreader import TestConfigReader
from dbconstatnts import DbConnector, LOGGER_DICT_STUB, IS_CONNECTED, \
    WORK_DB_NAME, CLEAR_DB_NAME, TABLE_NAME, TABLE_NAME_2, TABLE_NAME_3,\
    TABLES, PRIMARY_KEY_COL, STR_COLUMNS, LINK_COLUMNS, INS_COLUMNS, DT_STR,\
    CREATE_DB_SCRIPT, INIT_TABLES_SCRIPT, DELETE_TABLES_SCRIPT, DROP_SCRIPT,\
    INSERT_SCRIPT_TEMPLATE


def extract_files(changelog_text):
    pattern = '(?<=file: \")[A-z._0-9]+(?=\")'
    return re.findall(pattern, changelog_text)


class TestMain(unittest.TestCase):
    templates = SqlServerTemplates()
    config = TestConfigReader().get_config()
    git_folder_path = config["repository"]["git_folder_path"]
    target_folder = config["repository"]["target_folder"]
    target_folder_path = git_folder_path + "/" + target_folder
    changelog_name = datetime.now().strftime("changelog_tree%Y%m.yml")
    changelog_path = target_folder_path + "/" + changelog_name
    repo = Repo(git_folder_path)
    start_commit = repo.head.commit
    connector = None
    cursor = None

    @classmethod
    def setUpClass(cls):
        if not IS_CONNECTED:
            return
        cls.connector = DbConnector()
        cls.cursor = cls.connector.get_cursor()
        cls.cursor.execute(CREATE_DB_SCRIPT)
        cls.cursor.execute(INIT_TABLES_SCRIPT)

    @classmethod
    def tearDownClass(cls):
        if cls.connector:
            cls.cursor.execute(DROP_SCRIPT)
            cls.connector.close()
        cls.repo.git.reset('--hard', cls.start_commit)
        cls.repo.git.push('--force')

    def setUp(self):
        if self.cursor:
            self.cursor.execute(DELETE_TABLES_SCRIPT)
        self.repo.git.reset('--hard', self.start_commit)
        self.repo.git.push('--force')

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_empty(self):
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        changelog_text = "databaseChangeLog:"
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            self.assertEqual(file.read(), changelog_text)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_delete_statement_single(self):
        values = f"(1,1,1.1,'test','{DT_STR}')"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        liquibase_string = self.config["liquibase_settings"]["liquibase_string"]
        str_id_list = "1"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_delete_statement_multi(self):
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        liquibase_string = self.config["liquibase_settings"]["liquibase_string"]
        str_id_list = "1,2,3"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_delete_statement_row_limit(self):
        config = copy.deepcopy(self.config)
        config["script_settings"]["row_limit"] = 1
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = config["liquibase_settings"]["liquibase_string"]
        str_id_list = ["1", "2", "3"]
        statements = [
            self.templates.delete_statement.format(TABLE_NAME, PRIMARY_KEY_COL,
                                                   num)
            for num in str_id_list
        ]
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string] + statements))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_delete_statement_multi_tables(self):
        tables = [TABLE_NAME, TABLE_NAME_2, TABLE_NAME_3]
        config = copy.deepcopy(self.config)
        config["table_settings"]["table_list"] = tables
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = config["liquibase_settings"]["liquibase_string"]
        str_id_list = ["1", "1", "1"]
        tables.reverse()
        statements = [
            self.templates.delete_statement.format(table, PRIMARY_KEY_COL, num)
            for num, table in zip(str_id_list, TABLES)
        ]
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string] + statements))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_delete_statement_upsert_only_list(self):
        tables = [TABLE_NAME, TABLE_NAME_2, TABLE_NAME_3]
        config = copy.deepcopy(self.config)
        config["table_settings"]["table_list"] = tables
        config["table_settings"]["upsert_only_list"] = [TABLE_NAME,
                                                        TABLE_NAME_2]
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = config["liquibase_settings"]["liquibase_string"]
        statement = self.templates.delete_statement.format(TABLE_NAME_3,
                                                           PRIMARY_KEY_COL, "1")
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_delete_statement_file_size(self):
        tables = [TABLE_NAME, TABLE_NAME_2, TABLE_NAME_3]
        config = copy.deepcopy(self.config)
        config["table_settings"]["table_list"] = tables
        config["script_settings"]["file_size_limit"] = 50
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = config["liquibase_settings"]["liquibase_string"]
        str_id_list = ["1", "1", "1"]
        tables.reverse()
        statements = [
            liquibase_string +
            self.templates.delete_statement.format(table, PRIMARY_KEY_COL, num)
            for num, table in zip(str_id_list, tables)]
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 3)
        file_texts = []
        for file in files:
            with open(self.target_folder_path + "/" + file, 'r') as f:
                file_texts.append(f.read())
        self.assertEqual(file_texts, statements)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_single(self):
        values = f"(1,1,1.1,'test','{DT_STR}')"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        liquibase_string = self.config["liquibase_settings"]["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_multi(self):
        values = (',\n'+' ' * 8).join([f"(1,123,1.23,'test','{DT_STR}')",
                                       f"(2,null,1.23,'''quoted''','{DT_STR}')",
                                       f"(3,123,null,'test','{DT_STR}')"])
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        liquibase_string = self.config["liquibase_settings"]["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_row_limit(self):
        config = copy.deepcopy(self.config)
        config["script_settings"]["row_limit"] = 1
        values = [f"(1,123,1.23,'test','{DT_STR}')",
                  f"(2,null,1.23,'''quoted''','{DT_STR}')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = config["liquibase_settings"]["liquibase_string"]
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values]
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string] + statements))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_multi_tables(self):
        tables = [TABLE_NAME, TABLE_NAME_2, TABLE_NAME_3]
        config = copy.deepcopy(self.config)
        config["table_settings"]["table_list"] = tables
        values = [f"(1,1,1.1,'','{DT_STR}')", f"(1,1,1.1,'','{DT_STR}')",
                  f"(1,1,1.1,'','{DT_STR}')"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = config["liquibase_settings"]["liquibase_string"]
        statements = [self.templates.upsert_statement.format(table,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for table, value
                      in zip(tables, values)]
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string] + statements))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_delete_only_list(self):
        tables = [TABLE_NAME, TABLE_NAME_2, TABLE_NAME_3]
        config = copy.deepcopy(self.config)
        config["table_settings"]["table_list"] = tables
        config["table_settings"]["delete_only_list"] = [TABLE_NAME_2,
                                                        TABLE_NAME_3]
        values = [f"(1,1,1.1,'','{DT_STR}')", f"(1,1,1.1,'','{DT_STR}')",
                  f"(1,1,1.1,'','{DT_STR}')"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = config["liquibase_settings"]["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values[0],
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_days_before_empty(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  "(2,1,1.2,'b','1999-03-30 23:19:14.777')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        changelog_text = "databaseChangeLog:"
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            self.assertEqual(file.read(), changelog_text)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_days_before_single(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  f"(2,1,1.2,'b','{DT_STR}')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        liquibase_string = self.config["liquibase_settings"]["liquibase_string"]
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values[1:]]
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string] + statements))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_days_before_multi(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  f"(2,1,1.2,'b','{DT_STR}')", f"(3,1,1.2,'b','{DT_STR}')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=self.config, outer_cursor=self.cursor)
        liquibase_string = self.config["liquibase_settings"]["liquibase_string"]
        str_values = (',\n'+' ' * 8).join(values[1:])
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           str_values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_commit_message(self):
        commit_message = "test commit message"
        config = copy.deepcopy(self.config)
        config["script_settings"]["upsert_message"] = commit_message
        values = f"(1,123,1.23,'test','{DT_STR}')"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        self.assertEqual(self.repo.head.commit.message, commit_message)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_statement_upload_all_rows(self):
        config = copy.deepcopy(self.config)
        config["script_settings"]["all_rows"] = True
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  f"(2,1,1.2,'b','{DT_STR}')", f"(3,1,1.2,'b','{DT_STR}')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        tested_main(outer_log_config=LOGGER_DICT_STUB,
                    outer_app_config=config, outer_cursor=self.cursor)
        liquibase_string = self.config["liquibase_settings"]["liquibase_string"]
        str_values = (',\n'+' ' * 8).join(values)
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           str_values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        changelog_text = ""
        self.assertTrue(os.path.exists(self.changelog_path))
        with open(self.changelog_path, 'r') as file:
            changelog_text = file.read()
        files = extract_files(changelog_text)
        self.assertEqual(len(files), 1)
        with open(self.target_folder_path + "/" + files[0], 'r') as file:
            self.assertEqual(file.read(),
                             "".join([liquibase_string, statement]))


if __name__ == '__main__':
    unittest.main()
