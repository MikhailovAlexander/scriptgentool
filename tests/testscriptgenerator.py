import unittest
from git import Repo
from datetime import datetime
import os

from core.scriptgenerator import ScriptGenerator
from core.sqlquerybuilder import SqlQueryBuilder
from core.sqlservertemplates import SqlServerTemplates
from testconfigreader import TestConfigReader
from dbconstatnts import DbConnector, LOGGER_DICT_STUB, IS_CONNECTED, \
    WORK_DB_NAME, CLEAR_DB_NAME, TABLE_NAME, TABLE_NAME_2, TABLE_NAME_3,\
    TABLES, PRIMARY_KEY_COL, UPDATE_DT_COL,\
    COLUMNS, STR_COLUMNS, LINK_COLUMNS, INS_COLUMNS, DT, DT_STR, SUB_TABLES,\
    CREATE_SUB_TABLES_SCRIPTS, DROP_SUB_TABLES_SCRIPTS, CREATE_DB_SCRIPT,\
    INIT_TABLES_SCRIPT, DELETE_TABLES_SCRIPT, DROP_SCRIPT,\
    INSERT_SCRIPT_TEMPLATE


class TestScriptGenerator(unittest.TestCase):
    config = TestConfigReader()
    repository_config = config.get_config("repository")
    templates = SqlServerTemplates()
    queries = SqlQueryBuilder(templates)
    git_folder_path = repository_config["git_folder_path"]
    target_folder = repository_config["target_folder"]
    repo = Repo(git_folder_path)
    start_commit = repo.head.commit
    connector = None
    cursor = None
    liquibase_settings_skip = {"skip_update": True, "liquibase_path": "",
                               "liquibase_properties_path": "",
                               "liquibase_log_path": "",
                               "liquibase_string": "test\n"}
    table_settings = {"table_list": TABLES, "upsert_only_list": [],
                      "delete_only_list": []}

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
    def test__init__(self):
        table_settings = {"table_list": [TABLE_NAME], "upsert_only_list": [],
                          "delete_only_list": []}
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     table_settings,
                                     self.liquibase_settings_skip)
        changelog_name = datetime.now().strftime("changelog_tree%Y%m.yml")
        changelog_path = "/".join([self.git_folder_path, self.target_folder,
                                   changelog_name])
        self.assertEqual(script_gen.changelog_filepath, changelog_path)
        self.assertTrue(os.path.exists(changelog_path))
        self.assertEqual(script_gen.committed_files, tuple())
        self.assertEqual(script_gen.table_names, [TABLE_NAME.lower()])

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test__init__multi_tables(self):
        table_list = [TABLE_NAME_3, TABLE_NAME_2, TABLE_NAME]
        table_settings = {"table_list": table_list, "upsert_only_list": [],
                          "delete_only_list": []}
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     table_settings,
                                     self.liquibase_settings_skip)
        table_list.reverse()
        self.assertEqual(script_gen.table_names, table_list)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_empty(self):
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     self.table_settings,
                                     self.liquibase_settings_skip)
        script_gen.upsert_tables(1, "")
        self.assertEqual(script_gen.committed_files, tuple())

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_list_single(self):
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     self.table_settings,
                                     self.liquibase_settings_skip)
        values = f"(1,1,1.1,'test','{DT_STR}')"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings_skip["liquibase_string"]
        str_id_list = "1"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        file_text = ""
        with open(script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_list_multi(self):
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     self.table_settings,
                                     self.liquibase_settings_skip)
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings_skip["liquibase_string"]
        str_id_list = "1,2,3"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        file_text = ""
        with open(script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_row_limit(self):
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     self.table_settings,
                                     self.liquibase_settings_skip)
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        script_gen.upsert_tables(10000, "", row_limit=1)
        liquibase_string = self.liquibase_settings_skip["liquibase_string"]
        str_id_list = ["1", "2", "3"]
        statements = [
            self.templates.delete_statement.format(TABLE_NAME, PRIMARY_KEY_COL,
                                                   num)
            for num in str_id_list
        ]
        file_text = ""
        with open(script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_list_multi_tables(self):
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     self.table_settings,
                                     self.liquibase_settings_skip)
        tables = script_gen.table_names
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings_skip["liquibase_string"]
        str_id_list = ["1", "1", "1"]
        tables.reverse()
        statements = [
            self.templates.delete_statement.format(table, PRIMARY_KEY_COL, num)
            for num, table in zip(str_id_list, TABLES)
        ]
        file_text = ""
        with open(script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_empty(self):
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     self.table_settings,
                                     self.liquibase_settings_skip)
        script_gen.upload_tables(1, "")
        self.assertEqual(script_gen.committed_files, tuple())


if __name__ == '__main__':
    unittest.main()
