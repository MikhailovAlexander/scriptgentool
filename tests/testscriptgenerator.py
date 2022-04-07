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
    TABLES, PRIMARY_KEY_COL, STR_COLUMNS, LINK_COLUMNS, INS_COLUMNS, DT_STR,\
    CREATE_DB_SCRIPT, INIT_TABLES_SCRIPT, DELETE_TABLES_SCRIPT, DROP_SCRIPT,\
    INSERT_SCRIPT_TEMPLATE

LIQUIBASE_SKIP = {"skip_update": True, "liquibase_path": "",
                  "liquibase_properties_path": "", "liquibase_log_path": "",
                  "liquibase_string": "test\n"}


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
    liquibase_settings = (config.get_config("liquibase_settings")
                          if config.get_config("liquibase_settings")
                             and not config.get_config("liquibase_settings")
                                ["skip_update"]
                          else LIQUIBASE_SKIP)
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
        self.script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                          self.queries, WORK_DB_NAME,
                                          CLEAR_DB_NAME, self.git_folder_path,
                                          self.target_folder,
                                          self.table_settings,
                                          self.liquibase_settings)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test__init__(self):
        table_settings = {"table_list": [TABLE_NAME], "upsert_only_list": [],
                          "delete_only_list": []}
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     table_settings,
                                     self.liquibase_settings)
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
                                     self.liquibase_settings)
        table_list.reverse()
        self.assertEqual(script_gen.table_names, table_list)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_empty(self):
        self.script_gen.upsert_tables(1, "")
        self.assertEqual(self.script_gen.committed_files, tuple())

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_single(self):
        values = f"(1,1,1.1,'test','{DT_STR}')"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        str_id_list = "1"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_multi(self):
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        str_id_list = "1,2,3"
        statement = self.templates.delete_statement.format(TABLE_NAME,
                                                           PRIMARY_KEY_COL,
                                                           str_id_list)
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_row_limit(self):
        values = "(1,1,1.1,'',null),(2,1,1.1,'',null),(3,1,1.1,'',null)"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "", row_limit=1)
        liquibase_string = self.liquibase_settings["liquibase_string"]
        str_id_list = ["1", "2", "3"]
        statements = [
            self.templates.delete_statement.format(TABLE_NAME, PRIMARY_KEY_COL,
                                                   num)
            for num in str_id_list
        ]
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_multi_tables(self):
        tables = self.script_gen.table_names
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        str_id_list = ["1", "1", "1"]
        tables.reverse()
        statements = [
            self.templates.delete_statement.format(table, PRIMARY_KEY_COL, num)
            for num, table in zip(str_id_list, TABLES)
        ]
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_statement_upsert_only_list(self):
        table_settings = {"table_list": [TABLE_NAME, TABLE_NAME_2,
                                         TABLE_NAME_3],
                          "upsert_only_list": [TABLE_NAME, TABLE_NAME_2],
                          "delete_only_list": []}
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     table_settings,
                                     self.liquibase_settings)
        tables = script_gen.table_names
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        str_id = "1"
        statement = self.templates.delete_statement.format(TABLE_NAME_3,
                                                           PRIMARY_KEY_COL,
                                                           str_id)
        file_text = ""
        with open(script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_delete_file_size_and_changelog(self):
        tables = self.script_gen.table_names
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(tables, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(50, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        str_id_list = ["1", "1", "1"]
        tables.reverse()
        statements = [
            liquibase_string +
            self.templates.delete_statement.format(table, PRIMARY_KEY_COL, num)
            for num, table in zip(str_id_list, tables)
        ]
        self.assertEqual(len(self.script_gen.committed_files), 3)
        file_texts = []
        include_str = ("\n - include: "
                       '{{ file: "{0}", relativeToChangelogFile: "true" }}')
        changelog_text = "databaseChangeLog:"
        for file in self.script_gen.committed_files:
            with open(file, 'r') as f:
                file_texts.append(f.read())
            changelog_text += include_str.format(os.path.basename(file))
        self.assertEqual(file_texts, statements)
        with open(self.script_gen.changelog_filepath, 'r') as f:
            self.assertEqual(f.read(), changelog_text)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_single(self):
        values = f"(1,1,1.1,'test','{DT_STR}')"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_multi(self):
        values = (',\n'+' ' * 8).join(["(1,123,1.23,'test',null)",
                                       "(2,null,1.23,'''quoted''',null)",
                                       "(3,123,null,'test',null)"])
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_row_limit(self):
        values = ["(1,123,1.23,'test',null)", "(2,null,1.23,'''quoted''',null)"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "", row_limit=1)
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values]
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_multi_tables(self):
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(self.script_gen.table_names, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statements = [self.templates.upsert_statement.format(table,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for table, value
                      in zip(self.script_gen.table_names, values)]
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_delete_only_list(self):
        table_settings = {"table_list": [TABLE_NAME, TABLE_NAME_2,
                                         TABLE_NAME_3],
                          "upsert_only_list": [],
                          "delete_only_list": [TABLE_NAME_2, TABLE_NAME_3]}
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     table_settings,
                                     self.liquibase_settings)
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(self.script_gen.table_names, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        script_gen.upsert_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values[0],
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        file_text = ""
        with open(script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_days_before_empty(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  "(2,1,1.2,'b','1999-03-30 23:19:14.777')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "", days_before=1)
        self.assertEqual(self.script_gen.committed_files, tuple())

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_days_before_single(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  f"(2,1,1.2,'b','{DT_STR}')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "", days_before=1)
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values[1:]]
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_upsert_statement_days_before_multi(self):
        values = ["(1,1,1.2,'a','1999-03-30 23:19:14.777')",
                  f"(2,1,1.2,'b','{DT_STR}')", f"(3,1,1.2,'b','{DT_STR}')"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, "", days_before=1)
        liquibase_string = self.liquibase_settings["liquibase_string"]
        str_values = (',\n'+' ' * 8).join(values[1:])
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           str_values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upsert_tables_commit_message(self):
        values = f"(1,1,1.1,'test',null)"
        commit_message = "test commit message"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(CLEAR_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upsert_tables(10000, commit_message)
        self.assertEqual(self.repo.head.commit.message, commit_message)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_empty(self):
        script_gen = ScriptGenerator(LOGGER_DICT_STUB, self.cursor,
                                     self.queries, WORK_DB_NAME, CLEAR_DB_NAME,
                                     self.git_folder_path, self.target_folder,
                                     self.table_settings,
                                     self.liquibase_settings)
        script_gen.upload_tables(1, "")
        self.assertEqual(script_gen.committed_files, tuple())

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_single(self):
        values = f"(1,1,1.1,'test','{DT_STR}')"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upload_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_multi(self):
        values = (',\n'+' ' * 8).join(["(1,123,1.23,'test',null)",
                                       "(2,null,1.23,'''quoted''',null)",
                                       "(3,123,null,'test',null)"])
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upload_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statement = self.templates.upsert_statement.format(TABLE_NAME,
                                                           STR_COLUMNS,
                                                           values,
                                                           PRIMARY_KEY_COL,
                                                           LINK_COLUMNS,
                                                           INS_COLUMNS)
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string, statement]))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_row_limit(self):
        values = ["(1,123,1.23,'test',null)", "(2,null,1.23,'''quoted''',null)"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        self.script_gen.upload_tables(10000, "", row_limit=1)
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statements = [self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values]
        file_text = ""
        with open(self.script_gen.committed_files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + statements))
        self.assertEqual(len(self.script_gen.committed_files), 1)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_multi_tables(self):
        values = ["(1,1,1.1,'',null)", "(1,1,1.1,'',null)", "(1,1,1.1,'',null)"]
        for table, value in zip(self.script_gen.table_names, values):
            ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, table,
                                                      STR_COLUMNS, value)
            self.cursor.execute(ins_query)
        self.script_gen.upload_tables(10000, "")
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statements = [liquibase_string +
                      self.templates.upsert_statement.format(table,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for table, value
                      in zip(self.script_gen.table_names, values)]
        file_texts = []
        for file in self.script_gen.committed_files:
            with open(file, 'r') as f:
                file_texts.append(f.read())
        self.assertEqual(file_texts, statements)
        self.assertEqual(len(self.script_gen.committed_files), 3)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_file_size(self):
        values = ["(1,123,1.23,'test',null)", "(2,null,1.23,'''quoted''',null)"]
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, ",".join(values))
        self.cursor.execute(ins_query)
        self.script_gen.upload_tables(50, "", row_limit=1)
        liquibase_string = self.liquibase_settings["liquibase_string"]
        statements = [liquibase_string +
                      self.templates.upsert_statement.format(TABLE_NAME,
                                                             STR_COLUMNS,
                                                             value,
                                                             PRIMARY_KEY_COL,
                                                             LINK_COLUMNS,
                                                             INS_COLUMNS)
                      for value in values]
        file_texts = []
        for file in self.script_gen.committed_files:
            with open(file, 'r') as f:
                file_texts.append(f.read())
        self.assertEqual(file_texts, statements)
        self.assertEqual(len(self.script_gen.committed_files), 2)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables_commit_message(self):
        values = f"(1,1,1.1,'test',null)"
        commit_message = "test commit message"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upload_tables(10000, commit_message)
        self.assertEqual(self.repo.head.commit.message, commit_message)

    @unittest.skipIf(not IS_CONNECTED, "Is not connected")
    def test_upload_tables__changelog(self):
        values = f"(1,1,1.1,'test',null)"
        commit_message = "test commit message"
        ins_query = INSERT_SCRIPT_TEMPLATE.format(WORK_DB_NAME, TABLE_NAME,
                                                  STR_COLUMNS, values)
        self.cursor.execute(ins_query)
        self.script_gen.upload_tables(10000, "")
        file_name = os.path.basename(self.script_gen.committed_files[0])
        changelog_text = ("databaseChangeLog:\n - include: "
                          f'{{ file: "{file_name}", '
                          f'relativeToChangelogFile: "true" }}')
        with open(self.script_gen.changelog_filepath, 'r') as f:
            self.assertEqual(f.read(), changelog_text)


if __name__ == '__main__':
    unittest.main()
