import unittest
import os
from core.filewriter import FileWriter
from dbconstatnts import LOGGER_DICT_STUB


FOLDER_PATH = os.getcwd() + '/unittest_filewriter'


class TestFileWriter(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        if not os.path.exists(FOLDER_PATH):
            os.mkdir(FOLDER_PATH)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(FOLDER_PATH):
            os.removedirs(FOLDER_PATH)

    def tearDown(self) -> None:
        if os.path.exists(FOLDER_PATH):
            for file in os.listdir(FOLDER_PATH):
                os.remove(FOLDER_PATH + '/' + file)

    def test__init__(self):
        file_writer = FileWriter(LOGGER_DICT_STUB, 10000, FOLDER_PATH, "test")
        self.assertEqual(file_writer.files, [])

    def test_save_scripts_empty(self):
        file_writer = FileWriter(LOGGER_DICT_STUB, 10000, FOLDER_PATH, "test")
        file_writer.save_scripts([], "prefix")
        self.assertEqual(len(file_writer.files), 0)

    def test_save_scripts_single(self):
        scripts = ["test\n"]
        liquibase_string = "first\n"
        file_writer = FileWriter(LOGGER_DICT_STUB, 10000, FOLDER_PATH,
                                 liquibase_string)
        file_writer.save_scripts(scripts, "prefix")
        files = file_writer.files
        self.assertEqual(len(files), 1)
        self.assertTrue(os.path.exists(files[0]))
        file_text = ""
        with open(files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + scripts))

    def test_save_scripts_multi(self):
        scripts = ["script1\n", "script2\n", "script3\n"]
        liquibase_string = "first\n"
        file_writer = FileWriter(LOGGER_DICT_STUB, 10000, FOLDER_PATH,
                                 liquibase_string)
        file_writer.save_scripts(scripts, "prefix")
        files = file_writer.files
        self.assertEqual(len(files), 1)
        self.assertTrue(os.path.exists(files[0]))
        file_text = ""
        with open(files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + scripts))

    def test_save_scripts_into_new_file(self):
        scripts = ["script1\n", "script2\n"]
        liquibase_string = "first\n"
        file_writer = FileWriter(LOGGER_DICT_STUB, 10000, FOLDER_PATH,
                                 liquibase_string)
        file_writer.save_scripts(scripts[:1], "prefix")
        file_writer.save_scripts(scripts[1:], "prefix", into_new_file=True)
        files = file_writer.files
        self.assertEqual(len(files), 2)
        file_text = ""
        with open(files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + scripts[:1]))
        with open(files[1], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + scripts[1:]))

    def test_save_scripts_size_limit(self):
        size_limit = 50
        scripts = ["Lorem ipsum dolor sit amet, consectetur adipiscing elit,\n",
                   "sed do eiusmod tempor incididunt ut labore et dolore magna"]
        liquibase_string = "first\n"
        file_writer = FileWriter(LOGGER_DICT_STUB, size_limit, FOLDER_PATH,
                                 liquibase_string)
        file_writer.save_scripts(scripts, "prefix")
        files = file_writer.files
        self.assertEqual(len(files), 2)
        file_text = ""
        with open(files[0], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + scripts[:1]))
        with open(files[1], 'r') as file:
            file_text = file.read()
        self.assertEqual(file_text, "".join([liquibase_string] + scripts[1:]))


if __name__ == '__main__':
    unittest.main()
